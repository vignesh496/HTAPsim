#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/memutils.h"

#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"

#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"

PG_MODULE_MAGIC;

typedef struct HtapTxn
{
    TransactionId xid;
    List *sql_list;   /* list of char* SQL strings */
} HtapTxn;

static HtapTxn *current_txn = NULL;


/* Signal flags */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;
static void log_sql_from_decoded_change(const char *data); 
// static void buffer_insert(const char *table, const char *data);
// static void buffer_delete(const char *table, const char *data);


extern HtapTxn *current_txn;

void _PG_init(void);
PGDLLEXPORT void row_to_column_main(Datum main_arg) pg_attribute_noreturn();

/* Signal handlers */
static void
row_to_column_sighup(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sighup = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void
row_to_column_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

PGDLLEXPORT void
row_to_column_main(Datum main_arg)
{
    pqsignal(SIGHUP, row_to_column_sighup);
    pqsignal(SIGTERM, row_to_column_sigterm);
    BackgroundWorkerUnblockSignals();

    /* Connect to target DB */
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    ereport(LOG,
        (errmsg("HTAP bgworker started (SQL logical decoding)")));

    while (!got_sigterm)
    {
        int rc;

        rc = WaitLatch(
            MyLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
            1L,
            0);

        ResetLatch(MyLatch);

        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);

        if (got_sighup)
        {
            got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        CHECK_FOR_INTERRUPTS();

        /* -------- LOGICAL DECODING -------- */

        StartTransactionCommand();

        /* REQUIRED in PG17 */
        PushActiveSnapshot(GetTransactionSnapshot());

        SPI_connect();

        int ret = SPI_execute(
            "SELECT lsn, xid, data "
            "FROM pg_logical_slot_get_changes('htap_sync', NULL, NULL)",
            true,
            0);
            if (SPI_processed > 0)
                ereport(LOG, (errmsg("[HTAP] decoded %lu rows", SPI_processed)));


        if (ret == SPI_OK_SELECT)
        {
            for (uint64 i = 0; i < SPI_processed; i++)
            {
                char *lsn =
                    SPI_getvalue(SPI_tuptable->vals[i],
                                SPI_tuptable->tupdesc, 1);
                char *xid =
                    SPI_getvalue(SPI_tuptable->vals[i],
                                SPI_tuptable->tupdesc, 2);
                char *data =
                    SPI_getvalue(SPI_tuptable->vals[i],
                                SPI_tuptable->tupdesc, 3);

                if (data)
                {
                    ereport(LOG,
                        (errmsg("[HTAP-DECODE] lsn=%s xid=%s %s",
                                lsn ? lsn : "NULL",
                                xid ? xid : "NULL",
                                data)));

                    log_sql_from_decoded_change(data);
                }
            }
        }

        SPI_finish();

        /* REQUIRED cleanup */
        PopActiveSnapshot();
        CommitTransactionCommand();
        /* ---------------------------------- */
    }

    ereport(LOG, (errmsg("HTAP bgworker shutting down")));
    proc_exit(0);
}

static void
log_sql_from_decoded_change(const char *data)
{
    char table[128];
    char col_table[160];
    const char *p;

    StringInfoData columns;
    StringInfoData values;
    StringInfoData sql;

    /* ---------- BEGIN ---------- */
    if (strncmp(data, "BEGIN", 5) == 0)
    {
        if (current_txn)
            return;

        current_txn = palloc0(sizeof(HtapTxn));
        current_txn->xid = GetCurrentTransactionIdIfAny();
        current_txn->sql_list = NIL;

        ereport(DEBUG1, (errmsg("[HTAP] BEGIN txn")));
        return;
    }

    /* ---------- COMMIT ---------- */
    if (strncmp(data, "COMMIT", 6) == 0)
    {
        if (!current_txn)
            return;

        ereport(LOG,
            (errmsg("[HTAP] COMMIT txn, applying %d changes",
                    list_length(current_txn->sql_list))));

        ListCell *lc;
        foreach (lc, current_txn->sql_list)
        {
            char *sqlstr = (char *) lfirst(lc);

            int rc = SPI_execute(sqlstr, false, 0);
            if (rc < 0)
                ereport(WARNING,
                    (errmsg("[HTAP] SPI rc=%d for: %s", rc, sqlstr)));
        }

        list_free_deep(current_txn->sql_list);
        pfree(current_txn);
        current_txn = NULL;

        ereport(DEBUG1, (errmsg("[HTAP] COMMIT txn applied")));
        return;
    }

    /* ---------- MUST BE IN TXN ---------- */
    if (!current_txn)
        return;

    /* ---------- FILTER TABLE ---------- */
    if (!strstr(data, "table public."))
        return;

    if (sscanf(data, "table %*[^.].%127[^:]:", table) != 1)
        return;

    snprintf(col_table, sizeof(col_table), "%s_col", table);

    // INSERT ONLY (UNCHANGED)

    if ((p = strstr(data, "INSERT:")) == NULL)
        return;

    initStringInfo(&columns);
    initStringInfo(&values);
    initStringInfo(&sql);

    p += strlen("INSERT:");
    bool first = true;

    while (*p)
    {
        char col[64];
        char val[256];

        if (sscanf(p, " %63[^[][%*[^]]]:%255s", col, val) != 2)
            break;

        if (!first)
        {
            appendStringInfoString(&columns, ", ");
            appendStringInfoString(&values, ", ");
        }

        appendStringInfoString(&columns, col);

        if (strcmp(val, "NULL") == 0)
            appendStringInfoString(&values, "NULL");
        else
            appendStringInfo(&values, "%s", val);

        first = false;

        p = strchr(p + 1, ' ');
        if (!p)
            break;
    }

    appendStringInfo(&sql,
        "INSERT INTO %s (%s) VALUES (%s);",
        col_table, columns.data, values.data);

    current_txn->sql_list =
        lappend(current_txn->sql_list, pstrdup(sql.data));

    ereport(DEBUG1, (errmsg("[HTAP-BUFFER INSERT] %s", sql.data)));

    pfree(columns.data);
    pfree(values.data);
    pfree(sql.data);
}

void
_PG_init(void)
{
    BackgroundWorker worker;

    memset(&worker, 0, sizeof(worker));

    worker.bgw_flags =
        BGWORKER_SHMEM_ACCESS |
        BGWORKER_BACKEND_DATABASE_CONNECTION;

    worker.bgw_start_time = BgWorkerStart_ConsistentState;
    worker.bgw_restart_time = 10;

    snprintf(worker.bgw_name, BGW_MAXLEN,
             "row_to_column_htap_worker");

    snprintf(worker.bgw_library_name, BGW_MAXLEN,
             "row_to_column");

    snprintf(worker.bgw_function_name, BGW_MAXLEN,
             "row_to_column_main");

    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&worker);
}


