#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "postmaster/bgworker.h"
#include "storage/latch.h"
#include "storage/ipc.h"

#include "executor/spi.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"

#include "lib/stringinfo.h"
#include "libpq/pqformat.h"          
#include "replication/logicalproto.h"

#include "portability/instr_time.h"
#include "utils/timestamp.h"


PG_MODULE_MAGIC;

/* ------------------------------------------------------------
 * SIGNAL
 * ------------------------------------------------------------*/
static volatile sig_atomic_t got_sigterm = false;

static void
handle_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

/* ------------------------------------------------------------
 * TRANSACTION BUFFER
 * ------------------------------------------------------------*/
typedef struct
{
    List *sql_list;
} TxnBuffer;

static TxnBuffer *current_txn = NULL;

/* ------------------------------------------------------------
 * RELATION CACHE
 * ------------------------------------------------------------*/
#define MAX_COLS 64

typedef struct
{
    Oid     relid;
    char    relname[NAMEDATALEN];
    int     ncols;
    Oid     coltypes[MAX_COLS];
} RelationCache;

static RelationCache relcache;

/* ------------------------------------------------------------
 * HELPERS
 * ------------------------------------------------------------*/
static bool
needs_quotes(Oid typid)
{
    switch (typid)
    {
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
            return false;
    }
    return true;
}

static void
buffer_sql(const char *sql)
{
    if (!current_txn)
        elog(ERROR, "buffer_sql outside transaction");

    current_txn->sql_list =
        lappend(current_txn->sql_list, pstrdup(sql));
}

/* 
 * ------------------------------------------------------------
 * PGOUTPUT DECODER (PG17 SAFE)
 * ------------------------------------------------------------
*/

static void
decode_pgoutput(bytea *data)
{
    StringInfoData msg;

    /* Correct initialization */
    msg.data   = VARDATA(data);
    msg.len    = VARSIZE(data) - VARHDRSZ;
    msg.maxlen = msg.len;
    msg.cursor = 0;

    char tag = pq_getmsgbyte(&msg);

    /* ---------- BEGIN ---------- */
    if (tag == 'B')
    {
        current_txn = palloc0(sizeof(TxnBuffer));
        return;
    }

    /* ---------- COMMIT ---------- */
    if (tag == 'C')
    {
        ListCell   *lc;
        instr_time  start, duration;
        uint64      row_count = list_length(current_txn->sql_list);

        INSTR_TIME_SET_CURRENT(start);

        foreach (lc, current_txn->sql_list)
            SPI_execute(lfirst(lc), false, 0);

        INSTR_TIME_SET_CURRENT(duration);
        INSTR_TIME_SUBTRACT(duration, start);

        /* Log performance data */
        elog(LOG, "[PERF] Columnar Sync: processed %lu rows in %.3f ms (Avg: %.4f ms/row)",
            row_count,
            INSTR_TIME_GET_MILLISEC(duration),
            INSTR_TIME_GET_MILLISEC(duration) / (row_count ? row_count : 1));

        list_free_deep(current_txn->sql_list);
        pfree(current_txn);
        current_txn = NULL;
        return;
    }

    /* ---------- RELATION ---------- */
    if (tag == 'R')
    {
        relcache.relid = pq_getmsgint(&msg, 4);

        pq_getmsgstring(&msg); /* namespace */

        strlcpy(relcache.relname,
                pq_getmsgstring(&msg),
                NAMEDATALEN);

        pq_getmsgbyte(&msg); /* replica identity */

        relcache.ncols = pq_getmsgint(&msg, 2);

        for (int i = 0; i < relcache.ncols; i++)
        {
            pq_getmsgbyte(&msg);        /* key flag */
            pq_getmsgstring(&msg);      /* column name */
            relcache.coltypes[i] = pq_getmsgint(&msg, 4);
            pq_getmsgint(&msg, 4);      /* typmod */
        }
        return;
    }

    /* ---------- INSERT ---------- */
    if (tag == 'I')
    {
        pq_getmsgint(&msg, 4); /* relid */

        if (pq_getmsgbyte(&msg) != 'N')
            elog(ERROR, "expected new tuple");

        int ncols = pq_getmsgint(&msg, 2);

        StringInfoData sql;
        initStringInfo(&sql);

        appendStringInfo(&sql,
                         "INSERT INTO %s_col VALUES (",
                         relcache.relname);

        for (int i = 0; i < ncols; i++)
        {
            char kind = pq_getmsgbyte(&msg);

            if (kind == 'n')
            {
                appendStringInfoString(&sql, "NULL");
            }
            else if (kind == 't')
            {
                int len = pq_getmsgint(&msg, 4);
                const char *val = pq_getmsgbytes(&msg, len);
                const char *q = needs_quotes(relcache.coltypes[i]) ? "'" : "";

                appendStringInfo(&sql, "%s%.*s%s",
                                 q, len, val, q);
            }
            else
                elog(ERROR, "unknown tuple kind %c", kind);

            if (i < ncols - 1)
                appendStringInfoString(&sql, ", ");
        }

        appendStringInfoString(&sql, ");");

        buffer_sql(sql.data);
        pfree(sql.data);
    }
}

/* ------------------------------------------------------------
 * BGWORKER MAIN
 * ------------------------------------------------------------*/
PGDLLEXPORT void
row_to_column_main(Datum arg)
{
    pqsignal(SIGTERM, handle_sigterm);
    BackgroundWorkerUnblockSignals();

    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    while (!got_sigterm)
    {
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());
        SPI_connect();

        
        // /* Corrected SPI_execute call */
        // SPI_execute("select data from pg_logical_slot_get_binary_changes('sample_slot2', null, null, 'proto_version', '2', 'publication_names', 'pub_sales', 'streaming', 'on')", true, 0);

        /* Corrected SPI_execute call --> with committed transactions only */
        SPI_execute("select data from pg_logical_slot_get_binary_changes('sample_slot2', null, null, 'proto_version', '1', 'publication_names', 'pub_sales')", true, 0);

        if (SPI_processed == 0)
        {
            WaitLatch(MyLatch,
                    WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                    50L,  
                    0);
            ResetLatch(MyLatch);
        }
        else
        {
            for (uint64 i = 0; i < SPI_processed; i++)
            {
                bool isnull;
                Datum d = SPI_getbinval(
                    SPI_tuptable->vals[i],
                    SPI_tuptable->tupdesc,
                    1,
                    &isnull);

                decode_pgoutput(DatumGetByteaP(d));
            }
        }


        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
    }

    proc_exit(0);
}

/* ------------------------------------------------------------
 * INIT
 * ------------------------------------------------------------*/
void
_PG_init(void)
{
    BackgroundWorker worker;

    MemSet(&worker, 0, sizeof(worker));

    worker.bgw_flags =
        BGWORKER_SHMEM_ACCESS |
        BGWORKER_BACKEND_DATABASE_CONNECTION;

    worker.bgw_start_time = BgWorkerStart_ConsistentState;
    worker.bgw_restart_time = 5;

    snprintf(worker.bgw_name, BGW_MAXLEN,
             "row_to_column_pgoutput");

    snprintf(worker.bgw_library_name, BGW_MAXLEN,
             "row_to_column");

    snprintf(worker.bgw_function_name, BGW_MAXLEN,
             "row_to_column_main");

    RegisterBackgroundWorker(&worker);
}





// 2026-01-23 12:46:59.377 IST [96048] LOG:  [PERF] Columnar Sync: processed 100000 rows in 4942.416 ms (Avg: 0.0494 ms/row)
// 2026-01-23 12:46:59.416 IST [96048] LOG:  starting logical decoding for slot "sample_slot2"


// 2026-01-23 12:49:05.952 IST [96048] LOG:  [PERF] Columnar Sync: processed 1000000 rows in 41021.798 ms (Avg: 0.0410 ms/row)
// 2026-01-23 12:49:06.110 IST [96048] LOG:  starting logical decoding for slot "sample_slot2"

// 2026-01-23 13:07:21.459 IST [96048] LOG:  [PERF] Columnar Sync: processed 10000000 rows in 399520.248 ms (Avg: 0.0400 ms/row)
// 2026-01-23 13:07:22.569 IST [96048] LOG:  starting logical decoding for slot "sample_slot2"


