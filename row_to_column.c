#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "postmaster/bgworker.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/ipc.h"

#include "executor/spi.h"
#include "utils/snapmgr.h"
#include "utils/memutils.h"

#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "replication/logicalproto.h"

#include "utils/hsearch.h"
#include "nodes/pg_list.h"

PG_MODULE_MAGIC;

/* ---------- SIGNAL HANDLING ---------- */
static volatile sig_atomic_t got_sigterm = false;

static void handle_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

/* ---------- RELATION CACHE ---------- */
#define MAX_COLS 128

typedef struct RelInfo
{
    Oid relid;
    char relname[NAMEDATALEN];
    int ncols;
    Oid coltypes[MAX_COLS];
} RelInfo;

static HTAB *relmap = NULL;

/* ---------- TXN BUFFER ---------- */
typedef struct TxnBuf
{
    List *sqls;
    struct TxnBuf *next;
} TxnBuf;

static TxnBuf *txn_head = NULL;
static TxnBuf *txn_tail = NULL;

/* Create a new transaction buffer */
static TxnBuf *txn_create(void)
{
    TxnBuf *txn = palloc0(sizeof(TxnBuf));
    txn->sqls = NIL;
    txn->next = NULL;
    return txn;
}

/* Push transaction buffer to linked list */
static void txn_push(TxnBuf *txn)
{
    if (!txn)
        return;

    if (!txn_head)
        txn_head = txn_tail = txn;
    else
    {
        txn_tail->next = txn;
        txn_tail = txn;
    }
}

/* Append SQL to a transaction buffer */
static void txn_append_sql(TxnBuf *txn, const char *sql)
{
    if (!txn || !sql)
        return;
    txn->sqls = lappend(txn->sqls, pstrdup(sql));
}

/* Execute a single transaction buffer */
static void
txn_process_buffer(TxnBuf *txn)
{
    ListCell *lc;

    if (!txn)
        return;

    foreach (lc, txn->sqls)
    {
        char *sql = lfirst(lc);

        int rc = SPI_execute(sql, false, 0);
        if (rc < 0)
            elog(ERROR, "SPI_execute failed: %s", sql);
    }

    list_free_deep(txn->sqls);
    pfree(txn);
}

/* Execute all ready transactions in the list and remove them */
static void txn_process_all(void)
{
    while (txn_head)
    {
        TxnBuf *next = txn_head->next;
        txn_process_buffer(txn_head);
        txn_head = next;
    }
    txn_tail = NULL;
}

/* ---------- HELPERS ---------- */
static bool needs_quotes(Oid typid)
{
    switch (typid)
    {
    case INT2OID:
    case INT4OID:
    case INT8OID:
    case FLOAT4OID:
    case FLOAT8OID:
    case NUMERICOID:
        return false;
    }
    return true;
}

/* ---------- PGOUTPUT DECODER ---------- */
static void decode_pgoutput(bytea *data)
{
    if (!data)
        return;

    StringInfoData msg;
    msg.data = VARDATA_ANY(data);
    msg.len = VARSIZE_ANY_EXHDR(data);
    msg.maxlen = msg.len;
    msg.cursor = 0;

    if (msg.len <= 0)
        return;

    char tag = pq_getmsgbyte(&msg);

    switch (tag)
    {
    case 'B': // BEGIN
    {
        TxnBuf *new_txn = txn_create();
        txn_push(new_txn);
        break;
    }

    case 'C': // COMMIT
        // Do nothing here; we process all committed transactions after decoding batch
        break;

    case 'R': // RELATION
    {
        bool found;
        RelInfo *r;
        Oid relid = pq_getmsgint(&msg, 4);
        pq_getmsgstring(&msg); // schema
        r = hash_search(relmap, &relid, HASH_ENTER, &found);
        r->relid = relid;
        strlcpy(r->relname, pq_getmsgstring(&msg), NAMEDATALEN);

        pq_getmsgbyte(&msg); // replica identity
        r->ncols = pq_getmsgint(&msg, 2);

        for (int i = 0; i < r->ncols; i++)
        {
            pq_getmsgbyte(&msg);
            pq_getmsgstring(&msg);
            r->coltypes[i] = pq_getmsgint(&msg, 4);
            pq_getmsgint(&msg, 4); // typmod
        }

        elog(LOG, "RELATION: %s (%d cols)", r->relname, r->ncols);
        break;
    }

    case 'I': // INSERT
    {
        if (!txn_tail)
        {
            TxnBuf *new_txn = txn_create();
            txn_push(new_txn);
        }

        TxnBuf *current_txn = txn_tail;
        Oid relid = pq_getmsgint(&msg, 4);
        char kind = pq_getmsgbyte(&msg); // N for new row
        int ncols = pq_getmsgint(&msg, 2);

        RelInfo *r = hash_search(relmap, &relid, HASH_FIND, NULL);
        if (!r)
        {
            for (int i = 0; i < ncols; i++)
            {
                char ck = pq_getmsgbyte(&msg);
                if (ck != 'n')
                {
                    int len = pq_getmsgint(&msg, 4);
                    pq_getmsgbytes(&msg, len);
                }
            }
            return;
        }

        if (strcmp(r->relname, "ddl_queue") == 0)
        {
            for (int i = 0; i < ncols; i++)
            {
                char ck = pq_getmsgbyte(&msg);
                if (ck == 'n' || ck == 'u')
                    continue;
                int len = pq_getmsgint(&msg, 4);
                const char *val = pq_getmsgbytes(&msg, len);
                if (i == 1) // second column = DDL SQL
                {
                    char *ddl_copy = palloc(len + 1);
                    memcpy(ddl_copy, val, len);
                    ddl_copy[len] = '\0';
                    txn_append_sql(current_txn, ddl_copy);
                }
            }
            return;
        }

        // Regular table INSERT
        StringInfoData sql;
        initStringInfo(&sql);
        appendStringInfo(&sql, "INSERT INTO %s_col VALUES (", r->relname);

        for (int i = 0; i < ncols; i++)
        {
            char ck = pq_getmsgbyte(&msg);
            if (ck == 'n')
                appendStringInfoString(&sql, "NULL");
            else
            {
                int len = pq_getmsgint(&msg, 4);
                const char *val = pq_getmsgbytes(&msg, len);
                const char *q = needs_quotes(r->coltypes[i]) ? "'" : "";
                appendStringInfo(&sql, "%s%.*s%s", q, len, val, q);
            }
            if (i < ncols - 1)
                appendStringInfoString(&sql, ", ");
        }
        appendStringInfoString(&sql, ");");
        txn_append_sql(current_txn, sql.data);
        pfree(sql.data);
        break;
    }

    default:
        elog(LOG, "Unknown WAL tag: %c", tag);
        break;
    }
}

/* ---------- BGWORKER MAIN ---------- */
PGDLLEXPORT void row_to_column_main(Datum arg)
{
    pqsignal(SIGTERM, handle_sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    elog(LOG, "row_to_column BGWorker started");

    while (!got_sigterm)
    {
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());
        SPI_connect();

        int ret = SPI_execute(
            "SELECT data FROM pg_logical_slot_get_binary_changes("
            "'sample_slot2', NULL, NULL, "
            "'proto_version','1', "
            "'publication_names','htap_pub')",
            true, 0);

        if (SPI_processed == 0)
        {
            WaitLatch(MyLatch,
                      WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                      1000L, 0);
            ResetLatch(MyLatch);
        }

        if (ret < 0)
            elog(WARNING, "Failed to fetch logical changes");

        // Decode all WAL messages → build transaction buffers
        for (uint64 i = 0; i < SPI_processed; i++)
        {
            bool isnull;
            Datum d = SPI_getbinval(
                SPI_tuptable->vals[i],
                SPI_tuptable->tupdesc,
                1, &isnull);
            if (!isnull)
                decode_pgoutput(DatumGetByteaP(d));
        }

        // Execute all transaction buffers in order
        txn_process_all();

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
    }

    elog(LOG, "row_to_column BGWorker exiting");
    proc_exit(0);
}

/* ---------- MODULE INIT ---------- */
void _PG_init(void)
{
    HASHCTL ctl;
    BackgroundWorker worker;

    MemSet(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(RelInfo);

    relmap = hash_create("row_to_column_relmap", 128,
                         &ctl, HASH_ELEM | HASH_BLOBS);

    MemSet(&worker, 0, sizeof(worker));
    worker.bgw_flags =
        BGWORKER_BACKEND_DATABASE_CONNECTION | BGWORKER_SHMEM_ACCESS;
    worker.bgw_start_time = BgWorkerStart_ConsistentState;
    worker.bgw_restart_time = 5;

    snprintf(worker.bgw_name, BGW_MAXLEN, "row_to_column_logger");
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "row_to_column");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "row_to_column_main");

    RegisterBackgroundWorker(&worker);
}
