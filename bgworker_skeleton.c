#include "postgres.h"

/* Essential PG17 headers */
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "pgstat.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "miscadmin.h"

PG_MODULE_MAGIC;

void _PG_init(void);
PGDLLEXPORT void row_to_column_main(Datum main_arg) pg_attribute_noreturn();

// PG_FUNCTION_INFO_V1(row_to_column_main); // Optional but recommended

/* Signal handling */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

static void row_to_column_sighup(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sighup = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void row_to_column_sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

PGDLLEXPORT void row_to_column_main(Datum main_arg) {
    pqsignal(SIGHUP, row_to_column_sighup);
    pqsignal(SIGTERM, row_to_column_sigterm);
    BackgroundWorkerUnblockSignals();

    /* Ensure the database "postgres" exists or change to your DB name */
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    ereport(LOG, (errmsg("bgworker row_to_column started")));

    while (!got_sigterm) {
        WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 5000L, PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);

        if (got_sighup) {
            got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
        
        CHECK_FOR_INTERRUPTS();
    }
    proc_exit(0);
}

void _PG_init(void) {
    BackgroundWorker worker;

    memset(&worker, 0, sizeof(worker));
    /* BGWORKER_SHMEM_ACCESS is fine here as long as you don't call RequestWorkerCount */
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_ConsistentState;
    worker.bgw_restart_time = 60;
    
    snprintf(worker.bgw_name, BGW_MAXLEN, "row_to_column_sync worker");
    snprintf(worker.bgw_type, BGW_MAXLEN, "row_to_column_sync");
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "row_to_column");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "row_to_column_main");
    
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&worker);
}