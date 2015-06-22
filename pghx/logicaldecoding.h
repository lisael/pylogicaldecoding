/* PgHacks logical decoding helper. The main structure is pghx_ld_reader.
 * It can create a replication slot, init the replication, send recieved
 * stream to a user defined callback, finish the replication and drop the
 * created slot.
 */
#ifndef PGHX_LD_H
#define PGHX_LD_H 1
#include <libpq-fe.h>
#include "utils.h"
// 10s
#define MAX_RETRY_INTERVAL 10000000


typedef struct slotStatus{
    char *slot_name;
    char *plugin;
} slotStatus;

typedef int (*stream_cb_)(void *user_data, char *data);

typedef struct pghx_ld_reader{
    // object properties
    char        *host;
    char        *port;
    char        *username;
    char        *dbname;
    char        *password;
    char        *progname;
    char        *plugin;
    char        *slot;
    char        create_slot;
    int         standby_message_timeout; // feedback interval in ms
    int64_t     connection_timeout; // retry time
    stream_cb_  stream_cb;
    void        *user_data;

    // internals
    PGconn      *conn;
    PGconn      *regularConn;
    bool        abort;
    XLogRecPtr  startpos; // where we start the replication (0/0 atm)
    XLogRecPtr  decoded_lsn; // log level successfully sent to user's callback
    XLogRecPtr  commited_lsn; // acked log level
    int64_t     last_status;
} pghx_ld_reader;

int
pghx_ld_reader_sendFeedback(pghx_ld_reader *r, int64_t now, bool force, bool replyRequested);

int
pghx_ld_reader_connect(pghx_ld_reader *r, bool replication);

/* returns reader's slot current status
 * slot_name, plugin, slot_type, datoid, database, active, xmin, catalog_xmin,
 * restart_lsn
 * return NULL on error, and a 0ed slotStatus if the slot does not exist
 *
 * TODO: finish to export data to slotStatus and expose this to python
 *
 */
slotStatus *
pghx_ld_reader_slot_status(pghx_ld_reader *r);

int
pghx_ld_reader_create_slot(pghx_ld_reader *r);

int
pghx_ld_reader_reply_keepalive(pghx_ld_reader *r, char *copybuf, int buf_len);

int
pghx_ld_reader_consume_stream(pghx_ld_reader *r, char *copybuf, int buf_len);

int
pghx_ld_reader_acknowledge(pghx_ld_reader *r);

int
pghx_ld_reader_drop_slot(pghx_ld_reader *r);

int
pghx_ld_reader_sendFeedback(pghx_ld_reader *r, int64_t now, bool force, bool replyRequested);

int
pghx_ld_reader_prepare(pghx_ld_reader *r);

int
pghx_ld_reader_init_replication(pghx_ld_reader *r);

/* inner main loop */
int
pghx_ld_reader_do_stream(pghx_ld_reader  *r);
/* main loop
 * listen on connection, call user's callbacks and send feedback to origin
 * */
int
pghx_ld_reader_stream(pghx_ld_reader *r);

int
pghx_ld_reader_stop(pghx_ld_reader *r);

int
pghx_ld_reader_init(pghx_ld_reader *r);

#endif
