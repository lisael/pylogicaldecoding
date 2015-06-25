#include <signal.h>
#include <math.h>
#include <errno.h>
#include "logicaldecoding.h"
#include "errors.h"

// globals
volatile sig_atomic_t global_abort = false;

bool verbose = true;

// not exported
struct timeval*
pghx_ld_reader_compute_wakeup(pghx_ld_reader *r, int64_t now, struct timeval *timeout);


/*
 * tell the main loop to exit at the next possible moment.
 */
void
sigint_handler(int signum)
{
    global_abort = true;
}

int
pghx_ld_reader_connect(pghx_ld_reader *r, bool replication)
{
    int			argcount = 7;	/* dbname, replication, fallback_app_name,
                                 * host, user, port, password */
    int			i;
    const char **keywords;
    const char **values;
    const char *tmpparam;
    PGconn *tmp = NULL;
    int atempts = 0;
    int64_t start_time, end_time;
    int64_t slept = 0;

    /* load map */
    i = 0;

    {
        keywords = pg_malloc0((argcount + 1) * sizeof(*keywords));
        values = pg_malloc0((argcount + 1) * sizeof(*values));
    }

    keywords[i] = "dbname";
    values[i] = r->dbname == NULL ? "replication" : r->dbname;
    i++;

    keywords[i] = "replication";
    //values[i] = r->dbname == NULL ? "true" : "database";
    values[i] = r->dbname == NULL ? "true" : replication ? "database" : "false";
    i++;

    if (r->progname)
    {
        keywords[i] = "fallback_application_name";
        values[i] = r->progname;
        i++;
    }

    if (r->host)
    {
        keywords[i] = "host";
        values[i] = r->host;
        i++;
    }

    if (r->username)
    {
        keywords[i] = "user";
        values[i] = r->username;
        i++;
    }

    if (r->port)
    {
        keywords[i] = "port";
        values[i] = r->port;
        i++;
    }

    if (r->password)
    {
        keywords[i] = "password";
        values[i] = r->password;
        i++;
    }


    start_time = feGetCurrentTimestamp();
    end_time = start_time + r->connection_timeout;
    while (!global_abort && !r->abort)
    {
        int64_t time_to_sleep;

        tmp = PQconnectdbParams(keywords, values, true);

        if (!tmp)
        {
            Pghx_set_error(PGHX_OUT_OF_MEMORY, "Could not create connection\n");
            // no possible retry
            goto error;
        }

        // TODO: perform some other checks to avoid retries on
        // not recoverable errors (e.g pg_hba misconfiguration)
        if (PQstatus(tmp) == CONNECTION_BAD && PQconnectionNeedsPassword(tmp))
        {
            Pghx_set_error(PGHX_PASSWORD_ERROR, "password needed");
            goto error;
        }

        if (PQstatus(tmp) == CONNECTION_OK )
            break;

        time_to_sleep = Min(MAX_RETRY_INTERVAL, 500000 * pow(2, atempts));
        time_to_sleep = Min(end_time - start_time - slept + 1, time_to_sleep );
        if (start_time + slept > end_time)
        {
            Pghx_format_error(PGHX_CONNECTION_ERROR,
                "Could not connect to server: %s\n",
                PQerrorMessage(tmp));
            goto error;
        }
        if (verbose)
            fprintf(stderr,
                    "cannot connect: %s\nRetry in %lims\n",
                    PQerrorMessage(tmp), time_to_sleep/1000);
        pg_usleep(time_to_sleep);
        slept = slept + time_to_sleep;
        atempts ++;
    }

    /* Connection ok! */
    free(values);
    free(keywords);

    /*
     * Ensure we have the same value of integer timestamps as the server we
     * are connecting to.
     */
    tmpparam = PQparameterStatus(tmp, "integer_datetimes");
    if (!tmpparam)
    {
        Pghx_set_error(PGHX_CONNECTION_ERROR,
                "Could not determine server setting for integer_datetimes");
        goto error;
    }

// TODO: check why pg_basebackup does check this precompiler stuff
//#ifdef HAVE_INT64_TIMESTAMP
    if (strcmp(tmpparam, "on") != 0)
//#else
//        if (strcmp(tmpparam, "off") != 0)
//#endif
        {
            Pghx_set_error(PGHX_CONNECTION_ERROR,
                    "Integer_datetimes compile flag does not match server");
            goto error;
        }
    if (replication){
        r->conn = tmp;
    }
    else
    {
        r->regularConn = tmp;
    }

    return 1;
error:
    free(values);
    free(keywords);
    if (tmp)
        PQfinish(tmp);
    if (r->conn)
    {
        PQfinish(r->conn);
        r->conn = NULL;
    }
    if (r->regularConn)
    {
        PQfinish(r->regularConn);
        r->regularConn = NULL;
    }
    return 0;
}

/* returns reader's slot current status
 * slot_name, plugin, slot_type, datoid, database, active, xmin, catalog_xmin,
 * restart_lsn
 * return NULL on error, and a 0ed slotStatus if the slot does not exist
 *
 * TODO: finish to export data to slotStatus.
 *
 */
slotStatus *
pghx_ld_reader_slot_status(pghx_ld_reader *r)
{
    char		query[256];
    PGresult    *res= NULL;
    slotStatus  *status = NULL;

    snprintf(query, sizeof(query),
            "SELECT * FROM pg_replication_slots WHERE slot_name='%s'",
                r->slot);

    if (!r->regularConn && !pghx_ld_reader_connect(r, false))
        goto error;

    res = PQexec(r->regularConn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        char *err_code = PQresultErrorField(res,PG_DIAG_SQLSTATE);
        Pghx_format_error(PGHX_QUERY_ERROR,
                "%s: could not send status command \"%s\": %s\n%s",
                r->progname, query, PQerrorMessage(r->regularConn), err_code);
        goto error;
    }

    if (PQntuples(res) > 1 || PQnfields(res) != 9)
    {
        /*char *err_code = PQresultErrorField(res, PG_DIAG_SQLSTATE);*/
        Pghx_format_error(PGHX_LD_STATUS_ERROR,
                "%s: wrong status field number \"%s\": got %d rows and %d fields, expected %d rows and %d fields\n",
                r->progname, r->slot, PQntuples(res), PQnfields(res), 1, 9);
        goto error;
    }


    status = pg_malloc0(sizeof(slotStatus));

    // the slot exists
    if (PQntuples(res) != 0)
    {
        // only slot_name and plugin ATM TODO:
        status->slot_name = strdup(PQgetvalue(res, 0, 0));
        status->plugin = strdup(PQgetvalue(res, 0, 1));
    }
    else{
        status->slot_name = "";
    }

    PQclear(res);
    return status;

error:
    PQfinish(r->regularConn);
    r->regularConn = NULL;
    PQclear(res);
    if (status)
        free(status);
    return NULL;
}

int
pghx_ld_reader_create_slot(pghx_ld_reader *r)
{
    char		query[256];
    PGresult    *res = NULL;
    uint32_t    hi,
                lo;

    if (!r->conn && !pghx_ld_reader_connect(r, true))
        // exception is setted in reader_connect...
        goto error;


    if (verbose)
        fprintf(stderr,
                "%s: creating replication slot \"%s\"\n",
                r->progname, r->slot);

    snprintf(query, sizeof(query), "CREATE_REPLICATION_SLOT \"%s\" LOGICAL \"%s\"",
                r->slot, r->plugin);

    res = PQexec(r->conn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        char *err_code = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        Pghx_format_error(PGHX_LD_REPLICATION_ERROR,
                "%s: could not send replication command \"%s\": %s %s\n",
                r->progname, query, PQerrorMessage(r->conn), err_code);
        goto error;
    }

    if (PQntuples(res) != 1 || PQnfields(res) != 4)
    {
        char *err_code = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        Pghx_format_error(PGHX_LD_REPLICATION_ERROR,
                "%s: could not create replication slot \"%s\": got %d rows and %d fields, expected %d rows and %d fields\n%s\n",
                r->progname, r->slot, PQntuples(res), PQnfields(res), 1, 4, err_code);
        goto error;
    }

    if (sscanf(PQgetvalue(res, 0, 1), "%X/%X", &hi, &lo) != 2)
    {
        char *err_code = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        Pghx_format_error(PGHX_LD_STREAM_PROTOCOL_ERROR,
                "%s: could not parse transaction log location \"%s\"\n%s\n",
                r->progname, PQgetvalue(res, 0, 1), err_code);
        goto error;
    }
    r->startpos = ((uint64_t) hi) << 32 | lo;

    // pg_recvlogical does this, I'm quite not sure why
    r->slot = strdup(PQgetvalue(res, 0, 0));

    PQclear(res);

    return 1;
error:
    if(res)
        PQclear(res);
    if (r->conn)
    {
        PQfinish(r->conn);
        r->conn = NULL;
    }
    return 0;
}

/* Compute when we need to wakeup to send a keepalive message. */
struct timeval*
pghx_ld_reader_compute_wakeup(pghx_ld_reader *r, int64_t now, struct timeval *timeout)
{
    int64_t message_target = 0;

    if (r->standby_message_timeout)
        message_target = r->last_status + (r->standby_message_timeout - 1) *
            ((int64_t) 1000);

    /* Now compute when to wakeup. */
    if (message_target > 0 )
    {
        int64_t     targettime;
        long        secs;
        int         usecs;

        targettime = message_target;

        feTimestampDifference(now,
                targettime,
                &secs,
                &usecs);
        if (secs <= 0)
            timeout->tv_sec = 1; /* Always sleep at least 1 sec */
        else
            timeout->tv_sec = secs;
        timeout->tv_usec = usecs;
        return timeout;
    }
    return NULL;
}

int
pghx_ld_reader_reply_keepalive(pghx_ld_reader *r, char *copybuf, int buf_len)
{
    int         pos;
    bool        replyRequested;
    XLogRecPtr  walEnd;

    /*
     * Parse the keepalive message, enclosed in the CopyData message.
     * We just check if the server requested a reply, and ignore the
     * rest.
     */
    pos = 1;  /* skip msgtype 'k' */
    walEnd = fe_recvint64(&copybuf[pos]);
    r->decoded_lsn = Max(walEnd, r->decoded_lsn);
    pos += 8;  /* read walEnd */
    pos += 8;  /* skip sendTime */

    if (buf_len < pos + 1)
    {
        Pghx_format_error(PGHX_LD_STREAM_PROTOCOL_ERROR,
                "Streaming header too small: %d\n",
                buf_len);
        return 0;
    }
    replyRequested = copybuf[pos];

    /* If the server requested an immediate reply, send one. */
    if (replyRequested)
    {
        int64_t now = feGetCurrentTimestamp();
        if (!pghx_ld_reader_sendFeedback(r, now, true, false))
        {
            return 0;
        }
        r->last_status = now;
    }
    return 1;
}

int
pghx_ld_reader_consume_stream(pghx_ld_reader *r, char *copybuf, int buf_len)
{
    int hdr_len;
    XLogRecPtr old_lsn;
    stream_cb_ callback = r->stream_cb;

    /*
     * Read the header of the XLogData message, enclosed in the CopyData
     * message. We only need the WAL location field (dataStart), the rest
     * of the header is ignored.
     */
    hdr_len = 1;			/* msgtype 'w' */
    hdr_len += 8;			/* dataStart */
    hdr_len += 8;			/* walEnd */
    hdr_len += 8;			/* sendTime */
    if (buf_len < hdr_len + 1)
    {
        Pghx_format_error(PGHX_LD_STREAM_PROTOCOL_ERROR,
                "Streaming header too small: %d\n",
                buf_len);
        return 0;
    }

    /* Extract WAL location for this block */
    {
        XLogRecPtr	temp = fe_recvint64(&copybuf[1]);
        old_lsn = r->decoded_lsn;
        r->decoded_lsn = Max(temp, r->decoded_lsn);
    }


    /* call users callback */
    if (callback)
    {
        if (!(*callback)(r->user_data, copybuf + hdr_len))
        {
            if(old_lsn)
                r->decoded_lsn = old_lsn;
            return 0;
        }
    }
    return 1;
}

int
pghx_ld_reader_acknowledge(pghx_ld_reader *r)
{
    int64_t now = feGetCurrentTimestamp();
    XLogRecPtr old_lsn = r->commited_lsn;

    r->commited_lsn = r->decoded_lsn;
    if (!pghx_ld_reader_sendFeedback(r, now, true, false))
    {
        r->commited_lsn = old_lsn;
        return 0;
    }
    return 1;
}

int
pghx_ld_reader_drop_slot(pghx_ld_reader *r)
{
    char    query[256];
    PGresult    *res = NULL;

    if (!r->conn && !pghx_ld_reader_connect(r, true))
        goto error;

    if (verbose)
        fprintf(stderr,
                "%s: dropping replication slot \"%s\"\n",
                r->progname, r->slot);

    snprintf(query, sizeof(query), "DROP_REPLICATION_SLOT \"%s\"",
            r->slot);
    res = PQexec(r->conn, query);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        Pghx_format_error(PGHX_COMMAND_ERROR,
                "%s: could not send replication command \"%s\": %s",
                r->progname, query, PQerrorMessage(r->conn));
        goto error;
    }

    if (PQntuples(res) != 0 || PQnfields(res) != 0)
    {
        Pghx_format_error(PGHX_COMMAND_ERROR,
                "%s: could not drop replication slot \"%s\": got %d rows and %d fields, expected %d rows and %d fields\n",
                r->progname, r->slot, PQntuples(res), PQnfields(res), 0, 0);
        goto error;
    }
    PQclear(res);
    return 1;
error:
    if(res)
        PQclear(res);
    if (r->conn)
    {
        PQfinish(r->conn);
        r->conn = NULL;
    }
    return 0;
}

int
pghx_ld_reader_sendFeedback(pghx_ld_reader *r, int64_t now, bool force, bool replyRequested)
{
    char		replybuf[1 + 8 + 8 + 8 + 8 + 1];
    int			len = 0;


    if (!force &&
            r->decoded_lsn == r->commited_lsn)
        return 1;
    if (verbose)
    {
        printf("feedback... %X/%X\n",
                    (uint32_t) (r->commited_lsn >> 32), (uint32_t) r->commited_lsn);
        fflush(stdout);
    }

    replybuf[len] = 'r';
    len += 1;
    fe_sendint64(r->commited_lsn, &replybuf[len]);	/* write */
    len += 8;
    fe_sendint64(r->commited_lsn, &replybuf[len]);		/* flush */
    len += 8;
    fe_sendint64(InvalidXLogRecPtr, &replybuf[len]);	/* apply */
    len += 8;
    fe_sendint64(now, &replybuf[len]);	/* sendTime */
    len += 8;
    replybuf[len] = replyRequested ? 1 : 0;		/* replyRequested */
    len += 1;
    if (!r->conn && !pghx_ld_reader_connect(r, true))
    {
        // exception is setted in reader_connect...
        return 0;
    }
    // TODO: this doesn't seem to detect broken connection...
    if (PQputCopyData(r->conn, replybuf, len) <= 0
            || PQflush(r->conn))
    {
        Pghx_format_error(PGHX_CONNECTION_ERROR,
                    "Could not send feedback packet: %s",
                    PQerrorMessage(r->conn));
        if (r->conn)
        {
            PQfinish(r->conn);
            r->conn = NULL;
        }
        return 0;
    }
    return 1;
}

/* check that the slot exists and create the slot if needed */
int
pghx_ld_reader_prepare(pghx_ld_reader *r)
{
    slotStatus  *status = pghx_ld_reader_slot_status(r);

    // got an error
    if (!status)
        goto error;

    // no slot
    if(strlen(status->slot_name) == 0){
        // create the slot if requested
        if (r->create_slot)
        {
            if (!pghx_ld_reader_create_slot(r))
            {
                goto error;
            }
            r->create_slot = 0;
        }
        else
        {
            Pghx_format_error(PGHX_LD_NO_SLOT,
                    "Slot \"%s\" does not exist",
                    r->slot);
            goto error;
        }
    }
    else
    {
        // check plugin name
        if (strcmp(r->plugin, status->plugin))
        {
            Pghx_format_error(PGHX_LD_BAD_PLUGIN,
                    "Slot \"%s\" uses pluguin \"%s\". You required \"%s\"",
                    r->slot, status->plugin, r->plugin);
            goto error;
        }
    }
    free(status);
    return 1;
error:
    if (status)
        free(status);
    return 0;
}

int
pghx_ld_reader_init_replication(pghx_ld_reader *r)
{
    PGresult    *res = NULL;
    int         i;
    PQExpBuffer query;

    // TODO: these should be pghx_ld_reader fields
    char        **options=NULL;
    int         noptions = 0;

    query = createPQExpBuffer();

    /*
     * Start the replication
     */
    if (verbose)
        fprintf(stderr,
                "%s: starting log streaming at %X/%X (slot %s)\n",
                r->progname, (uint32_t) (r->startpos >> 32), (uint32_t) r->startpos,
                r->slot);

    /* Initiate the replication stream at specified location */
    appendPQExpBuffer(query, "START_REPLICATION SLOT \"%s\" LOGICAL %X/%X",
            r->slot, (uint32_t) (r->startpos >> 32), (uint32_t) r->startpos);

    /* print options if there are any */
    if (noptions)
        appendPQExpBufferStr(query, " (");

    for (i = 0; i < noptions; i++)
    {
        /* separator */
        if (i > 0)
            appendPQExpBufferStr(query, ", ");

        /* write option name */
        appendPQExpBuffer(query, "\"%s\"", options[(i * 2)]);

        /* write option value if specified */
        if (options[(i * 2) + 1] != NULL)
            appendPQExpBuffer(query, " '%s'", options[(i * 2) + 1]);
    }

    if (noptions)
        appendPQExpBufferChar(query, ')');

    if (verbose)
        fprintf(stderr, "%s\n", query->data);

    res = PQexec(r->conn, query->data);
    if (PQresultStatus(res) != PGRES_COPY_BOTH)
    {
        char *error_msg = PQresultErrorMessage(res);
        Pghx_format_error(PGHX_COMMAND_ERROR,
                "Could not send replication command \"%s\": %s",
                query->data, error_msg);
        destroyPQExpBuffer(query);
        return 0;
    }

    destroyPQExpBuffer(query);

    if (verbose)
        fprintf(stderr,
                "%s: streaming initiated\n",
                r->progname);
    return 1;
}

/* inner main loop */
int
pghx_ld_reader_do_stream(pghx_ld_reader  *r)
{
    PGresult    *res = NULL;
    char        *copybuf = NULL;

    while (!global_abort && !r->abort)
    {
        int         buf_len;
        int64_t     now;

        if (copybuf != NULL)
        {
            PQfreemem(copybuf);
            copybuf = NULL;
        }

        /*
         * Potentially send a status message to the master
         */
        now = feGetCurrentTimestamp();

        if (r->standby_message_timeout > 0 &&
                feTimestampDifferenceExceeds(r->last_status, now,
                    r->standby_message_timeout))
        {
            /* Time to send feedback! */
            if (!pghx_ld_reader_sendFeedback(r, now, true, false))
            {
                goto error;
            }

            r->last_status = now;
        }

        buf_len = PQgetCopyData(r->conn, &copybuf, 1);
        if (buf_len == 0)
        {
            /*
             * In async mode, and no data available. We block on reading but
             * not more than the specified timeout, so that we can send a
             * response back to the client.
             */
            fd_set          input_mask;
            struct timeval  timeout;
            struct timeval  *timeoutptr = NULL;

            timeoutptr = pghx_ld_reader_compute_wakeup(r, now, &timeout);

            FD_ZERO(&input_mask);
            FD_SET(PQsocket(r->conn), &input_mask);

            buf_len = select(PQsocket(r->conn) + 1, &input_mask, NULL, NULL, timeoutptr);
            if (buf_len == 0 || (buf_len < 0 && errno == EINTR))
            {
                /*
                 * Got a timeout or signal. Continue the loop and either
                 * deliver a status packet to the server or just go back into
                 * blocking.
                 */
                continue;
            }
            else if (buf_len < 0)
            {
                Pghx_format_error(PGHX_IO_ERROR,
                        "select() failed: %s\n",
                        strerror(errno));
                goto error;
            }

            /* Else there is actually data on the socket */
            if (PQconsumeInput(r->conn) == 0)
            {
                Pghx_format_error(PGHX_IO_ERROR,
                        "Could not receive data from WAL stream:\t%s",
                        PQerrorMessage(r->conn));
                goto error;
            }
            continue;
        }

        /* End of copy stream */
        if (buf_len == -1)
        {
            break;
        }

        /* Failure while reading the copy stream */
        if (buf_len == -2)
        {
            Pghx_format_error(PGHX_IO_ERROR,
                    "Could not read COPY data: %s",
                    PQerrorMessage(r->conn));
            goto error;
        }

        /* Check the message type. */
        if (copybuf[0] == 'k')
        {
            if (!pghx_ld_reader_reply_keepalive(r, copybuf, buf_len)){
                goto error;
            }
            continue;
        }
        else if (copybuf[0] != 'w')
        {
            Pghx_format_error(PGHX_LD_STREAM_PROTOCOL_ERROR,
                    "Unrecognized streaming header: \"%c\"\n",
                    copybuf[0]);
            goto error;
        }

        if (!pghx_ld_reader_consume_stream(r, copybuf, buf_len))
            goto error;
    }

    res = PQgetResult(r->conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        Pghx_format_error(PGHX_IO_ERROR,
                "unexpected termination of replication stream: %s",
                PQresultErrorMessage(res));
        goto error;
    }
    PQclear(res);
    PQfinish(r->conn);
    r->conn = NULL;
    return 1;

error:
    if (copybuf != NULL)
    {
        PQfreemem(copybuf);
        copybuf = NULL;
    }
    if (res)
        PQclear(res);
    PQfinish(r->conn);
    r->conn = NULL;
    return 0;
}

/* main loop
 * listen on connection, call user's callbacks and send feedback to origin
 * */
int
pghx_ld_reader_stream(pghx_ld_reader *r)
{

    if (!pghx_ld_reader_prepare(r))
        return 0;

    r->abort = false;

    while (!global_abort && !r->abort)
    {
        int64_t     now;

        if (!r->conn && !pghx_ld_reader_connect(r, true))
            return 0;

        if (!pghx_ld_reader_init_replication(r))
            // TODO: implement retry
            return 0;

        now = feGetCurrentTimestamp();
        if (!pghx_ld_reader_sendFeedback(r, now, true, false))
        {
            return 0;
        }

        if (!pghx_ld_reader_do_stream(r))
        {
            if (r->conn)
            {
                PQfinish(r->conn);
                r->conn = NULL;
            }
            continue;
        }
    }
    // TODO: cleanup, dorp slot if the user said so
    return 1;
}


int
pghx_ld_reader_stop(pghx_ld_reader *r)
{
    r->abort = true;
    return 1;
}

int
pghx_ld_reader_init(pghx_ld_reader *r)
{
    signal(SIGINT, sigint_handler);
    r->host = r->port = r->username = r->password = NULL;
    r->dbname = "postgres";
    r->progname = "pghx";
    r->plugin = "test_decoding";
    r->slot = "test_slot";
    r->create_slot = 1;
    r->startpos = InvalidXLogRecPtr;
    r->standby_message_timeout = 10 * 1000;
    r->connection_timeout = 60 * 1000 * 1000; // 1 minute
    r->decoded_lsn = InvalidXLogRecPtr;
    r->last_status = -1;
    r->stream_cb = NULL;
    r->user_data = NULL;
    return 1;
}
