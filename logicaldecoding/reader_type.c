#include <signal.h>
#include "logicaldecoding.h"
#include "connection.h"
#include "streamutils.h"

#define reader_stream_doc \
"stream() -> start the main loop"

#define reader_stop_doc \
"stop() -> stop the main loop"

#define reader_commit_doc \
"commit() -> send feedback message acknowledging all preceding stream\n"\
"It's user's responsability to send regular acknowledgemnts. If"\
"ommited, the master keeps all it's WAL on disk and eventually"\
"Cthulhu eats the physical server"

#define reader_drop_slot_doc \
"drop_slot() -> drop the replication slot"


static volatile sig_atomic_t global_abort = false;

bool verbose = true;

typedef struct slotStatus{
    char *slot_name;
    char *plugin;
} slotStatus;

typedef struct readerObject{
    PyObject_HEAD

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

    // internals
    PGconn      *conn;
    PGconn      *regularConn;
    bool        abort;
    XLogRecPtr  startpos; // where we start the replication (0/0 atm)
    XLogRecPtr  decoded_lsn; // log level successfully sent to user's callback
    XLogRecPtr  commited_lsn; // acked log level
    int64_t     last_status;
} readerObject;

static int
reader_sendFeedback(readerObject *self, int64_t now, bool force, bool replyRequested);
int
reader_commit(readerObject *self);

/*
 * tell the main loop to exit at the next possible moment.
 */
static void
sigint_handler(int signum)
{
    global_abort = true;
}

static int
reader_connect(readerObject *self, bool replication)
{
    int			argcount = 7;	/* dbname, replication, fallback_app_name,
                                 * host, user, port, password */
    int			i;
    const char **keywords;
    const char **values;
    const char *tmpparam;
    PGconn *tmp;

    /* load map */
    i = 0;

    {
        keywords = pg_malloc0((argcount + 1) * sizeof(*keywords));
        values = pg_malloc0((argcount + 1) * sizeof(*values));
    }

    keywords[i] = "dbname";
    values[i] = self->dbname == NULL ? "replication" : self->dbname;
    i++;

    keywords[i] = "replication";
    //values[i] = self->dbname == NULL ? "true" : "database";
    values[i] = self->dbname == NULL ? "true" : replication ? "database" : "false";
    i++;

    if (self->progname)
    {
        keywords[i] = "fallback_application_name";
        values[i] = self->progname;
        i++;
    }

    if (self->host)
    {
        keywords[i] = "host";
        values[i] = self->host;
        i++;
    }

    if (self->username)
    {
        keywords[i] = "user";
        values[i] = self->username;
        i++;
    }

    if (self->port)
    {
        keywords[i] = "port";
        values[i] = self->port;
        i++;
    }

    if (self->password)
    {
        keywords[i] = "password";
        values[i] = self->password;
        i++;
    }

    tmp = PQconnectdbParams(keywords, values, true);

    if (!tmp)
    {
        PyErr_NoMemory();
        goto error;
    }

    if (PQstatus(tmp) == CONNECTION_BAD)
    {
        if (PQconnectionNeedsPassword(tmp))
        {
            PyErr_SetString(PyExc_ValueError, "password needed");
            goto error;
        }
    }

    if (PQstatus(tmp) != CONNECTION_OK)
    {
        PyErr_Format(PyExc_IOError,
            "Could not connect to server: %s\n",
             PQerrorMessage(tmp));
        goto error;
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
        PyErr_SetString(PyExc_ValueError,
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
            PyErr_SetString(PyExc_ValueError,
                    "Integer_datetimes compile flag does not match server");
            goto error;
        }
    if (replication){
        self->conn = tmp;
    }
    else
    {
        self->regularConn = tmp;
    }

    return 1;
error:
    free(values);
    free(keywords);
    if (tmp)
        PQfinish(tmp);
    if (self->conn)
        PQfinish(self->conn);
    if (self->regularConn)
        PQfinish(self->regularConn);
    return 0;
}

/* returns reader's slot current status
 * slot_name, plugin, slot_type, datoid, database, active, xmin, catalog_xmin,
 * restart_lsn
 * return NULL on error, and a 0ed slotStatus if the slot does not exist
 *
 * TODO: finish to export data to slotStatus and expose this to python
 *
 */
static slotStatus *
reader_slot_status(readerObject *self)
{
    char		query[256];
    PGresult    *res= NULL;
    slotStatus  *status = NULL;

    snprintf(query, sizeof(query),
            "SELECT * FROM pg_replication_slots WHERE slot_name='%s'",
                self->slot);

    if (!self->regularConn && !reader_connect(self, false))
        goto error;

    res = PQexec(self->regularConn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        char *err_code = PQresultErrorField(res,PG_DIAG_SQLSTATE);
        PyErr_Format(PyExc_ValueError,
                "%s: could not send status command \"%s\": %s\n%s",
                self->progname, query, PQerrorMessage(self->regularConn), err_code);
        goto error;
    }

    if (PQntuples(res) > 1 || PQnfields(res) != 9)
    {
        /*char *err_code = PQresultErrorField(res, PG_DIAG_SQLSTATE);*/
        PyErr_Format(PyExc_ValueError,
                "%s: wrong status field number \"%s\": got %d rows and %d fields, expected %d rows and %d fields\n",
                self->progname, self->slot, PQntuples(res), PQnfields(res), 1, 9);
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
    PQfinish(self->regularConn);
    self->regularConn = NULL;
    PQclear(res);
    if (status)
        free(status);
    return NULL;
}

static int
reader_create_slot(readerObject *self)
{
    char		query[256];
    PGresult    *res = NULL;
    uint32_t    hi,
                lo;

    if (!self->conn && !reader_connect(self, true))
        // exception is setted in reader_connect...
        goto error;


    if (verbose)
        fprintf(stderr,
                "%s: creating replication slot \"%s\"\n",
                self->progname, self->slot);

    snprintf(query, sizeof(query), "CREATE_REPLICATION_SLOT \"%s\" LOGICAL \"%s\"",
                self->slot, self->plugin);

    res = PQexec(self->conn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        char *err_code = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        PyErr_Format(PyExc_ValueError,
                "%s: could not send replication command \"%s\": %s %s\n",
                self->progname, query, PQerrorMessage(self->conn), err_code);
        goto error;
    }

    if (PQntuples(res) != 1 || PQnfields(res) != 4)
    {
        char *err_code = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        PyErr_Format(PyExc_ValueError,
                "%s: could not create replication slot \"%s\": got %d rows and %d fields, expected %d rows and %d fields\n%s\n",
                self->progname, self->slot, PQntuples(res), PQnfields(res), 1, 4, err_code);
        goto error;
    }

    if (sscanf(PQgetvalue(res, 0, 1), "%X/%X", &hi, &lo) != 2)
    {
        char *err_code = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        PyErr_Format(PyExc_ValueError,
                "%s: could not parse transaction log location \"%s\"\n%s\n",
                self->progname, PQgetvalue(res, 0, 1), err_code);
        goto error;
    }
    self->startpos = ((uint64_t) hi) << 32 | lo;

    // pg_recvlogical does this, I'm quite not sure why
    self->slot = strdup(PQgetvalue(res, 0, 0));

    PQclear(res);

    return 1;
error:
    if(res)
        PQclear(res);
    return 0;
}

/* Compute when we need to wakeup to send a keepalive message. */
struct timeval*
reader_compute_wakeup(readerObject *self, int64_t now, struct timeval *timeout)
{
    int64_t message_target = 0;

    if (self->standby_message_timeout)
        message_target = self->last_status + (self->standby_message_timeout - 1) *
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
reader_reply_keepalive(readerObject *self, char *copybuf, int buf_len)
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
    self->decoded_lsn = Max(walEnd, self->decoded_lsn);
    pos += 8;  /* read walEnd */
    pos += 8;  /* skip sendTime */

    if (buf_len < pos + 1)
    {
        PyErr_Format(PyExc_ValueError,
                "Streaming header too small: %d\n",
                buf_len);
        return 0;
    }
    replyRequested = copybuf[pos];

    /* If the server requested an immediate reply, send one. */
    if (replyRequested)
    {
        int64_t now = feGetCurrentTimestamp();
        if (!reader_sendFeedback(self, now, true, false))
        {
            return 0;
        }
        self->last_status = now;
    }
    return 1;
}

int
reader_consume_stream(readerObject *self, char *copybuf, int buf_len)
{
    PyObject    *pFunc = NULL,
                *pArgs = NULL,
                *pValue = NULL,
                *result = NULL;
    int hdr_len;
    XLogRecPtr old_lsn;
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
        PyErr_Format(PyExc_ValueError,
                "Streaming header too small: %d\n",
                buf_len);
        return 0;
    }

    /* Extract WAL location for this block */
    {
        XLogRecPtr	temp = fe_recvint64(&copybuf[1]);
        old_lsn = self->decoded_lsn;
        self->decoded_lsn = Max(temp, self->decoded_lsn);
    }


    /* call users callback */
    {
        pFunc = PyObject_GetAttrString((PyObject *)self, "event");
        if (pFunc == NULL){goto error;}
        pArgs = PyTuple_New(1);
        if (pArgs == NULL){goto error;}
        pValue = Text_FromUTF8(copybuf + hdr_len);
        Py_INCREF(pValue);
        if (pValue == NULL){goto error;}
        PyTuple_SetItem(pArgs, 0, pValue);
        result = PyObject_CallObject(pFunc, pArgs);
        if (result == NULL){goto error;}
        old_lsn = 0;
        Py_DECREF(pFunc);
        Py_DECREF(pArgs);
        Py_DECREF(pValue);
        Py_DECREF(result);
    }
    return 1;

error:
    if(old_lsn){
        self->decoded_lsn = old_lsn;
    }
    Py_XDECREF(pFunc);
    Py_XDECREF(pArgs);
    Py_XDECREF(pValue);
    Py_XDECREF(result);
    return 0;
}

int
reader_commit(readerObject *self)
{
    int64_t now = feGetCurrentTimestamp();
    XLogRecPtr old_lsn = self->commited_lsn;

    self->commited_lsn = self->decoded_lsn;
    if (!reader_sendFeedback(self, now, true, false))
    {
        self->commited_lsn = old_lsn;
        return 0;
    }
    return 1;
}

int
reader_drop_slot(readerObject *self)
{
    char    query[256];
    PGresult    *res = NULL;

    if (!self->conn && !reader_connect(self, true))
        goto error;

    if (verbose)
        fprintf(stderr,
                "%s: dropping replication slot \"%s\"\n",
                self->progname, self->slot);

    snprintf(query, sizeof(query), "DROP_REPLICATION_SLOT \"%s\"",
            self->slot);
    res = PQexec(self->conn, query);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        PyErr_Format(PyExc_ValueError,
                "%s: could not send replication command \"%s\": %s",
                self->progname, query, PQerrorMessage(self->conn));
        goto error;
    }

    if (PQntuples(res) != 0 || PQnfields(res) != 0)
    {
        PyErr_Format(PyExc_ValueError,
                "%s: could not drop replication slot \"%s\": got %d rows and %d fields, expected %d rows and %d fields\n",
                self->progname, self->slot, PQntuples(res), PQnfields(res), 0, 0);
        goto error;
    }
    PQclear(res);
    return 1;
error:
    if(res)
        PQclear(res);
    return 0;
}

static int
reader_sendFeedback(readerObject *self, int64_t now, bool force, bool replyRequested)
{
    char		replybuf[1 + 8 + 8 + 8 + 8 + 1];
    int			len = 0;


    if (!force &&
            self->decoded_lsn == self->commited_lsn)
        return 1;
    if (verbose)
    {
        printf("feedback... %X/%X\n",
                    (uint32_t) (self->commited_lsn >> 32), (uint32_t) self->commited_lsn);
        fflush(stdout);
    }

    replybuf[len] = 'r';
    len += 1;
    fe_sendint64(self->commited_lsn, &replybuf[len]);	/* write */
    len += 8;
    fe_sendint64(self->commited_lsn, &replybuf[len]);		/* flush */
    len += 8;
    fe_sendint64(InvalidXLogRecPtr, &replybuf[len]);	/* apply */
    len += 8;
    fe_sendint64(now, &replybuf[len]);	/* sendTime */
    len += 8;
    replybuf[len] = replyRequested ? 1 : 0;		/* replyRequested */
    len += 1;
    if (!self->conn && !reader_connect(self, true))
        // exception is setted in reader_connect...
        return 0;
    if (PQputCopyData(self->conn, replybuf, len) <= 0
            || PQflush(self->conn))
    {
        PyErr_Format(PyExc_IOError,
                    "Could not send feedback packet: %s",
                    PQerrorMessage(self->conn));
        return 0;
    }
    return 1;
}

/* main loop
 * listen on connection, call user's callbacks and send feedback to origin
 * */
int
reader_stream(readerObject *self)
{
    PGresult    *res = NULL;
    char        *copybuf = NULL;
    int         i;
    PQExpBuffer query;

    char        **options=NULL;

    int         noptions = 0;
    slotStatus  *status = NULL;

    static bool first_loop = true;

    self->abort = false;
    query = createPQExpBuffer();

    /*
     * check slot and create if it doesn't exist
     */
    status = reader_slot_status(self);
    // got an error
    if (!status)
        goto error;
    // no slot
    if(strlen(status->slot_name) == 0){
        // create the slot if requested
        if (self->create_slot)
        {
            if (!reader_create_slot(self))
            {
                goto error;
            }
            self->create_slot = 0;
        }
        else
        {
            PyErr_Format(PyExc_ValueError,
                    "Slot \"%s\" does not exist",
                    self->slot);
            goto error;
        }
    }
    else
    {
        // check plugin name
        if (strcmp(self->plugin, status->plugin))
        {
            PyErr_Format(PyExc_ValueError,
                    "Slot \"%s\" uses pluguin \"%s\". You required \"%s\"",
                    self->slot, status->plugin, self->plugin);
            goto error;
        }
    }


    /*
     * Connect in replication mode to the server
     */
    if (!self->conn && !reader_connect(self, true))
        // exception is setted in reader_connect...
        goto error;

    /*
     * Start the replication
     */
    if (verbose)
        fprintf(stderr,
                "%s: starting log streaming at %X/%X (slot %s)\n",
                self->progname, (uint32_t) (self->startpos >> 32), (uint32_t) self->startpos,
                self->slot);

    /* Initiate the replication stream at specified location */
    appendPQExpBuffer(query, "START_REPLICATION SLOT \"%s\" LOGICAL %X/%X",
            self->slot, (uint32_t) (self->startpos >> 32), (uint32_t) self->startpos);

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

    res = PQexec(self->conn, query->data);
    if (PQresultStatus(res) != PGRES_COPY_BOTH)
    {
        char *error_msg = PQresultErrorMessage(res);
        PyErr_Format(PyExc_ValueError,
                "Could not send replication command \"%s\": %s",
                query->data, error_msg);
        goto error;
    }

    resetPQExpBuffer(query);

    if (verbose)
        fprintf(stderr,
                "%s: streaming initiated\n",
                self->progname);

    while (!global_abort && !self->abort)
    {
        int         r;
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

        if (first_loop || (self->standby_message_timeout > 0 &&
                feTimestampDifferenceExceeds(self->last_status, now,
                    self->standby_message_timeout)))
        {
            /* Time to send feedback! */
            if (!reader_sendFeedback(self, now, true, false))
            {
                goto error;
            }

            self->last_status = now;
        }
        first_loop=false;

        r = PQgetCopyData(self->conn, &copybuf, 1);
        if (r == 0)
        {
            /*
             * In async mode, and no data available. We block on reading but
             * not more than the specified timeout, so that we can send a
             * response back to the client.
             */
            fd_set          input_mask;
            struct timeval  timeout;
            struct timeval  *timeoutptr = NULL;

            timeoutptr = reader_compute_wakeup(self, now, &timeout);

            FD_ZERO(&input_mask);
            FD_SET(PQsocket(self->conn), &input_mask);

            r = select(PQsocket(self->conn) + 1, &input_mask, NULL, NULL, timeoutptr);
            if (r == 0 || (r < 0 && errno == EINTR))
            {
                /*
                 * Got a timeout or signal. Continue the loop and either
                 * deliver a status packet to the server or just go back into
                 * blocking.
                 */
                continue;
            }
            else if (r < 0)
            {
                PyErr_Format(PyExc_ValueError,
                        "select() failed: %s\n",
                        strerror(errno));
                goto error;
            }

            /* Else there is actually data on the socket */
            if (PQconsumeInput(self->conn) == 0)
            {
                PyErr_Format(PyExc_ValueError,
                        "Could not receive data from WAL stream:\t%s",
                        PQerrorMessage(self->conn));
                goto error;
            }
            continue;
        }

        /* End of copy stream */
        if (r == -1)
        {
            break;
        }

        /* Failure while reading the copy stream */
        if (r == -2)
        {
            PyErr_Format(PyExc_ValueError,
                    "Could not read COPY data: %s",
                    PQerrorMessage(self->conn));
            goto error;
        }

        /* Check the message type. */
        if (copybuf[0] == 'k')
        {
            if (!reader_reply_keepalive(self, copybuf, r)){
                goto error;
            }
            continue;
        }
        else if (copybuf[0] != 'w')
        {
            PyErr_Format(PyExc_ValueError,
                    "Unrecognized streaming header: \"%c\"\n",
                    copybuf[0]);
            goto error;
        }

        if (!reader_consume_stream(self, copybuf, r))
            goto error;
    }

    res = PQgetResult(self->conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        PyErr_Format(PyExc_ValueError,
                "unexpected termination of replication stream: %s",
                PQresultErrorMessage(res));
        goto error;
    }
    PQclear(res);
    destroyPQExpBuffer(query);
    PQfinish(self->conn);
    self->conn = NULL;
    return 1;

error:
    if (copybuf != NULL)
    {
        PQfreemem(copybuf);
        copybuf = NULL;
    }
    if (res)
        PQclear(res);
    destroyPQExpBuffer(query);
    PQfinish(self->conn);
    self->conn = NULL;
    return 0;
}

int
reader_stop(readerObject *self)
{
    self->abort = true;
    return 1;
}

/* PYTHON STUFF */

static int
reader_init(PyObject *obj, PyObject *args, PyObject *kwargs)
{
    readerObject *self = (readerObject *)obj;
    static char *kwlist[] = {
        "host", "port", "username", "dbname", "password",
        "progname", "plugin", "slot", "create_slot", "feedback_interval", NULL};

    self->host=self->port=self->username=self->password = NULL;
    self->dbname = "postgres";
    self->progname = "pylogicaldecoding";
    self->plugin = "test_decoding";
    self->slot = "test_slot";
    self->create_slot = 1;
    self->startpos = InvalidXLogRecPtr;
    self->standby_message_timeout = 10 * 1000;

    // TODO: not sure where we should register the signal
    // maybe we should expose a function here called at module init.
    signal(SIGINT, sigint_handler);

    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "|ssssssssbi", kwlist,
            &(self->host), &(self->port), &(self->username),
            &(self->dbname), &(self->password), &(self->progname),
            &(self->plugin), &(self->slot), &(self->create_slot),
            &(self->standby_message_timeout)))
        return -1;

    self->decoded_lsn = InvalidXLogRecPtr;
    self->last_status = -1;
    // test a connection
    return reader_connect(self, true);
}

static PyObject *
reader_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc(type, 0);
}

static PyObject *
reader_repr(readerObject *self)
{
    return PyString_FromFormat(
        "<Reader object at %p, slot=`%s`>", self, self->slot);
}

static void
reader_dealloc(PyObject* obj)
{
    readerObject *self = (readerObject *)obj;

    if (self->conn)
        PQfinish(self->conn);

    // TODO: these come from PyParseArgs. Not sure if free is
    // perfectly safe
    {
        free(self->host);
        free(self->port);
        free(self->username);
        free(self->dbname);
        free(self->password);
        free(self->progname);
        free(self->plugin);
        free(self->slot);
    }

    Py_TYPE(obj)->tp_free(obj);
}

static PyObject *
py_reader_stream(readerObject *self)
{
    if(!reader_stream(self))
        return NULL;
    Py_RETURN_NONE;
}

static PyObject *
py_reader_stop(readerObject *self)
{
    if(!reader_stop(self))
        return NULL;
    Py_RETURN_NONE;
}

static PyObject *
py_reader_commit(readerObject *self)
{
    if(!reader_commit(self))
        return NULL;
    Py_RETURN_NONE;
}

static PyObject *
py_reader_drop_slot(readerObject *self)
{
    if(!reader_drop_slot(self))
        return NULL;
    Py_RETURN_NONE;
}

static struct PyMethodDef reader_methods[] = {
    {"stream", (PyCFunction)py_reader_stream, METH_NOARGS,
    reader_stream_doc},
    {"stop", (PyCFunction)py_reader_stop, METH_NOARGS,
    reader_stop_doc},
    {"commit", (PyCFunction)py_reader_commit, METH_NOARGS,
    reader_commit_doc},
    {"drop_slot", (PyCFunction)py_reader_drop_slot, METH_NOARGS,
    reader_drop_slot_doc},
    {NULL}
};

static struct PyMemberDef reader_members[] = {
    /*{"conn_params", T_OBJECT, offsetof(readerObject, conn_params), READONLY},*/
    {NULL}
};

#define readerType_doc \
"Reader(dsn) -> new reader object\n\n"

PyTypeObject readerType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "logicaldecoding.Reader",
    sizeof(readerObject), 0,
    reader_dealloc, /*tp_dealloc*/
    0,          /*tp_print*/
    0,          /*tp_getattr*/
    0,          /*tp_setattr*/
    0,          /*tp_compare*/
    (reprfunc)reader_repr, /*tp_repr*/
    0,          /*tp_as_number*/
    0,          /*tp_as_sequence*/
    0,          /*tp_as_mapping*/
    0,          /*tp_hash */
    0,          /*tp_call*/
    (reprfunc)reader_repr, /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
                /*tp_flags*/
    readerType_doc, /*tp_doc*/

    //(traverseproc)connection_traverse, /*tp_traverse*/
    0,

    //(inquiry)connection_clear, /*tp_clear*/
    0,

    0,          /*tp_richcompare*/

    //offsetof(connectionObject, weakreflist), /* tp_weaklistoffset */
    0,

    0,          /*tp_iter*/
    0,          /*tp_iternext*/
    reader_methods, /*tp_methods*/
    reader_members, /*tp_members*/

    //connectionObject_getsets, /*tp_getset*/
    0,

    0,          /*tp_base*/
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    reader_init, /*tp_init*/

    0,          /*tp_alloc*/
    reader_new, /*tp_new*/
};
