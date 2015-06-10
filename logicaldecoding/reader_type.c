#include <signal.h>
#include "logicaldecoding.h"
#include "connection.h"
#include "streamutils.h"

#define reader_start_doc \
"start() -> start the main loop"

#define reader_stop_doc \
"stop() -> stop the main loop"

#define reader_commit_doc \
"commit() -> send feedback message acking all preceding stream\n"\
"It's user's responsability to send regular acknowledgemnts. If"\
"ommited, the master keeps all it's WAL on disk and eventually"\
"Cthulhu eats the physical server (or something like that, I"\
"just can't find this damn server, now)"


static volatile sig_atomic_t global_abort = false;

typedef struct readerObject{
    PyObject_HEAD
    char *host;
    char *port;
    char *username;
    char *dbname;
    char *password;
    char *progname;
    char *decoder;
    char *slot;
    PGconn *conn;
    bool abort;
    XLogRecPtr decoded_lsn; // log level successfully sent to user's callback
    XLogRecPtr commited_lsn; // acked log level
} readerObject;

static bool
reader_sendFeedback(readerObject *self, int64_t now, bool force, bool replyRequested);

/*
 * tell the main loop to exit at the next possible moment.
 */
static void
sigint_handler(int signum)
{
    global_abort = true;
}

char *
py_map_get_string_or_null(PyObject *o, char *key)
{
    PyObject *val, *ascii_string;
    char *str, *result;
    val = PyMapping_GetItemString(o, key);
    if (val == NULL || val == Py_None)
        return NULL;
    ascii_string = PyUnicode_AsASCIIString(val);
    if (!ascii_string) return NULL;
    str = Bytes_AsString(ascii_string);
    result = malloc(strlen(str)+1 * sizeof(char));
    strcpy(result, str);
    Py_DECREF(ascii_string);
    Py_DECREF(val);
    return result;
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
        free(self->decoder);
        free(self->slot);
    }

    Py_TYPE(obj)->tp_free(obj);
}

static int
reader_connect(readerObject *self)
{
    int			argcount = 7;	/* dbname, replication, fallback_app_name,
                                 * host, user, port, password */
    int			i;
    const char **keywords;
    const char **values;
    const char *tmpparam;

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
    values[i] = self->dbname == NULL ? "true" : "database";
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

    self->conn = PQconnectdbParams(keywords, values, true);

    if (!self->conn)
    {
        PyErr_NoMemory();
        goto error;
    }

    if (PQstatus(self->conn) == CONNECTION_BAD)
    {
        if (PQconnectionNeedsPassword(self->conn))
        {
            PyErr_SetString(PyExc_ValueError, "password needed");
            goto error;
        }
    }

    if (PQstatus(self->conn) != CONNECTION_OK)
    {
        PyErr_Format(PyExc_IOError,
            "Could not connect to server: %s\n",
             PQerrorMessage(self->conn));
        goto error;
    }

    /* Connection ok! */
    free(values);
    free(keywords);

    /*
     * Ensure we have the same value of integer timestamps as the server we
     * are connecting to.
     */
    tmpparam = PQparameterStatus(self->conn, "integer_datetimes");
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

    return 1;
error:
    free(values);
    free(keywords);
    if (self->conn)
        PQfinish(self->conn);
    return 0;
}

static int
reader_init(PyObject *obj, PyObject *args, PyObject *kwargs)
{
    readerObject *self = (readerObject *)obj;
    static char *kwlist[] = {
        "host", "port", "username", "dbname", "password",
        "progname", "decoder", "slot", NULL};

    self->host=self->port=self->username=self->password = NULL;
    self->dbname = "postgres";
    self->progname = "pylogicaldecoding";
    self->decoder = "test_decoding";
    self->slot = "test_slot";

    // TODO: not sure where we should register the signal
    // maybe we should expose a function here called at module init.
    signal(SIGINT, sigint_handler);

    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "|ssssssss", kwlist,
            &(self->host), &(self->port), &(self->username),
            &(self->dbname), &(self->password), &(self->progname),
            &(self->decoder), &(self->slot))){
        return -1;
    }

    self->progname = "pylogicaldecoding";
    self->decoded_lsn = InvalidXLogRecPtr;
    // test a connection
    return reader_connect(self);
}

static PyObject *
reader_stop(readerObject *self)
{
    self->abort = true;
    Py_RETURN_NONE;
}

static PyObject *
reader_start(readerObject *self)
{
    PGresult   *res;
    char	   *copybuf = NULL;
    int64_t		last_status = -1;
    int			i;
    PQExpBuffer query;
    PyObject *pFunc = NULL,
             *pArgs = NULL,
             *pValue = NULL,
             *result = NULL;

    char **options=NULL;
    int standby_message_timeout = 10 * 1000;

    bool verbose=false;
    int noptions = 0;
    XLogRecPtr startpos = InvalidXLogRecPtr;
    XLogRecPtr old_lsn = 0;

    self->abort = false;

    query = createPQExpBuffer();

    /*
     * Connect in replication mode to the server
     */
    {
        if (!reader_connect(self))
            // exception is setted in reader_connect...
            return NULL;
    }

    /*
     * Start the replication
     */
    if (verbose)
        fprintf(stderr,
                "%s: starting log streaming at %X/%X (slot %s)\n",
                self->progname, (uint32_t) (startpos >> 32), (uint32_t) startpos,
                self->slot);

    /* Initiate the replication stream at specified location */
    appendPQExpBuffer(query, "START_REPLICATION SLOT \"%s\" LOGICAL %X/%X",
            self->slot, (uint32_t) (startpos >> 32), (uint32_t) startpos);

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

    res = PQexec(self->conn, query->data);
    if (PQresultStatus(res) != PGRES_COPY_BOTH)
    {
        PyErr_Format(PyExc_ValueError,
                "Could not send replication command \"%s\": %s",
                query->data, PQresultErrorMessage(res));
        PQclear(res);
        goto error;
    }
    PQclear(res);
    resetPQExpBuffer(query);

    if (verbose)
        fprintf(stderr,
                "%s: streaming initiated\n",
                self->progname);

    while (!global_abort && !self->abort)
    {
        int			r;
        int64_t		now;
        int			hdr_len;
        static bool first_loop = true;

        if (copybuf != NULL)
        {
            PQfreemem(copybuf);
            copybuf = NULL;
        }

        /*
         * Potentially send a status message to the master
         */
        now = feGetCurrentTimestamp();

        if (first_loop || (standby_message_timeout > 0 &&
                feTimestampDifferenceExceeds(last_status, now,
                    standby_message_timeout)))
        {
            /* Time to send feedback! */
            if (!reader_sendFeedback(self, now, true, false))
            {
                PyErr_Format(PyExc_IOError,
                        "Could not send feedback packet: %s",
                        PQerrorMessage(self->conn));
                goto error;
            }

            last_status = now;
        }
        first_loop=false;

        r = PQgetCopyData(self->conn, &copybuf, 1);
        //fprintf(stderr, "data len : %i\n", r); fflush(stderr);
        if (r == 0)
        {
            /*
             * In async mode, and no data available. We block on reading but
             * not more than the specified timeout, so that we can send a
             * response back to the client.
             */
            fd_set		input_mask;
            int64_t		message_target = 0;
            struct timeval timeout;
            struct timeval *timeoutptr = NULL;

            FD_ZERO(&input_mask);
            FD_SET(PQsocket(self->conn), &input_mask);

            /* Compute when we need to wakeup to send a keepalive message. */
            if (standby_message_timeout)
                message_target = last_status + (standby_message_timeout - 1) *
                    ((int64_t) 1000);

            /* Now compute when to wakeup. */
            if (message_target > 0 )
            {
                int64_t		targettime;
                long		secs;
                int			usecs;

                targettime = message_target;

                feTimestampDifference(now,
                        targettime,
                        &secs,
                        &usecs);
                if (secs <= 0)
                    timeout.tv_sec = 1; /* Always sleep at least 1 sec */
                else
                    timeout.tv_sec = secs;
                timeout.tv_usec = usecs;
                timeoutptr = &timeout;
            }

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
        //fprintf(stderr, "header: %s\n", copybuf + 25); fflush(stderr);

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
            int			pos;
            bool		replyRequested;
            XLogRecPtr	walEnd;

            /*
             * Parse the keepalive message, enclosed in the CopyData message.
             * We just check if the server requested a reply, and ignore the
             * rest.
             */
            pos = 1;			/* skip msgtype 'k' */
            walEnd = fe_recvint64(&copybuf[pos]);
            self->decoded_lsn = Max(walEnd, self->decoded_lsn);

            pos += 8;			/* read walEnd */

            pos += 8;			/* skip sendTime */

            if (r < pos + 1)
            {
                PyErr_Format(PyExc_ValueError,
                        "Streaming header too small: %d\n",
                        r);
                goto error;
            }
            replyRequested = copybuf[pos];

            /* If the server requested an immediate reply, send one. */
            // TODO: no exception set!
            if (replyRequested)
            {
                now = feGetCurrentTimestamp();
                if (!reader_sendFeedback(self, now, true, false))
                {
                    PyErr_Format(PyExc_IOError,
                            "Could not send feedback packet: %s",
                            PQerrorMessage(self->conn));
                    goto error;
                }
                last_status = now;
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


        /*
         * Read the header of the XLogData message, enclosed in the CopyData
         * message. We only need the WAL location field (dataStart), the rest
         * of the header is ignored.
         */
        hdr_len = 1;			/* msgtype 'w' */
        hdr_len += 8;			/* dataStart */
        hdr_len += 8;			/* walEnd */
        hdr_len += 8;			/* sendTime */
        if (r < hdr_len + 1)
        {
            PyErr_Format(PyExc_ValueError,
                    "Streaming header too small: %d\n",
                    r);
            goto error;
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
    Py_RETURN_NONE;

error:
    if (copybuf != NULL)
    {
        PQfreemem(copybuf);
        copybuf = NULL;
    }
    if(old_lsn){
        self->decoded_lsn = old_lsn;
    }
    destroyPQExpBuffer(query);
    PQfinish(self->conn);
    self->conn = NULL;
    Py_XDECREF(pFunc);
    Py_XDECREF(pArgs);
    Py_XDECREF(pValue);
    Py_XDECREF(result);
    return NULL;
}

static PyObject *
reader_commit(readerObject *self)
{
    bool success;
    int64_t now = feGetCurrentTimestamp();
    XLogRecPtr old_lsn = self->commited_lsn;

    self->commited_lsn = self->decoded_lsn;
    success = reader_sendFeedback(self, now, true, false);
    if (!success)
    {
        PyErr_Format(PyExc_IOError,
                "Could not send feedback packet: %s",
                PQerrorMessage(self->conn));
        self->commited_lsn = old_lsn;
        return NULL;
    }
    Py_RETURN_NONE;
}

static struct PyMethodDef reader_methods[] = {
    {"start", (PyCFunction)reader_start, METH_NOARGS,
    reader_start_doc},
    {"stop", (PyCFunction)reader_stop, METH_NOARGS,
    reader_stop_doc},
    {"commit", (PyCFunction)reader_commit, METH_NOARGS,
    reader_commit_doc},
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

static bool
reader_sendFeedback(readerObject *self, int64_t now, bool force, bool replyRequested)
{
    char		replybuf[1 + 8 + 8 + 8 + 8 + 1];
    int			len = 0;

    if (!force &&
            self->decoded_lsn == self->commited_lsn)
        return true;
    /*printf("feedback... %X/%X\n",*/
                /*(uint32_t) (self->commited_lsn >> 32), (uint32_t) self->commited_lsn);*/
    /*fflush(stdout);*/

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
    if (PQputCopyData(self->conn, replybuf, len) <= 0
            || PQflush(self->conn))
        return false;
    return true;
}



