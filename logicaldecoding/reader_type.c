#include <signal.h>
#include <math.h>
#include "pylogicaldecoding.h"
#include <pghx/logicaldecoding.h>
#include <pghx/errors.h>

#define reader_stream_doc \
"stream() -> start the main loop"

#define reader_stop_doc \
"stop() -> stop the main loop"

#define reader_acknowledge_doc \
"ack() -> send feedback message acknowledging all preceding stream\n"\
"It's user's responsibility to send regular acknowledgements. If"\
"omited, the master keeps all its WAL on disk and eventually"\
"Cthulhu eats the physical server"

#define reader_drop_slot_doc \
"drop_slot() -> drop the replication slot"

// 10s
#define MAX_RETRY_INTERVAL 10000000

extern volatile sig_atomic_t global_abort;

typedef struct PyLDReader{
    PyObject_HEAD
    pghx_ld_reader reader;
} PyLDReader;

int
reader_stream_cb(void *self, char *data)
{
    PyObject    *pFunc = NULL,
                *pArgs = NULL,
                *pValue = NULL,
                *result = NULL;

    pFunc = PyObject_GetAttrString((PyObject *)self, "event");
    if (pFunc == NULL){goto error;}
    pArgs = PyTuple_New(1);
    if (pArgs == NULL){goto error;}
    pValue = Text_FromUTF8(data);
    Py_INCREF(pValue);
    if (pValue == NULL){goto error;}
    PyTuple_SetItem(pArgs, 0, pValue);
    result = PyObject_CallObject(pFunc, pArgs);
    if (result == NULL){goto error;}
    Py_DECREF(pFunc);
    Py_DECREF(pArgs);
    Py_DECREF(pValue);
    Py_DECREF(result);
    return 1;

error:
    Py_XDECREF(pFunc);
    Py_XDECREF(pArgs);
    Py_XDECREF(pValue);
    Py_XDECREF(result);
    return 0;
}

static void
py_set_pghx_error(void){
    PyObject *exception;
    static PyObject *error_map[PGHX_ERRORS_NUM] = { NULL };
    // early return in case of OOM (it soon gonna crash anyway,
    // let's try to write the cause)
    if (pghx_error == PGHX_OUT_OF_MEMORY)
    {
        fprintf(stderr, "%s\n", pghx_error_info);
        fflush(stderr);
        PyErr_NoMemory();
        return;
    }
    // this is bad, maybe we should raise something ugly or exit.
    if (pghx_error == PGHX_NO_ERROR)
    {
        fputs("py_set_pghx_error() called without error\n", stderr);
        fflush(stderr);
        return;
    }

    // initialize the error map
    // TODO: use or create sensible exceptions
    if (!error_map[1])
    {
        error_map[PGHX_IO_ERROR] = PyExc_ValueError;
        error_map[PGHX_OUT_OF_MEMORY] = PyExc_ValueError;

        // generic DB errors
        error_map[PGHX_CONNECTION_ERROR] = PyExc_ValueError;
        error_map[PGHX_PASSWORD_ERROR] = PyExc_ValueError;
        error_map[PGHX_COMMAND_ERROR] = PyExc_ValueError;
        error_map[PGHX_QUERY_ERROR] = PyExc_ValueError;

        // logical decoding specific errors
        error_map[PGHX_LD_STREAM_PROTOCOL_ERROR] = PyExc_ValueError;
        error_map[PGHX_LD_REPLICATION_ERROR] = PyExc_ValueError;
        error_map[PGHX_LD_NO_SLOT] = PyExc_ValueError;
        error_map[PGHX_LD_BAD_PLUGIN] = PyExc_ValueError;
        error_map[PGHX_LD_STATUS_ERROR] = PyExc_ValueError;
        error_map[PGHX_LD_PARSE_ERROR] = PyExc_ValueError;
    }
    exception = error_map[pghx_error];
    // probably something new in pghx, we don't handle yet
    if (!exception){
        PyErr_Format(PyExc_Exception, "Unknown error: %s", pghx_error_info);
    }
    else
    {
        PyErr_SetString(error_map[(int)pghx_error], pghx_error_info);
    }
}

static int
reader_init(PyObject *obj, PyObject *args, PyObject *kwargs)
{
    PyLDReader *self = (PyLDReader *)obj;
    pghx_ld_reader *r = &(self->reader);

    static char *kwlist[] = {
        "host", "port", "username", "dbname", "password",
        "progname", "plugin", "slot", "create_slot", "feedback_interval",
        "connection_timeout", NULL};

    pghx_ld_reader_init(r);

    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "|ssssssssbil", kwlist,
            &(r->host), &(r->port), &(r->username),
            &(r->dbname), &(r->password), &(r->progname),
            &(r->plugin), &(r->slot), &(r->create_slot),
            &(r->standby_message_timeout),&(r->connection_timeout)))
        return -1;

    r->stream_cb = reader_stream_cb;
    r->user_data = (void *)self;
    // test a connection
    return pghx_ld_reader_connect(r, true);
}

static PyObject *
reader_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc(type, 0);
}

static PyObject *
reader_repr(PyLDReader *self)
{
    pghx_ld_reader *r = &(self->reader);

    return PyString_FromFormat(
        "<Reader object at %p, slot=`%s`>", self, r->slot);
}

static void
reader_dealloc(PyObject* obj)
{
    PyLDReader *self = (PyLDReader *)obj;
    pghx_ld_reader *r = &(self->reader);

    if (r->conn)
        PQfinish(r->conn);

    // TODO: these come from PyParseArgs. Not sure if free is
    // perfectly safe
    {
        free(r->host);
        free(r->port);
        free(r->username);
        free(r->dbname);
        free(r->password);
        free(r->progname);
        free(r->plugin);
        free(r->slot);
    }

    Py_TYPE(obj)->tp_free(obj);
}

static PyObject *
py_reader_stream(PyLDReader *self)
{
    if(!pghx_ld_reader_stream(&(self->reader)))
    {
        py_set_pghx_error();
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
py_reader_stop(PyLDReader *self)
{
    if(!pghx_ld_reader_stop(&(self->reader)))
    {
        py_set_pghx_error();
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
py_reader_acknowledge(PyLDReader *self)
{
    if(!pghx_ld_reader_acknowledge(&(self->reader)))
    {
        py_set_pghx_error();
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
py_reader_drop_slot(PyLDReader *self)
{
    if(!pghx_ld_reader_drop_slot(&(self->reader)))
    {
        py_set_pghx_error();
        return NULL;
    }
    Py_RETURN_NONE;
}

static struct PyMethodDef reader_methods[] = {
    {"stream", (PyCFunction)py_reader_stream, METH_NOARGS,
    reader_stream_doc},
    {"stop", (PyCFunction)py_reader_stop, METH_NOARGS,
    reader_stop_doc},
    {"ack", (PyCFunction)py_reader_acknowledge, METH_NOARGS,
    reader_acknowledge_doc},
    {"drop_slot", (PyCFunction)py_reader_drop_slot, METH_NOARGS,
    reader_drop_slot_doc},
    {NULL}
};

static struct PyMemberDef reader_members[] = {
    /*{"conn_params", T_OBJECT, offsetof(PyLDReader, conn_params), READONLY},*/
    {NULL}
};

#define readerType_doc \
"Reader(dsn) -> new reader object\n\n"

PyTypeObject readerType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "logicaldecoding.Reader",
    sizeof(PyLDReader), 0,
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
