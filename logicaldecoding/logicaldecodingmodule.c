
#define PSYCOPG_MODULE
#include "logicaldecoding/pylogicaldecoding.h"

extern HIDDEN PyTypeObject readerType;

static PyMethodDef logicaldecodingMethods[] = {
    {NULL,NULL,0, NULL}
};

#if PY_MAJOR_VERSION > 2
static struct PyModuleDef logicaldecodingmodule = {
        PyModuleDef_HEAD_INIT,
        "_psycopg",
        NULL,
        -1,
        logicaldecodingMethods,
        NULL,
        NULL,
        NULL,
        NULL
};
#endif

PyMODINIT_FUNC
INIT_MODULE(_logicaldecoding)(void)
{
    PyObject *module = NULL;

    /* initialize the module */
#if PY_MAJOR_VERSION < 3
    module = Py_InitModule("_logicaldecoding", logicaldecodingMethods);
#else
    module = PyModule_Create(&logicaldecodingmodule);
#endif
    if (!module) { goto exit; }

    Py_TYPE(&readerType) = &PyType_Type;
    if (PyType_Ready(&readerType) == -1) goto exit;

    PyModule_AddObject(module, "Reader", (PyObject*)&readerType);
exit:
#if PY_MAJOR_VERSION > 2
    return module;
#else
    return;
#endif
}
