#ifndef PYLD_H
#define PYLD_H 1
#include <Python.h>
#include <libpq-fe.h>

#if __GNUC__ >= 4 && !defined(__MINGW32__)
#  define HIDDEN __attribute__((visibility("hidden")))
#else
#  define HIDDEN
#endif

#include "logicaldecoding/python.h"
#endif
