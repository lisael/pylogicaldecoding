TESTS = check_pghx
check_PROGRAMS = check_pghx
check_pghx_SOURCES = check_pghx.c $(top_builddir)/src/pghx/logicaldecoding.h
check_pghx_CFLAGS = @CHECK_CFLAGS@ -lpq
check_pghx_LDADD = $(top_builddir)/src/pghx/libpghx.la @CHECK_LIBS@
AM_CPPFLAGS = -I/usr/include/postgresql -I/usr/include

