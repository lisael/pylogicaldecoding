AC_INIT([pghx], [0.1], [lisael@lisael.org])
AC_CONFIG_AUX_DIR([build-aux])
AC_CONFIG_MACRO_DIR([m4])
AM_INIT_AUTOMAKE([foreign -Wall -Werror])
AM_PROG_AR
AM_PATH_CHECK
LT_INIT
AC_PROG_CC
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_FILES([Makefile src/Makefile src/pghx/Makefile tests/Makefile])
AC_CONFIG_SUBDIRS([pghx tests])
AC_SEARCH_LIBS([floor], [m])
AC_SEARCH_LIBS([timer_create], [rt])
AC_SEARCH_LIBS([pthread_create], [pthread])
AC_OUTPUT
