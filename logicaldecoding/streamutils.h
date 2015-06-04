#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <sys/time.h>
#include <string.h>

/* for ntohl/htonl */
#include <netinet/in.h>
#include <arpa/inet.h>

#include <pg_config_manual.h>
#include <internal/pqexpbuffer.h>

#define INT64CONST(x)  ((int64_t) x##LL)
#define POSTGRES_EPOCH_JDATE    2451545
#define UNIX_EPOCH_JDATE        2440588
#define SECS_PER_DAY    86400
#define USECS_PER_DAY   INT64CONST(86400000000)
#define USECS_PER_SEC   INT64CONST(1000000)


typedef uint64_t XLogRecPtr;
#define Max(x, y)       ((x) > (y) ? (x) : (y))

/*
 * Zero is used indicate an invalid pointer. Bootstrap skips the first possible
 * WAL segment, initializing the first WAL page at XLOG_SEG_SIZE, so no XLOG
 * record can begin at zero.
 */
#define InvalidXLogRecPtr  0


void * pg_malloc0(size_t size);
int64_t feGetCurrentTimestamp(void);
void feTimestampDifference(int64_t start_time, int64_t stop_time, long *secs, int *microsecs);
bool feTimestampDifferenceExceeds(int64_t start_time, int64_t stop_time, int msec);
int64_t fe_recvint64(char *buf);
void fe_sendint64(int64_t i, char *buf);

