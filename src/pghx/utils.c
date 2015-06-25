#include "utils.h"


void *
pg_malloc(size_t size)
{
    void	   *tmp;

    /* Avoid unportable behavior of malloc(0) */
    if (size == 0)
        size = 1;
    tmp = malloc(size);
    if (!tmp)
    {
        fprintf(stderr, "out of memory\n");
        exit(EXIT_FAILURE);
    }
    return tmp;
}

void *
pg_malloc0(size_t size)
{
    void	   *tmp;

    tmp = pg_malloc(size);
    memset(tmp, 0, size);
    return tmp;
}

int64_t
feGetCurrentTimestamp(void)
{
    int64_t result;
    struct timeval tp;

    gettimeofday(&tp, NULL);

    result = (int64_t) tp.tv_sec -
        ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);

    result = (result * USECS_PER_SEC) + tp.tv_usec;

    return result;
}

void
feTimestampDifference(int64_t start_time, int64_t stop_time,
        long *secs, int *microsecs)
{
    int64_t diff = stop_time - start_time;

    if (diff <= 0)
    {
        *secs = 0;
        *microsecs = 0;
    }
    else
    {
        *secs = (long) (diff / USECS_PER_SEC);
        *microsecs = (int) (diff % USECS_PER_SEC);
    }
}

bool
feTimestampDifferenceExceeds(int64_t start_time,
        int64_t stop_time,
        int msec)
{
    int64_t diff = stop_time - start_time;

    return (diff >= msec * INT64CONST(1000));
}

/*
 * Converts an int64 from network byte order to native format.
 */
int64_t
fe_recvint64(char *buf)
{
    int64_t     result;
    uint32_t    h32;
    uint32_t    l32;

    memcpy(&h32, buf, 4);
    memcpy(&l32, buf + 4, 4);
    h32 = ntohl(h32);
    l32 = ntohl(l32);

    result = h32;
    result <<= 32;
    result |= l32;

    return result;
}

/*
 * Converts an int64 to network byte order.
 */
void
fe_sendint64(int64_t i, char *buf)
{
    uint32_t n32;

    /* High order half first, since we're doing MSB-first */
    n32 = (uint32_t) (i >> 32);
    n32 = htonl(n32);
    memcpy(&buf[0], &n32, 4);

    /* Now the low order half */
    n32 = (uint32_t) i;
    n32 = htonl(n32);
    memcpy(&buf[4], &n32, 4);
}

void
pg_usleep(long microsec)
{
	if (microsec > 0)
	{
#ifndef WIN32
		struct timeval delay;

		delay.tv_sec = microsec / 1000000L;
		delay.tv_usec = microsec % 1000000L;
		(void) select(0, NULL, NULL, NULL, &delay);
#else
		SleepEx((microsec < 500 ? 1 : (microsec + 500) / 1000), FALSE);
#endif
	}
}
