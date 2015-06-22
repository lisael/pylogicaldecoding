#ifndef PGHX_ERRORS_H
#define PGHX_ERRORS_H 1
typedef enum pghx_error_type{
    PGHX_NO_ERROR,

    // system errors
    PGHX_IO_ERROR,
    PGHX_OUT_OF_MEMORY,

    // generic DB errors
    PGHX_CONNECTION_ERROR,
    PGHX_PASSWORD_ERROR,
    PGHX_COMMAND_ERROR,
    PGHX_QUERY_ERROR,

    // logical decoding specific errors
    PGHX_LD_STREAM_PROTOCOL_ERROR,
    PGHX_LD_REPLICATION_ERROR,
    PGHX_LD_NO_SLOT,
    PGHX_LD_BAD_PLUGIN,
    PGHX_LD_STATUS_ERROR,

    // you'll need this to create a mapping with your errors/ecxeptions
    // (I love that trick, though I can't remember where I read that first)
    PGHX_ERRORS_NUM,
} pghx_error_type;

extern pghx_error_type pghx_error;
extern char pghx_error_info[1024];

#define Pghx_set_error(tp, info) do{\
    pghx_error = (tp);\
    strcpy(pghx_error_info, info);\
}while(0)

#define Pghx_format_error(tp, info,...) do{\
    pghx_error = (tp);\
    snprintf(pghx_error_info, 1024, (info), __VA_ARGS__);\
}while(0)
#endif


