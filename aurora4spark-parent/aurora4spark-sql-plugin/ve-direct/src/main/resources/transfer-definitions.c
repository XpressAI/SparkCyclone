#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct
{
    void **data;
    size_t count;
    size_t size;
} data_out;

typedef struct
{
    char *data;
    int *offsets;
    long count;
} varchar_vector;

typedef struct
{
    char *data;
} word_count;

typedef struct
{
    int *data;
    int count;
} non_null_int_vector;