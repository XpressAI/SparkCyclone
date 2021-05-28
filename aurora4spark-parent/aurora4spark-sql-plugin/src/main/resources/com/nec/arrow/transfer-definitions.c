#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <omp.h>

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
    int count;
} varchar_vector;

typedef struct
{
    int *data;
    long count;
} non_null_int_vector;

typedef struct
{
    double *data;
    long count;
} non_null_double_vector;