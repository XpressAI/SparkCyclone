#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <limits>
#include <iostream>
#ifndef VE_TD_DEFS
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
    int count;
} non_null_int_vector;

typedef struct
{
    double *data;
    long count;
} non_null_double_vector;

typedef struct
{
    double *data;
    long count;
    unsigned char* validityBuffer;

} nullable_double_vector;

typedef struct
{
    long *data;
    int count;
} non_null_bigint_vector;

typedef struct
{
    char *data;
    int *offsets;
    int size;
    int count;
} non_null_varchar_vector;

typedef struct
{
    char *data;
    int length;
} non_null_c_bounded_string;

#define VE_TD_DEFS 1
#endif

