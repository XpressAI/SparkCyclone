#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <limits>
#include <iostream>
#include <vector>

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
    int *data;
    unsigned char *validityBuffer;
    int count;
} nullable_int_vector;

typedef struct
{
    double *data;
    long count;
} non_null_double_vector;

typedef struct
{
    double *data;
    unsigned char *validityBuffer;
    long count;

} nullable_double_vector;

typedef struct
{
    long long *data;
    unsigned char *validityBuffer;
    long count;

} nullable_bigint_vector;


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
    int *offsets;
    unsigned char *validityBuffer;
    int size;
    int count;
} nullable_varchar_vector;

typedef struct
{
    char *data;
    int length;
} non_null_c_bounded_string;

void set_validity(unsigned char *validityBuffer, int idx, int validity) {
    int byte = idx / 8;
    int bitIndex = idx % 8;
    if ( validity ) {
        validityBuffer[byte] |= (1UL << bitIndex);
    } else {
        validityBuffer[byte] &= ~(1UL << bitIndex);
    }
};

int check_valid(unsigned char *validityBuffer, int idx) {
    int byte = idx / 8;
    int bitIndex = idx % 8;
    return (validityBuffer[byte] >> bitIndex) & 1;
};

#define VE_TD_DEFS 1
#endif

