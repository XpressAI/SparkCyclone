#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
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
    int32_t *offsets;
    int32_t count;
} varchar_vector;

typedef struct
{
    int32_t *data;
    int32_t count;
} non_null_int_vector;

typedef struct
{
    int32_t *data;
    unsigned char *validityBuffer;
    int32_t count;
} nullable_int_vector;

typedef struct
{
    double *data;
    int64_t count;
} non_null_double_vector;

typedef struct
{
    double *data;
    unsigned char *validityBuffer;
    int64_t count;

} nullable_double_vector;

typedef struct
{
    int64_t *data;
    unsigned char *validityBuffer;
    int64_t count;

} nullable_bigint_vector;


typedef struct
{
    int64_t *data;
    int32_t count;
} non_null_bigint_vector;

typedef struct
{
    char *data;
    int32_t *offsets;
    int32_t size;
    int32_t count;
} non_null_varchar_vector;

typedef struct
{
    char *data;
    int32_t length;
} non_null_c_bounded_string;

void set_validity(unsigned char *validityBuffer, int32_t idx, int32_t validity) {
    int32_t byte = idx / 8;
    int32_t bitIndex = idx % 8;
    if ( validity ) {
        validityBuffer[byte] |= (1UL << bitIndex);
    } else {
        validityBuffer[byte] &= ~(1UL << bitIndex);
    }
};

int32_t check_valid(unsigned char *validityBuffer, int32_t idx) {
    int32_t byte = idx / 8;
    int32_t bitIndex = idx % 8;
    return (validityBuffer[byte] >> bitIndex) & 1;
};

#define VE_TD_DEFS 1
#endif

