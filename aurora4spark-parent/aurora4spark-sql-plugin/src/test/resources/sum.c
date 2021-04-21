#include <stdio.h>

double sum(double *a, int n)
{
    int i;
    double sum = 0;
    for (i = 0; i < n; i++) {
        sum += a[i];
    }

    return sum;
}


