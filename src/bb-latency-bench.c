/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>

#include "abt.h"
#include "abt-snoozer.h"
#include "bake-bulk.h"

static void bench_routine(bake_target_id_t bti, int iterations, int size);

int main(int argc, char **argv) 
{
    int ret;
    bake_target_id_t bti;
    int min_size, max_size, iterations, cur_size;
 
    if(argc != 5)
    {
        fprintf(stderr, "Usage: bb-latency-bench <server addr> <iterations> <min_sz> <max_sz>\n");
        fprintf(stderr, "  Example: ./bb-latency-bench tcp://localhost:1234 1000 4 32\n");
        return(-1);
    }       

    ret = sscanf(argv[2], "%d", &iterations);
    assert(ret == 1);

    ret = sscanf(argv[3], "%d", &min_size);
    assert(ret == 1);

    ret = sscanf(argv[4], "%d", &max_size);
    assert(ret == 1);

    /* set up Argobots */
    ret = ABT_init(argc, argv);
    if(ret != 0)
    {
        fprintf(stderr, "Error: ABT_init()\n");
        return(-1);
    }
    ret = ABT_snoozer_xstream_self_set();
    if(ret != 0)
    {
        ABT_finalize();
        fprintf(stderr, "Error: ABT_snoozer_xstream_self_set()\n");
        return(-1);
    }

    ret = bake_probe_instance(argv[1], &bti);
    if(ret < 0)
    {
        ABT_finalize();
        fprintf(stderr, "Error: bake_probe_instance()\n");
        return(-1);
    }

    printf("# <op> <iterations> <size> <avg> <min> <max>\n");
    for(cur_size=min_size; cur_size <= max_size; cur_size *= 2)
    {
        bench_routine(bti, iterations, cur_size);
    }
    
    bake_release_instance(bti);

    ABT_finalize();

    return(0);
}

static double Wtime(void)
{
    struct timeval t;
    gettimeofday(&t, NULL);
    return((double)t.tv_sec + (double)(t.tv_usec) / 1000000);
}

static void bench_routine(bake_target_id_t bti, int iterations, int size)
{
    bake_bulk_region_id_t rid;
    int ret;
    double tm1, tm2, min = 0, max = 0, sum = 0;
    char *buffer;
    uint64_t region_offset = 0;
    int i;

    buffer = calloc(1, size);
    assert(buffer);

    /* create region */
    ret = bake_bulk_create(bti, size*iterations, &rid);
    assert(ret == 0);

    sleep(1);

    for(i=0; i<iterations; i++)
    {
        tm1 = Wtime();
        /* transfer data (writes) */
        ret = bake_bulk_write(
            bti,
            rid,
            region_offset,
            buffer,
            size);
        tm2 = Wtime();
        assert(ret == 0);
        region_offset += size;

        sum += tm2-tm1;
        if(min == 0 || min > (tm2-tm1))
            min = tm2-tm1;
        if(max == 0 || max < (tm2-tm1))
            max = tm2-tm1;
    }

    printf("write\t%d\t%d\t%.9f\t%.9f\t%.9f\n", iterations, size, sum/((double)iterations), min, max);

    region_offset = 0;
    sleep(1);

    for(i=0; i<iterations; i++)
    {
        tm1 = Wtime();
        /* transfer data (reads) */
        ret = bake_bulk_read(
            bti,
            rid,
            region_offset,
            buffer,
            size);
        tm2 = Wtime();
        assert(ret == 0);
        region_offset += size;

        sum += tm2-tm1;
        if(min == 0 || min > (tm2-tm1))
            min = tm2-tm1;
        if(max == 0 || max < (tm2-tm1))
            max = tm2-tm1;
    }

    printf("read\t%d\t%d\t%.9f\t%.9f\t%.9f\n", iterations, size, sum/((double)iterations), min, max);

    /* persist */
    ret = bake_bulk_persist(bti, rid);
    assert(ret == 0);

    free(buffer);

    return;
}
