/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <assert.h>
#include <unistd.h>

#include "abt.h"
#include "abt-snoozer.h"
#include "bake-bulk.h"

/* client program that will shut down a BAKE bulk server. */

int main(int argc, char **argv) 
{
    int ret;
    bake_target_id_t bti;
    bake_bulk_region_id_t rid;
 
    if(argc != 2)
    {
        fprintf(stderr, "Usage: bb-copy-to <server addr>\n");
        fprintf(stderr, "  Example: ./bb-copy-to tcp://localhost:1234\n");
        return(-1);
    }       

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
        fprintf(stderr, "Error: ABT_snoozer_xstream_self_set()\n");
        return(-1);
    }

    ret = bake_probe_instance(argv[1], &bti);
    if(ret < 0)
    {
        fprintf(stderr, "Error: bake_probe_instance()\n");
        return(-1);
    }

    /* TODO: find local file, check it's size */

    /* TODO: create appropriate size region */
    ret = bake_bulk_create(bti, 1024, &rid);
    {
        fprintf(stderr, "Error: bake_bulk_create()\n");
        return(-1);
    }

    /* TODO: a way to print region id */

    /* TODO: data transfer */

    bake_release_instance(bti);
    
    ABT_finalize();

    return(0);
}

