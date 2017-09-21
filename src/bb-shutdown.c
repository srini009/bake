/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <assert.h>
#include <unistd.h>

#include "bake-bulk-client.h"

/* client program that will shut down a BAKE bulk server. */

int main(int argc, char **argv) 
{
    int ret;
    bake_target_id_t bti;
 
    if(argc != 2)
    {
        fprintf(stderr, "Usage: bb-shutdown <server addr to stop>\n");
        fprintf(stderr, "  Example: ./bb-shutdown tcp://localhost:1234\n");
        return(-1);
    }       

    ret = bake_probe_instance(argv[1], &bti);
    if(ret < 0)
    {
        fprintf(stderr, "Error: bake_probe_instance()\n");
        return(-1);
    }

    /* shutdown server */
    bake_shutdown_service(bti);

    bake_release_instance(bti);

    return(0);
}

