/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <abt.h>
#include <abt-snoozer.h>
#include <margo.h>

#include "bake-bulk-rpc.h"

/* client program that will shut down a BAKE bulk server. */

static hg_id_t bake_bulk_shutdown_id;

int main(int argc, char **argv) 
{
    int ret;
    ABT_xstream xstream;
    ABT_pool pool;
    margo_instance_id mid;
    hg_context_t *hg_context;
    hg_class_t *hg_class;
    hg_addr_t svr_addr = HG_ADDR_NULL;
    hg_handle_t handle;
 
    if(argc != 2)
    {
        fprintf(stderr, "Usage: bb-shutdown <server addr to stop>\n");
        fprintf(stderr, "  Example: ./bb-shutdown tcp://localhost:1234\n");
        return(-1);
    }       

    /* boilerplate HG initialization steps */
    /***************************************/
    /* NOTE: the listening address is not actually used in this case (the
     * na_listen flag is false); but we pass in the *target* server address
     * here to make sure that Mercury starts up the correct transport
     */
    hg_class = HG_Init(argv[1], HG_FALSE);
    if(!hg_class)
    {
        fprintf(stderr, "Error: HG_Init()\n");
        return(-1);
    }
    hg_context = HG_Context_create(hg_class);
    if(!hg_context)
    {
        fprintf(stderr, "Error: HG_Context_create()\n");
        HG_Finalize(hg_class);
        return(-1);
    }

    /* set up argobots */
    /***************************************/
    ret = ABT_init(argc, argv);
    if(ret != 0)
    {
        fprintf(stderr, "Error: ABT_init()\n");
        return(-1);
    }

    /* set primary ES to idle without polling */
    ret = ABT_snoozer_xstream_self_set();
    if(ret != 0)
    {
        fprintf(stderr, "Error: ABT_snoozer_xstream_self_set()\n");
        return(-1);
    }

    /* retrieve current pool to use for ULT creation */
    ret = ABT_xstream_self(&xstream);
    if(ret != 0)
    {
        fprintf(stderr, "Error: ABT_xstream_self()\n");
        return(-1);
    }
    ret = ABT_xstream_get_main_pools(xstream, 1, &pool);
    if(ret != 0)
    {
        fprintf(stderr, "Error: ABT_xstream_get_main_pools()\n");
        return(-1);
    }

    /* actually start margo */
    /* provide argobots pools for driving communication progress and
     * executing rpc handlers as well as class and context for Mercury
     * communication.  The rpc handler pool is null in this example program
     * because this is a pure client that will not be servicing rpc requests.
     */
    /***************************************/
    mid = margo_init_pool(pool, ABT_POOL_NULL, hg_context);

    /* register RPC */
    bake_bulk_shutdown_id = MERCURY_REGISTER(hg_class, "bake_bulk_shutdown_rpc", void, void, 
        NULL);

    /* send one rpc to server to shut it down */
    /* find addr for server */
    ret = margo_addr_lookup(mid, argv[1], &svr_addr);
    assert(ret == 0);

    /* create handle */
    ret = HG_Create(hg_context, svr_addr, bake_bulk_shutdown_id, &handle);
    assert(ret == 0);

    margo_forward(mid, handle, NULL);

    /* shut down everything */
    margo_finalize(mid);
    
    ABT_finalize();

    HG_Context_destroy(hg_context);
    HG_Finalize(hg_class);

    return(0);
}

