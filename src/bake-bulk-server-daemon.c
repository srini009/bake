/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <uuid/uuid.h>
#include <margo.h>
#include <libpmemobj.h>
#include <bake-bulk-server.h>

int main(int argc, char **argv) 
{
    int ret;
    margo_instance_id mid;
    PMEMobjpool *bb_pmem_pool = NULL;
    struct bake_bulk_root bb_pmem_root;

    if(argc != 3)
    {
        fprintf(stderr, "Usage: bake-bulk-server <HG listening addr> <pmem pool>\n");
        fprintf(stderr, "  Example: ./bake-bulk-server tcp://localhost:1234 /dev/shm/foo.dat\n");
        return(-1);
    }

    ret = bake_server_makepool(argv[2], &bb_pmem_pool, &bb_pmem_root);

    /* start margo */
    /* use the main xstream for driving progress and executing rpc handlers */
    mid = margo_init(argv[1], MARGO_SERVER_MODE, 0, -1);
    assert(mid);

    /* register the bake bulk server */
    bake_server_register(mid, bb_pmem_pool, &bb_pmem_root);

    /* NOTE: at this point this server ULT has two options.  It can wait on
     * whatever mechanism it wants to (however long the daemon should run and
     * then call margo_finalize().  Otherwise, it can call
     * margo_wait_for_finalize() on the assumption that it should block until
     * some other entity calls margo_finalize().
     *
     * This example does the latter.  Margo will be finalized by a special
     * RPC from the client.
     *
     * This approach will allow the server to idle gracefully even when
     * executed in "single" mode, in which the main thread of the server
     * daemon and the progress thread for Mercury are executing in the same
     * ABT pool.
     */
    margo_wait_for_finalize(mid);

    pmemobj_close(bb_pmem_pool);

    return(0);
}

