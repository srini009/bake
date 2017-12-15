/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <mercury.h>
#include <abt.h>
#include <margo.h>

#include "bake-client.h"

#define ALLOC_BUF_SIZE 512


int main(int argc, char *argv[])
{
    int i;
    char cli_addr_prefix[64] = {0};
    char *bake_svr_addr_str;
    margo_instance_id mid;
    hg_addr_t svr_addr;
    bake_target_id_t bti;
    bake_region_id_t the_rid;
    const char *test_str = "This is a test string for create-write-persist test.";
    char *buf;
    uint64_t buf_size;
    hg_return_t hret;
    int ret;

    if(argc != 2)
    {
        fprintf(stderr, "Usage: create-write-persist-test <bake server addr>\n");
        fprintf(stderr, "  Example: ./create-write-persist-test tcp://localhost:1234\n");
        return(-1);
    }
    bake_svr_addr_str = argv[1];

    /* initialize Margo using the transport portion of the server
     * address (i.e., the part before the first : character if present)
     */
    for(i=0; (i<63 && bake_svr_addr_str[i] != '\0' && bake_svr_addr_str[i] != ':'); i++)
        cli_addr_prefix[i] = bake_svr_addr_str[i];

    /* start margo */
    mid = margo_init(cli_addr_prefix, MARGO_SERVER_MODE, 0, 0);
    if(mid == MARGO_INSTANCE_NULL)
    {
        fprintf(stderr, "Error: margo_init()\n");
        return(-1);
    }

    /* look up the BAKE server address */
    hret = margo_addr_lookup(mid, bake_svr_addr_str, &svr_addr);
    if(hret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: margo_addr_lookup()\n");
        margo_finalize(mid);
        return(-1);
    }

    /* obtain info on the server's BAKE target */
    ret = bake_probe_instance(mid, svr_addr, &bti);
    if(ret != 0)
    {
        fprintf(stderr, "Error: bake_probe_instance()\n");
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }

    buf = malloc(ALLOC_BUF_SIZE);
    if(!buf)
    {
        bake_release_instance(bti);
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }

    /**** write phase ****/

    /* copy the test string into a buffer and forward to the proxy server */
    strcpy(buf, test_str);
    buf_size = strlen(test_str) + 1;

    ret = bake_create_write_persist(bti, buf_size, 0, buf, buf_size, &the_rid);
    if(ret != 0)
    {
        fprintf(stderr, "Error: bake_create_write_persist()\n");
        free(buf);
        bake_release_instance(bti);
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }

    /**** read-back phase ****/

    /* reset the buffer and read it back via BAKE */
    memset(buf, 0, ALLOC_BUF_SIZE);

    ret = bake_read(bti, the_rid, 0, buf, buf_size);
    if(ret != 0)
    {
        fprintf(stderr, "Error: bake_read()\n");
        free(buf);
        bake_release_instance(bti);
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }

    /* check to make sure we get back the string we expect */
    if(strcmp(buf, test_str) != 0)
    {
        fprintf(stderr, "Error: unexpected buffer contents returned from BAKE\n");
        free(buf);
        bake_release_instance(bti);
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }

    /* shutdown the server */
    ret = bake_shutdown_service(bti);

    /**** cleanup ****/

    free(buf);
    bake_release_instance(bti);
    margo_addr_free(mid, svr_addr);
    margo_finalize(mid);
    return(ret);
}

