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

#include "bb-proxy-rpc.h"

static hg_id_t proxy_bulk_write_id;
static hg_id_t proxy_shutdown_id;

int main(int argc, char *argv[])
{
    int i;
    char cli_addr_prefix[64] = {0};
    char *svr_addr_str;
    margo_instance_id mid;
    hg_addr_t svr_addr;
    hg_addr_t self_addr;
    char self_addr_str[128];
    hg_size_t self_addr_str_sz = 128;
    proxy_bulk_write_in_t in;
    proxy_bulk_write_out_t out;
    char *buf;
    hg_size_t buf_size;
    hg_handle_t handle;
    hg_bulk_t bulk_handle;
    hg_return_t hret;
    const char *test_str = "This is a test string for bake-proxy-test.";
    int ret;

    if(argc != 2)
    {
        fprintf(stderr, "Usage: bb-proxy-test <proxy server addr>\n");
        fprintf(stderr, "  Example: ./bb-proxy-test tcp://localhost:1234\n");
        return(-1);
    }
    svr_addr_str = argv[1];

    /* initialize Margo using the transport portion of the server
     * address (i.e., the part before the first : character if present)
     */
    for(i=0; (i<63 && svr_addr_str[i] != '\0' && svr_addr_str[i] != ':'); i++)
        cli_addr_prefix[i] = svr_addr_str[i];

    /* start margo */
    mid = margo_init(cli_addr_prefix, MARGO_SERVER_MODE, 0, 0);
    if(mid == MARGO_INSTANCE_NULL)
    {
        fprintf(stderr, "Error: margo_init()\n");
        return(-1);
    }

    proxy_bulk_write_id = MARGO_REGISTER(mid, "proxy_bulk_write",
        proxy_bulk_write_in_t, proxy_bulk_write_out_t, NULL);
    proxy_shutdown_id = MARGO_REGISTER(mid, "proxy_shutdown",
        void, void, NULL);

    /* look up the proxy server address */
    hret = margo_addr_lookup(mid, svr_addr_str, &svr_addr);
    if(hret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: margo_addr_lookup()\n");
        margo_finalize(mid);
        return(-1);
    }

    /* get self address to include in requests to proxy server */
    hret = margo_addr_self(mid, &self_addr);
    if(hret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: margo_addr_self()\n");
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }
    hret = margo_addr_to_string(mid, self_addr_str, &self_addr_str_sz, self_addr);
    if(hret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: margo_addr_to_string()\n");
        margo_addr_free(mid, self_addr);
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }
    margo_addr_free(mid, self_addr);

    buf = malloc(512);
    if(!buf)
    {
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }

    /* **************** */

    /* set up bulk handle and other proxy params to send in request */
    strcpy(buf, test_str);
    buf_size = strlen(test_str) + 1;

    hret = margo_bulk_create(mid, 1, (void **)&buf, &buf_size, HG_BULK_READ_ONLY,
        &in.bulk_handle);
    if(hret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: margo_bulk_create()\n");
        free(buf);
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }
    in.bulk_offset = 0;
    in.bulk_size = buf_size;
    in.bulk_addr = self_addr_str;

    hret = margo_create(mid, svr_addr, proxy_bulk_write_id, &handle);
    if(hret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: margo_create()\n");
        free(buf);
        margo_bulk_free(bulk_handle);
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }

    hret = margo_forward(handle, &in);
    if(hret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: margo_forward()\n");
        free(buf);
        margo_destroy(handle);
        margo_bulk_free(bulk_handle);
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: margo_get_output()\n");
        free(buf);
        margo_destroy(handle);
        margo_bulk_free(bulk_handle);
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }

    /* check return code */
    if(out.ret != 0)
    {
        fprintf(stderr, "Error: unexpected return from bake proxy write RPC\n");
        free(buf);
        margo_destroy(handle);
        margo_bulk_free(bulk_handle);
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }

    /* XXX check the buffer ? */

    margo_free_output(handle, &out);
    margo_destroy(handle);
    margo_bulk_free(bulk_handle);

    /* **************** */

    free(buf);

    /* send the shutdown signal to the proxy server */
    hret = margo_create(mid, svr_addr, proxy_shutdown_id, &handle);
    if(hret != HG_SUCCESS)
    {
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }

    hret = margo_forward(handle, NULL);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }

    margo_destroy(handle);
    margo_addr_free(mid, svr_addr);
    margo_finalize(mid);

    return(0);
}
