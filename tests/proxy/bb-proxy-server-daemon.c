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
#include <bake-bulk-client.h>

#include "bb-proxy-rpc.h"

DECLARE_MARGO_RPC_HANDLER(proxy_bulk_write_ult)
DECLARE_MARGO_RPC_HANDLER(proxy_shutdown_ult)

struct options
{
    char *listen_addr_str;
    char *bake_svr_addr_str;
    char *host_file;
};

struct bb_proxy_server_context
{
    bake_target_id_t bb_svr_bti;
};

static struct bb_proxy_server_context *g_proxy_svr_ctx = NULL;

static void usage(int argc, char **argv)
{
    fprintf(stderr, "Usage: bb-proxy-server-daemon [OPTIONS] <listen_addr> <bake_server_addr>\n");
    fprintf(stderr, "       listen_addr is the Mercury address to listen on\n");
    fprintf(stderr, "       bake_server_addr is the Mercury address of the bake server\n");
    fprintf(stderr, "       [-f filename] to write the proxy server address to a file\n");
    fprintf(stderr, "Example: ./bb-proxy-server-daemon na+sm na+sm://3005/0\n");
    return;
}

static void parse_args(int argc, char **argv, struct options *opts)
{
    int opt;

    memset(opts, 0, sizeof(*opts));

    /* get options */
    while((opt = getopt(argc, argv, "f:")) != -1)
    {
        switch(opt)
        {
            case 'f':
                opts->host_file = optarg;
                break;
            default:
                usage(argc, argv);
                exit(EXIT_FAILURE);
        }
    }

    /* get required arguments after options */
    if((argc - optind) != 2)
    {
        usage(argc, argv);
        exit(EXIT_FAILURE);
    }
    opts->listen_addr_str = argv[optind++];
    opts->bake_svr_addr_str = argv[optind++];

    return;
}

int main(int argc, char **argv) 
{
    struct options opts;
    margo_instance_id mid;
    hg_addr_t bake_svr_addr;
    hg_return_t hret;
    int ret;

    parse_args(argc, argv, &opts);

    g_proxy_svr_ctx = malloc(sizeof(*g_proxy_svr_ctx));
    if(!g_proxy_svr_ctx)
        return(-1);
    memset(g_proxy_svr_ctx, 0, sizeof(*g_proxy_svr_ctx));

    /* start margo */
    mid = margo_init(opts.listen_addr_str, MARGO_SERVER_MODE, 0, -1);
    if(mid == MARGO_INSTANCE_NULL)
    {
        fprintf(stderr, "Error: margo_init()\n");
        return(-1);
    }

    if(opts.host_file)
    {
        /* write the server address to file if requested */
        FILE *fp;
        hg_addr_t self_addr;
        char self_addr_str[128];
        hg_size_t self_addr_str_sz = 128;
        hg_return_t hret;

        /* figure out what address this server is listening on */
        hret = margo_addr_self(mid, &self_addr);
        if(hret != HG_SUCCESS)
        {
            fprintf(stderr, "Error: margo_addr_self()\n");
            margo_finalize(mid);
            return(-1);
        }
        hret = margo_addr_to_string(mid, self_addr_str, &self_addr_str_sz, self_addr);
        if(hret != HG_SUCCESS)
        {
            fprintf(stderr, "Error: margo_addr_to_string()\n");
            margo_addr_free(mid, self_addr);
            margo_finalize(mid);
            return(-1);
        }
        margo_addr_free(mid, self_addr);

        fp = fopen(opts.host_file, "w");
        if(!fp)
        {
            perror("fopen");
            margo_finalize(mid);
            return(-1);
        }

        fprintf(fp, "%s", self_addr_str);
        fclose(fp);
    }

    /* lookup the bake-bulk server address */
    hret = margo_addr_lookup(mid, opts.bake_svr_addr_str, &bake_svr_addr);
    if(hret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: margo_addr_to_string()\n");
        margo_finalize(mid);
        return(-1);
    }

    /* probe the bake-bulk server for an instance */
    ret = bake_probe_instance(mid, bake_svr_addr, &g_proxy_svr_ctx->bb_svr_bti);
    if(ret < 0)
    {
        fprintf(stderr, "Error: bake_probe_instance()\n");
        return(-1);
    }
    margo_addr_free(mid, bake_svr_addr);

    /* register proxy service RPCs */
    MARGO_REGISTER(mid, "proxy_bulk_write", proxy_bulk_write_in_t, proxy_bulk_write_out_t,
        proxy_bulk_write_ult);
    MARGO_REGISTER(mid, "proxy_shutdown", void, void, proxy_shutdown_ult);

    /* wait for the shutdown signal */
    margo_wait_for_finalize(mid);

    return(0);
}

static void proxy_bulk_write_ult(hg_handle_t handle)
{
    proxy_bulk_write_in_t in;
    proxy_bulk_write_out_t out;
    bake_bulk_region_id_t rid;
    hg_return_t hret;
    int ret;

    assert(g_proxy_svr_ctx);

    /* get RPC input */
    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS);

    /* create bake region to store this write in */
    ret = bake_bulk_create(g_proxy_svr_ctx->bb_svr_bti, in.bulk_size, &rid);
    assert(ret == 0);

    /* perform proxy write on behalf of client */
    ret = bake_bulk_proxy_write(g_proxy_svr_ctx->bb_svr_bti, rid, 0,
        in.bulk_handle, in.bulk_offset, in.bulk_addr, in.bulk_size);
    assert(ret == 0);

    /* persist the bake region */
    ret = bake_bulk_persist(g_proxy_svr_ctx->bb_svr_bti, rid);
    assert(ret == 0);

    /* set return value */
    out.ret = 2;

    hret = margo_respond(handle, &out);
    assert(hret == HG_SUCCESS);

#if 1
    char *buf = malloc(in.bulk_size);
    memset(buf, 0, in.bulk_size);

    ret = bake_bulk_read(g_proxy_svr_ctx->bb_svr_bti, rid, 0, buf, in.bulk_size);
    assert(ret == 0);

    printf("bake got the buf %s\n", buf);
    free(buf);
#endif

    margo_free_input(handle, &in);
    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(proxy_bulk_write_ult)

static void proxy_shutdown_ult(hg_handle_t handle)
{
    margo_instance_id mid;
    hg_return_t hret;

    assert(g_proxy_svr_ctx);

    /* get margo instance from handle */
    mid = margo_hg_handle_get_instance(handle);
    assert(mid != MARGO_INSTANCE_NULL);

    hret = margo_respond(handle, NULL);
    assert(hret == HG_SUCCESS);

    margo_destroy(handle);

    /* forward shutdown to the bake-bulk server */
    bake_shutdown_service(g_proxy_svr_ctx->bb_svr_bti);

    /* cleanup global state */
    bake_release_instance(g_proxy_svr_ctx->bb_svr_bti);
    free(g_proxy_svr_ctx);

    margo_finalize(mid);

    return;
}
DEFINE_MARGO_RPC_HANDLER(proxy_shutdown_ult)
