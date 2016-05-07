/*
 * (C) 2016 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <bake-bulk.h>
#include <margo.h>
#include "bake-bulk-rpc.h"

struct bake_instance
{
    bake_target_id_t bti; /* persistent identifier for this target */
    hg_addr_t dest;         /* remote Mercury address */

    /* TODO: stuff will probably be split out into an hg instance */
    margo_instance_id mid;  /* Margo instance */
    hg_class_t *hg_class;
    hg_context_t *hg_context;
};

struct bake_instance g_binst; /* TODO: replace later, hard coded global instance */
static hg_id_t g_bake_bulk_shutdown_id; /* TODO: probably not global in the long run either */
static hg_id_t g_bake_bulk_create_id; /* TODO: probably not global in the long run either */
static hg_id_t g_bake_bulk_write_id; /* TODO: probably not global in the long run either */

int bake_probe_instance(
    const char *mercury_dest,
    bake_target_id_t *bti)
{
    *bti = 0; /* TODO: use a real id eventually, just a placeholder for now */
    hg_return_t hret;

    memset(&g_binst, 0, sizeof(g_binst));

    /* TODO: eventually we will not init HG on every target probe, probably
     * separate step so hg instance can be shared.  Simple test case is to
     * build a single client application that communicates with two distinct
     * targets (on different servers) simultaneously.
     */

    /* boilerplate HG initialization steps */
    /***************************************/
    /* NOTE: the listening address is not actually used in this case (the
     * na_listen flag is false); but we pass in the *target* server address
     * here to make sure that Mercury starts up the correct transport
     */
    g_binst.hg_class = HG_Init(mercury_dest, HG_FALSE);
    if(!g_binst.hg_class)
    {
        return(-1);
    }
    g_binst.hg_context = HG_Context_create(g_binst.hg_class);
    if(!g_binst.hg_context)
    {
        HG_Finalize(g_binst.hg_class);
        return(-1);
    }

    /* register RPCs */
    g_bake_bulk_shutdown_id = 
        MERCURY_REGISTER(g_binst.hg_class, 
        "bake_bulk_shutdown_rpc", void, void, 
        NULL);
    g_bake_bulk_create_id = 
        MERCURY_REGISTER(g_binst.hg_class, 
        "bake_bulk_create_rpc", 
        bake_bulk_create_in_t,
        bake_bulk_create_out_t,
        NULL);
    g_bake_bulk_write_id = 
        MERCURY_REGISTER(g_binst.hg_class, 
        "bake_bulk_write_rpc", 
        bake_bulk_write_in_t,
        bake_bulk_write_out_t,
        NULL);

    g_binst.mid = margo_init(0, 0, g_binst.hg_context);
    if(!g_binst.mid)
    {
        HG_Context_destroy(g_binst.hg_context);
        HG_Finalize(g_binst.hg_class);
        return(-1);
    }

    hret = margo_addr_lookup(g_binst.mid, mercury_dest, &g_binst.dest);
    if(hret != HG_SUCCESS)
    {
        margo_finalize(g_binst.mid);
        HG_Context_destroy(g_binst.hg_context);
        HG_Finalize(g_binst.hg_class);
        return(-1);
    }

    return(0);
}
  
void bake_release_instance(
    bake_target_id_t bti)
{
    HG_Addr_free(g_binst.hg_class, g_binst.dest);
    margo_finalize(g_binst.mid);
    HG_Context_destroy(g_binst.hg_context);
    HG_Finalize(g_binst.hg_class);
    memset(&g_binst, 0, sizeof(g_binst));
    return;
}

int bake_shutdown_service(bake_target_id_t bti)
{
    hg_return_t hret;
    hg_handle_t handle;

    /* create handle */
    hret = HG_Create(g_binst.hg_context, g_binst.dest, 
        g_bake_bulk_shutdown_id, &handle);
    if(hret != HG_SUCCESS)
    {
        return(-1);
    }

    hret = margo_forward(g_binst.mid, handle, NULL);
    if(hret != HG_SUCCESS)
    {
        HG_Destroy(handle);
        return(-1);
    }

    HG_Destroy(handle);
    return(0);
}

int bake_bulk_write(
    bake_target_id_t bti,
    bake_bulk_region_id_t rid,
    uint64_t region_offset,
    void const *buf,
    uint64_t buf_size)
{

    return(-1);
}

int bake_bulk_create(
    bake_target_id_t bti,
    uint64_t region_size,
    bake_bulk_region_id_t *rid)
{
    hg_return_t hret;
    hg_handle_t handle;
    bake_bulk_create_in_t in;
    bake_bulk_create_out_t out;
    int ret;

    /* create handle */
    hret = HG_Create(g_binst.hg_context, g_binst.dest, 
        g_bake_bulk_create_id, &handle);
    if(hret != HG_SUCCESS)
    {
        return(-1);
    }

    in.bti = bti;
    in.region_size = region_size;

    hret = margo_forward(g_binst.mid, handle, &in);
    if(hret != HG_SUCCESS)
    {
        HG_Destroy(handle);
        return(-1);
    }

    hret = HG_Get_output(handle, &out);
    if(hret != HG_SUCCESS)
    {
        HG_Destroy(handle);
        return(-1);
    }

    ret = out.ret;
    *rid = out.rid;

    HG_Free_output(handle, &out);
    HG_Destroy(handle);
    return(ret);
}


