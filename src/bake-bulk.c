/*
 * (C) 2016 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <assert.h>
#include <bake-bulk.h>
#include <margo.h>
#include "uthash.h"
#include "bake-bulk-rpc.h"

/* Refers to a single Mercury/Margo initialization, for now this is shared by
 * all remote targets.  In the future we probably need to support multiple in
 * case we run atop more than one transport at a time.
 */
struct hg_instance
{
    margo_instance_id mid;  
    hg_class_t *hg_class;
    hg_context_t *hg_context;
    int refct;
    
    hg_id_t bake_bulk_probe_id;
    hg_id_t bake_bulk_shutdown_id; 
    hg_id_t bake_bulk_create_id;
    hg_id_t bake_bulk_write_id;
    hg_id_t bake_bulk_persist_id;
    hg_id_t bake_bulk_get_size_id;
    hg_id_t bake_bulk_read_id;
};

/* Refers to an instance connected to a specific target */
struct bake_instance
{
    bake_target_id_t bti; /* persistent identifier for this target */
    hg_addr_t dest;         /* remote Mercury address */
};

/* TODO: replace later, hard coded global instance */
struct bake_instance g_binst = {
    .dest = HG_ADDR_NULL,
};

struct hg_instance g_hginst = {
    .mid = MARGO_INSTANCE_NULL,
    .hg_class = NULL,
    .hg_context = NULL,
    .refct = 0,
};

static int hg_instance_init(const char *mercury_dest)
{
    /* have we already started a Mercury instance? */
    if(g_hginst.refct > 0)
        return(0);

    /* boilerplate HG initialization steps */
    /***************************************/
    /* NOTE: the listening address is not actually used in this case (the
     * na_listen flag is false); but we pass in the *target* server address
     * here to make sure that Mercury starts up the correct transport
     */
    g_hginst.hg_class = HG_Init(mercury_dest, HG_FALSE);
    if(!g_hginst.hg_class)
    {
        return(-1);
    }
    g_hginst.hg_context = HG_Context_create(g_hginst.hg_class);
    if(!g_hginst.hg_context)
    {
        HG_Finalize(g_hginst.hg_class);
        return(-1);
    }

    /* register RPCs */
    g_hginst.bake_bulk_probe_id = 
        MERCURY_REGISTER(g_hginst.hg_class, 
        "bake_bulk_probe_rpc", void, bake_bulk_probe_out_t, 
        NULL);
    g_hginst.bake_bulk_shutdown_id = 
        MERCURY_REGISTER(g_hginst.hg_class, 
        "bake_bulk_shutdown_rpc", void, void, 
        NULL);
    g_hginst.bake_bulk_create_id = 
        MERCURY_REGISTER(g_hginst.hg_class, 
        "bake_bulk_create_rpc", 
        bake_bulk_create_in_t,
        bake_bulk_create_out_t,
        NULL);
    g_hginst.bake_bulk_write_id = 
        MERCURY_REGISTER(g_hginst.hg_class, 
        "bake_bulk_write_rpc", 
        bake_bulk_write_in_t,
        bake_bulk_write_out_t,
        NULL);
    g_hginst.bake_bulk_persist_id = 
        MERCURY_REGISTER(g_hginst.hg_class, 
        "bake_bulk_persist_rpc", 
        bake_bulk_persist_in_t,
        bake_bulk_persist_out_t,
        NULL);
    g_hginst.bake_bulk_get_size_id = 
        MERCURY_REGISTER(g_hginst.hg_class, 
        "bake_bulk_get_size_rpc", 
        bake_bulk_get_size_in_t,
        bake_bulk_get_size_out_t,
        NULL);
    g_hginst.bake_bulk_read_id = 
        MERCURY_REGISTER(g_hginst.hg_class, 
        "bake_bulk_read_rpc", 
        bake_bulk_read_in_t,
        bake_bulk_read_out_t,
        NULL);

    g_hginst.mid = margo_init(0, 0, g_hginst.hg_context);
    if(!g_hginst.mid)
    {
        HG_Context_destroy(g_hginst.hg_context);
        HG_Finalize(g_hginst.hg_class);
        return(-1);
    }
    g_hginst.refct = 1;

    return(0);
}

void hg_instance_finalize(void)
{
    g_hginst.refct--;

    assert(g_hginst.refct > -1);

    if(g_hginst.refct == 0)
    {
        margo_finalize(g_hginst.mid);
        HG_Context_destroy(g_hginst.hg_context);
        HG_Finalize(g_hginst.hg_class);
    }
}

int bake_probe_instance(
    const char *mercury_dest,
    bake_target_id_t *bti)
{
    hg_return_t hret;
    int ret;
    bake_bulk_probe_out_t out;
    hg_handle_t handle;

    ret = hg_instance_init(mercury_dest);
    if(ret < 0)
        return(ret);

    hret = margo_addr_lookup(g_hginst.mid, mercury_dest, &g_binst.dest);
    if(hret != HG_SUCCESS)
    {
        hg_instance_finalize();
        return(-1);
    }

    /* create handle */
    hret = HG_Create(g_hginst.hg_context, g_binst.dest, 
        g_hginst.bake_bulk_probe_id, &handle);
    if(hret != HG_SUCCESS)
    {
        return(-1);
    }

    hret = margo_forward(g_hginst.mid, handle, NULL);
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
    *bti = out.bti;

    HG_Free_output(handle, &out);
    HG_Destroy(handle);

    return(ret);
}
  
void bake_release_instance(
    bake_target_id_t bti)
{
    HG_Addr_free(g_hginst.hg_class, g_binst.dest);
    hg_instance_finalize();
    memset(&g_binst, 0, sizeof(g_binst));
    return;
}

int bake_shutdown_service(bake_target_id_t bti)
{
    hg_return_t hret;
    hg_handle_t handle;

    /* create handle */
    hret = HG_Create(g_hginst.hg_context, g_binst.dest, 
        g_hginst.bake_bulk_shutdown_id, &handle);
    if(hret != HG_SUCCESS)
    {
        return(-1);
    }

    hret = margo_forward(g_hginst.mid, handle, NULL);
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
    hg_return_t hret;
    hg_handle_t handle;
    bake_bulk_write_in_t in;
    bake_bulk_write_out_t out;
    int ret;

    in.bti = bti;
    in.rid = rid;
    in.region_offset = region_offset;

    hret = HG_Bulk_create(g_hginst.hg_class, 1, (void**)(&buf), &buf_size, 
        HG_BULK_READ_ONLY, &in.bulk_handle);
    if(hret != HG_SUCCESS)
    {
        return(-1);
    }
   
    /* create handle */
    hret = HG_Create(g_hginst.hg_context, g_binst.dest, 
        g_hginst.bake_bulk_write_id, &handle);
    if(hret != HG_SUCCESS)
    {
        HG_Bulk_free(in.bulk_handle);
        return(-1);
    }

    hret = margo_forward(g_hginst.mid, handle, &in);
    if(hret != HG_SUCCESS)
    {
        HG_Destroy(handle);
        HG_Bulk_free(in.bulk_handle);
        return(-1);
    }

    hret = HG_Get_output(handle, &out);
    if(hret != HG_SUCCESS)
    {
        HG_Destroy(handle);
        HG_Bulk_free(in.bulk_handle);
        return(-1);
    }
    
    ret = out.ret;

    HG_Free_output(handle, &out);
    HG_Destroy(handle);
    HG_Bulk_free(in.bulk_handle);
    return(ret);
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

    in.bti = bti;
    in.region_size = region_size;

    /* create handle */
    hret = HG_Create(g_hginst.hg_context, g_binst.dest, 
        g_hginst.bake_bulk_create_id, &handle);
    if(hret != HG_SUCCESS)
    {
        return(-1);
    }

    hret = margo_forward(g_hginst.mid, handle, &in);
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


int bake_bulk_persist(
    bake_target_id_t bti,
    bake_bulk_region_id_t rid)
{
    hg_return_t hret;
    hg_handle_t handle;
    bake_bulk_persist_in_t in;
    bake_bulk_persist_out_t out;
    int ret;

    in.bti = bti;
    in.rid = rid;

    /* create handle */
    hret = HG_Create(g_hginst.hg_context, g_binst.dest, 
        g_hginst.bake_bulk_persist_id, &handle);
    if(hret != HG_SUCCESS)
    {
        return(-1);
    }

    hret = margo_forward(g_hginst.mid, handle, &in);
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

    HG_Free_output(handle, &out);
    HG_Destroy(handle);
    return(ret);
}

int bake_bulk_get_size(
    bake_target_id_t bti,
    bake_bulk_region_id_t rid,
    uint64_t *region_size)
{
    hg_return_t hret;
    hg_handle_t handle;
    bake_bulk_get_size_in_t in;
    bake_bulk_get_size_out_t out;
    int ret;

    in.bti = bti;
    in.rid = rid;

    /* create handle */
    hret = HG_Create(g_hginst.hg_context, g_binst.dest, 
        g_hginst.bake_bulk_get_size_id, &handle);
    if(hret != HG_SUCCESS)
    {
        return(-1);
    }

    hret = margo_forward(g_hginst.mid, handle, &in);
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
    *region_size = out.size;

    HG_Free_output(handle, &out);
    HG_Destroy(handle);
    return(ret);
}

int bake_bulk_read(
    bake_target_id_t bti,
    bake_bulk_region_id_t rid,
    uint64_t region_offset,
    void *buf,
    uint64_t buf_size)
{
    hg_return_t hret;
    hg_handle_t handle;
    bake_bulk_read_in_t in;
    bake_bulk_read_out_t out;
    int ret;

    in.bti = bti;
    in.rid = rid;
    in.region_offset = region_offset;

    hret = HG_Bulk_create(g_hginst.hg_class, 1, (void**)(&buf), &buf_size, 
        HG_BULK_WRITE_ONLY, &in.bulk_handle);
    if(hret != HG_SUCCESS)
    {
        return(-1);
    }
   
    /* create handle */
    hret = HG_Create(g_hginst.hg_context, g_binst.dest, 
        g_hginst.bake_bulk_read_id, &handle);
    if(hret != HG_SUCCESS)
    {
        HG_Bulk_free(in.bulk_handle);
        return(-1);
    }

    hret = margo_forward(g_hginst.mid, handle, &in);
    if(hret != HG_SUCCESS)
    {
        HG_Destroy(handle);
        HG_Bulk_free(in.bulk_handle);
        return(-1);
    }

    hret = HG_Get_output(handle, &out);
    if(hret != HG_SUCCESS)
    {
        HG_Destroy(handle);
        HG_Bulk_free(in.bulk_handle);
        return(-1);
    }
    
    ret = out.ret;

    HG_Free_output(handle, &out);
    HG_Destroy(handle);
    HG_Bulk_free(in.bulk_handle);
    return(ret);
}


