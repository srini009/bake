/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <assert.h>
#include <libpmemobj.h>

#include "bake-bulk-rpc.h"

/* TODO: this should not be global in the long run; server may provide access
 * to multiple targets
 */
extern PMEMobjpool *pmem_pool;

/* service a remote RPC that instructs the server daemon to shut down */
static void bake_bulk_shutdown_ult(hg_handle_t handle)
{
    hg_return_t hret;
    struct hg_info *hgi;
    margo_instance_id mid;

    printf("Got RPC request to shutdown.\n");

    hgi = HG_Get_info(handle);
    assert(hgi);
    mid = margo_hg_class_to_instance(hgi->hg_class);

    hret = margo_respond(mid, handle, NULL);
    assert(hret == HG_SUCCESS);

    HG_Destroy(handle);

    /* NOTE: we assume that the server daemon is using
     * margo_wait_for_finalize() to suspend until this RPC executes, so there
     * is no need to send any extra signal to notify it.
     */
    margo_finalize(mid);

    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_bulk_shutdown_ult)

/* service a remote RPC that creates a bulk region */
static void bake_bulk_create_ult(hg_handle_t handle)
{
    bake_bulk_create_out_t out;
    bake_bulk_create_in_t in;
    PMEMoid oid;
    hg_return_t hret;

    printf("Got RPC request to create bulk region.\n");
    
    memset(&out, 0, sizeof(out));

    hret = HG_Get_input(handle, &in);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        HG_Respond(handle, NULL, NULL, &out);
        HG_Destroy(handle);
        return;
    }

    out.ret = pmemobj_alloc(pmem_pool, &oid, in.region_size, 0, NULL, NULL);
    if(out.ret == 0)
    {
        /* TODO: real translation functions for opaque type */
        memcpy(&out.rid, &oid, sizeof(oid));
    }

    HG_Free_input(handle, &in);
    HG_Respond(handle, NULL, NULL, &out);
    HG_Destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_bulk_create_ult)

/* service a remote RPC that writes to a bulk region */
static void bake_bulk_write_ult(hg_handle_t handle)
{
    bake_bulk_write_out_t out;
    bake_bulk_write_in_t in;
    hg_return_t hret;
    PMEMoid oid;
    char* buffer;
    hg_size_t size;
    hg_bulk_t bulk_handle;
    struct hg_info *hgi;
    margo_instance_id mid;

    printf("Got RPC request to write bulk region.\n");
    
    memset(&out, 0, sizeof(out));

    hgi = HG_Get_info(handle);
    assert(hgi);
    mid = margo_hg_class_to_instance(hgi->hg_class);

    hret = HG_Get_input(handle, &in);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        HG_Respond(handle, NULL, NULL, &out);
        HG_Destroy(handle);
        return;
    }

    /* TODO: real translation functions for opaque type */
    memcpy(&oid, &in.rid, sizeof(oid));

    /* find memory address for target object */
    buffer = pmemobj_direct(oid);
    if(!buffer)
    {
        out.ret = -1;
        HG_Free_input(handle, &in);
        HG_Respond(handle, NULL, NULL, &out);
        HG_Destroy(handle);
        return;
    }

    size = HG_Bulk_get_size(in.bulk_handle);

    /* create bulk handle for local side of transfer */
    hret = HG_Bulk_create(hgi->hg_class, 1, (void**)(&buffer), &size, 
        HG_BULK_WRITE_ONLY, &bulk_handle);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        HG_Free_input(handle, &in);
        HG_Respond(handle, NULL, NULL, &out);
        HG_Destroy(handle);
        return;
    }

    hret = margo_bulk_transfer(mid, HG_BULK_PULL, hgi->addr, in.bulk_handle,
        0, bulk_handle, 0, size);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        HG_Bulk_free(bulk_handle);
        HG_Free_input(handle, &in);
        HG_Respond(handle, NULL, NULL, &out);
        HG_Destroy(handle);
        return;
    }

    out.ret = 0;

    HG_Bulk_free(bulk_handle);
    HG_Free_input(handle, &in);
    HG_Respond(handle, NULL, NULL, &out);
    HG_Destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_bulk_write_ult)

