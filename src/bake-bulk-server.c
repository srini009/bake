/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <assert.h>
#include <bake-bulk-server.h>
#include <libpmemobj.h>
#include "bake-bulk-rpc.h"

/* definition of internal region_id_t identifier for libpmemobj back end */
typedef struct {
    PMEMoid oid;
    uint64_t size;
} pmemobj_region_id_t;

/* TODO: this should not be global in the long run; server may provide access
 * to multiple targets
 */
static PMEMobjpool *g_pmem_pool = NULL;
static struct bake_bulk_root *g_pmem_root = NULL;

int bake_server_makepool(
	char *poolname, PMEMobjpool **bb_pmem_pool,
	struct bake_bulk_root *bb_pmem_root)
{
    PMEMoid root_oid;
    char target_string[64];

    /* open pmem pool */
    *bb_pmem_pool = pmemobj_open(poolname, NULL);
    if(!bb_pmem_pool)
    {
        fprintf(stderr, "pmemobj_open: %s\n", pmemobj_errormsg());
        return(-1);
    }

    /* find root */
    root_oid = pmemobj_root(*bb_pmem_pool, sizeof(*bb_pmem_root));
    bb_pmem_root = pmemobj_direct(root_oid);
    if(uuid_is_null(bb_pmem_root->target_id.id))
    {
        uuid_generate(bb_pmem_root->target_id.id);
        pmemobj_persist(*bb_pmem_pool, bb_pmem_root, sizeof(*bb_pmem_root));
    }
    uuid_unparse(bb_pmem_root->target_id.id, target_string);
    fprintf(stderr, "BAKE target ID: %s\n", target_string);

    return 0;
}


void bake_server_register(margo_instance_id mid, PMEMobjpool *bb_pmem_pool,
    struct bake_bulk_root *bb_pmem_root)
{
    /* register RPCs */
    MARGO_REGISTER(mid, "bake_bulk_shutdown_rpc", void, void,
        bake_bulk_shutdown_ult);
    MARGO_REGISTER(mid, "bake_bulk_create_rpc", bake_bulk_create_in_t,
        bake_bulk_create_out_t,
        bake_bulk_create_ult);
    MARGO_REGISTER(mid, "bake_bulk_write_rpc", bake_bulk_write_in_t,
        bake_bulk_write_out_t,
        bake_bulk_write_ult);
    MARGO_REGISTER(mid, "bake_bulk_eager_write_rpc", bake_bulk_eager_write_in_t,
        bake_bulk_eager_write_out_t,
        bake_bulk_eager_write_ult);
    MARGO_REGISTER(mid, "bake_bulk_eager_read_rpc", bake_bulk_eager_read_in_t,
        bake_bulk_eager_read_out_t,
        bake_bulk_eager_read_ult);
    MARGO_REGISTER(mid, "bake_bulk_persist_rpc", bake_bulk_persist_in_t,
        bake_bulk_persist_out_t,
        bake_bulk_persist_ult);
    MARGO_REGISTER(mid, "bake_bulk_get_size_rpc", bake_bulk_get_size_in_t,
        bake_bulk_get_size_out_t,
        bake_bulk_get_size_ult);
    MARGO_REGISTER(mid, "bake_bulk_read_rpc", bake_bulk_read_in_t,
        bake_bulk_read_out_t,
        bake_bulk_read_ult);
    MARGO_REGISTER(mid, "bake_bulk_probe_rpc", void,
        bake_bulk_probe_out_t,
        bake_bulk_probe_ult);
    MARGO_REGISTER(mid, "bake_bulk_noop_rpc", void,
        void,
        bake_bulk_noop_ult);

    /* set global pmem variables needed by the bake server */
    g_pmem_pool = bb_pmem_pool;
    g_pmem_root = bb_pmem_root;

    return;
}

/* service a remote RPC that instructs the server daemon to shut down */
static void bake_bulk_shutdown_ult(hg_handle_t handle)
{
    hg_return_t hret;
    margo_instance_id mid;

    // printf("Got RPC request to shutdown.\n");

    mid = margo_hg_handle_get_instance(handle);

    hret = margo_respond(handle, NULL);
    assert(hret == HG_SUCCESS);

    margo_destroy(handle);

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
    hg_return_t hret;
    pmemobj_region_id_t* prid;

    /* TODO: this check needs to be somewhere else */
    assert(sizeof(pmemobj_region_id_t) <= BAKE_BULK_REGION_ID_DATA_SIZE);
    // printf("Got RPC request to create bulk region.\n");
    
    memset(&out, 0, sizeof(out));

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    prid = (pmemobj_region_id_t*)out.rid.data;
    prid->size = in.region_size;
    out.ret = pmemobj_alloc(g_pmem_pool, &prid->oid, in.region_size, 0, NULL, NULL);

    margo_free_input(handle, &in);
    margo_respond(handle, &out);
    margo_destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_bulk_create_ult)

/* service a remote RPC that writes to a bulk region */
static void bake_bulk_write_ult(hg_handle_t handle)
{
    bake_bulk_write_out_t out;
    bake_bulk_write_in_t in;
    hg_return_t hret;
    char* buffer;
    hg_size_t size;
    hg_bulk_t bulk_handle;
    const struct hg_info *hgi;
    margo_instance_id mid;
    pmemobj_region_id_t* prid;

    // printf("Got RPC request to write bulk region.\n");
    
    memset(&out, 0, sizeof(out));

    hgi = margo_get_info(handle);
    assert(hgi);
    mid = margo_hg_info_get_instance(hgi);

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    prid = (pmemobj_region_id_t*)in.rid.data;

    /* find memory address for target object */
    buffer = pmemobj_direct(prid->oid);
    if(!buffer)
    {
        out.ret = -1;
        margo_free_input(handle, &in);
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    size = margo_bulk_get_size(in.bulk_handle);

    /* create bulk handle for local side of transfer */
    hret = margo_bulk_create(mid, 1, (void**)(&buffer), &size, 
        HG_BULK_WRITE_ONLY, &bulk_handle);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        margo_free_input(handle, &in);
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    hret = margo_bulk_transfer(mid, HG_BULK_PULL, hgi->addr, in.bulk_handle,
        0, bulk_handle, 0, size);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        margo_bulk_free(bulk_handle);
        margo_free_input(handle, &in);
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    out.ret = 0;

    margo_bulk_free(bulk_handle);
    margo_free_input(handle, &in);
    margo_respond(handle, &out);
    margo_destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_bulk_write_ult)


/* service a remote RPC that writes to a bulk region in eager mode */
static void bake_bulk_eager_write_ult(hg_handle_t handle)
{
    bake_bulk_eager_write_out_t out;
    bake_bulk_eager_write_in_t in;
    hg_return_t hret;
    char* buffer;
    hg_bulk_t bulk_handle;
    pmemobj_region_id_t* prid;

    // printf("Got RPC request to write bulk region.\n");
    
    memset(&out, 0, sizeof(out));

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    prid = (pmemobj_region_id_t*)in.rid.data;

    /* find memory address for target object */
    buffer = pmemobj_direct(prid->oid);
    if(!buffer)
    {
        out.ret = -1;
        margo_free_input(handle, &in);
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    memcpy(buffer, in.buffer, in.size);

    out.ret = 0;

    margo_free_input(handle, &in);
    margo_respond(handle, &out);
    margo_destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_bulk_eager_write_ult)

/* service a remote RPC that persists to a bulk region */
static void bake_bulk_persist_ult(hg_handle_t handle)
{
    bake_bulk_persist_out_t out;
    bake_bulk_persist_in_t in;
    hg_return_t hret;
    char* buffer;
    pmemobj_region_id_t* prid;

    // printf("Got RPC request to persist bulk region.\n");
    
    memset(&out, 0, sizeof(out));

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    prid = (pmemobj_region_id_t*)in.rid.data;

    /* find memory address for target object */
    buffer = pmemobj_direct(prid->oid);
    if(!buffer)
    {
        out.ret = -1;
        margo_free_input(handle, &in);
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    /* TODO: should this have an abt shim in case it blocks? */
    pmemobj_persist(g_pmem_pool, buffer, prid->size);

    out.ret = 0;

    margo_free_input(handle, &in);
    margo_respond(handle, &out);
    margo_destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_bulk_persist_ult)

/* service a remote RPC that retrieves the size of a bulk region */
static void bake_bulk_get_size_ult(hg_handle_t handle)
{
    bake_bulk_get_size_out_t out;
    bake_bulk_get_size_in_t in;
    hg_return_t hret;
    pmemobj_region_id_t* prid;

    // printf("Got RPC request to get_size bulk region.\n");
    
    memset(&out, 0, sizeof(out));

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    prid = (pmemobj_region_id_t*)in.rid.data;

    /* kind of cheating here; the size is encoded in the RID */
    out.size = prid->size;
    out.ret = 0;

    margo_free_input(handle, &in);
    margo_respond(handle, &out);
    margo_destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_bulk_get_size_ult)

/* service a remote RPC for a no-op */
static void bake_bulk_noop_ult(hg_handle_t handle)
{
    // printf("Got RPC request to noop bulk region.\n");

    margo_respond(handle, NULL);
    margo_destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_bulk_noop_ult)

/* TODO consolidate with write handler; read and write are nearly identical */
/* service a remote RPC that reads to a bulk region */
static void bake_bulk_read_ult(hg_handle_t handle)
{
    bake_bulk_read_out_t out;
    bake_bulk_read_in_t in;
    hg_return_t hret;
    char* buffer;
    hg_size_t size;
    hg_bulk_t bulk_handle;
    const struct hg_info *hgi;
    margo_instance_id mid;
    pmemobj_region_id_t* prid;

    // printf("Got RPC request to read bulk region.\n");
    
    memset(&out, 0, sizeof(out));

    hgi = margo_get_info(handle);
    assert(hgi);
    mid = margo_hg_info_get_instance(hgi);

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    prid = (pmemobj_region_id_t*)in.rid.data;

    /* find memory address for target object */
    buffer = pmemobj_direct(prid->oid);
    if(!buffer)
    {
        out.ret = -1;
        margo_free_input(handle, &in);
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    size = margo_bulk_get_size(in.bulk_handle);

    /* create bulk handle for local side of transfer */
    hret = margo_bulk_create(mid, 1, (void**)(&buffer), &size, 
        HG_BULK_READ_ONLY, &bulk_handle);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        margo_free_input(handle, &in);
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    hret = margo_bulk_transfer(mid, HG_BULK_PUSH, hgi->addr, in.bulk_handle,
        0, bulk_handle, 0, size);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        margo_bulk_free(bulk_handle);
        margo_free_input(handle, &in);
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    out.ret = 0;

    margo_bulk_free(bulk_handle);
    margo_free_input(handle, &in);
    margo_respond(handle, &out);
    margo_destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_bulk_read_ult)


/* service a remote RPC that reads to a bulk region and eagerly sends
 * response */
static void bake_bulk_eager_read_ult(hg_handle_t handle)
{
    bake_bulk_eager_read_out_t out;
    bake_bulk_eager_read_in_t in;
    hg_return_t hret;
    char* buffer;
    hg_size_t size;
    pmemobj_region_id_t* prid;

    // printf("Got RPC request to read bulk region.\n");
    
    memset(&out, 0, sizeof(out));

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    prid = (pmemobj_region_id_t*)in.rid.data;

    /* find memory address for target object */
    buffer = pmemobj_direct(prid->oid);
    if(!buffer)
    {
        out.ret = -1;
        margo_free_input(handle, &in);
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    out.ret = 0;
    out.buffer = buffer;
    out.size = in.size;

    margo_free_input(handle, &in);
    margo_respond(handle, &out);
    margo_destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_bulk_eager_read_ult)

/* service a remote RPC that probes for a target id */
static void bake_bulk_probe_ult(hg_handle_t handle)
{
    bake_bulk_probe_out_t out;

    // printf("Got RPC request to probe bulk region.\n");
    
    memset(&out, 0, sizeof(out));

    out.ret = 0;
    out.bti = g_pmem_root->target_id;

    margo_respond(handle, &out);
    margo_destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_bulk_probe_ult)


