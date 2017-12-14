/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include "bake-config.h"

#include <assert.h>
#include <libpmemobj.h>
#include <bake-server.h>
#include "bake-rpc.h"

/* definition of BAKE root data structure (just a target_id for now) */
typedef struct
{   
    bake_target_id_t target_id;
} bake_root_t;
 
/* definition of internal BAKE region_id_t identifier for libpmemobj back end */
typedef struct
{
    PMEMoid oid;
    uint64_t size;
} pmemobj_region_id_t;

typedef struct
{
    PMEMobjpool *pmem_pool;
    bake_root_t *pmem_root;

    /* server shutdown conditional logic */
    ABT_mutex shutdown_mutex;
    ABT_cond shutdown_cond;
    int shutdown_flag;
    int ref_count;
} bake_server_context_t;

static void bake_server_cleanup(bake_server_context_t *svr_ctx);

/* TODO: this should not be global in the long run; server may provide access
 * to multiple targets
 */
static bake_server_context_t *g_svr_ctx = NULL;

int bake_server_makepool(
	const char *pool_name,
    size_t pool_size,
    mode_t pool_mode)
{
    PMEMobjpool *pool;
    PMEMoid root_oid;
    bake_root_t *root;

    pool = pmemobj_create(pool_name, NULL, pool_size, pool_mode);
    if(!pool)
    {
        fprintf(stderr, "pmemobj_create: %s\n", pmemobj_errormsg());
        return(-1);
    }

    /* find root */
    root_oid = pmemobj_root(pool, sizeof(bake_root_t));
    root = pmemobj_direct(root_oid);

    /* store the target id for this bake pool at the root */
    uuid_generate(root->target_id.id);
    pmemobj_persist(pool, root, sizeof(bake_root_t));
#if 0
    char target_string[64];
    uuid_unparse(root->target_id.id, target_string);
    fprintf(stderr, "created BAKE target ID: %s\n", target_string);
#endif

    pmemobj_close(pool);

    return(0);
}

int bake_server_init(
    margo_instance_id mid,
    const char *pool_name)
{
    PMEMobjpool *pool;
    PMEMoid root_oid;
    bake_root_t *root;
    bake_server_context_t *tmp_svr_ctx;
    
    /* make sure to initialize the server only once */
    if(g_svr_ctx)
    {
        fprintf(stderr, "Error: BAKE server already initialized\n");
        return(-1);
    }

    tmp_svr_ctx = malloc(sizeof(*tmp_svr_ctx));
    if(!tmp_svr_ctx)
        return(-1);
    memset(tmp_svr_ctx, 0, sizeof(*tmp_svr_ctx));

    /* open the given pmem pool */
    pool = pmemobj_open(pool_name, NULL);
    if(!pool)
    {
        fprintf(stderr, "pmemobj_open: %s\n", pmemobj_errormsg());
        return(-1);
    }

    /* check to make sure the root is properly set */
    root_oid = pmemobj_root(pool, sizeof(bake_root_t));
    root = pmemobj_direct(root_oid);
    if(uuid_is_null(root->target_id.id))
    {
        fprintf(stderr, "Error: BAKE pool is not properly initialized\n");
        pmemobj_close(pool);
        return(-1);
    }
#if 0
    char target_string[64];
    uuid_unparse(root->target_id.id, target_string);
    fprintf(stderr, "opened BAKE target ID: %s\n", target_string);
#endif

    /* register RPCs */
    MARGO_REGISTER(mid, "bake_shutdown_rpc",
        void, void, bake_shutdown_ult);
    MARGO_REGISTER(mid, "bake_create_rpc",
        bake_create_in_t, bake_create_out_t, bake_create_ult);
    MARGO_REGISTER(mid, "bake_write_rpc",
        bake_write_in_t, bake_write_out_t, bake_write_ult);
    MARGO_REGISTER(mid, "bake_eager_write_rpc",
        bake_eager_write_in_t, bake_eager_write_out_t, bake_eager_write_ult);
    MARGO_REGISTER(mid, "bake_eager_read_rpc",
        bake_eager_read_in_t, bake_eager_read_out_t, bake_eager_read_ult);
    MARGO_REGISTER(mid, "bake_persist_rpc",
        bake_persist_in_t, bake_persist_out_t, bake_persist_ult);
    MARGO_REGISTER(mid, "bake_create_write_persist_rpc",
        bake_create_write_persist_in_t, bake_create_write_persist_out_t,
        bake_create_write_persist_ult);
    MARGO_REGISTER(mid, "bake_get_size_rpc",
        bake_get_size_in_t, bake_get_size_out_t, bake_get_size_ult);
    MARGO_REGISTER(mid, "bake_read_rpc",
        bake_read_in_t, bake_read_out_t, bake_read_ult);
    MARGO_REGISTER(mid, "bake_probe_rpc",
        void, bake_probe_out_t, bake_probe_ult);
    MARGO_REGISTER(mid, "bake_noop_rpc",
        void, void, bake_noop_ult);

    /* set global server context */
    tmp_svr_ctx->pmem_pool = pool;
    tmp_svr_ctx->pmem_root = root;
    tmp_svr_ctx->ref_count = 1;
    ABT_mutex_create(&tmp_svr_ctx->shutdown_mutex);
    ABT_cond_create(&tmp_svr_ctx->shutdown_cond);
    g_svr_ctx = tmp_svr_ctx;

    return(0);
}

void bake_server_shutdown()
{
    bake_server_context_t *svr_ctx = g_svr_ctx;
    int do_cleanup;

    assert(svr_ctx);

    ABT_mutex_lock(svr_ctx->shutdown_mutex);
    svr_ctx->shutdown_flag = 1;
    ABT_cond_broadcast(svr_ctx->shutdown_cond);

    svr_ctx->ref_count--;
    do_cleanup = svr_ctx->ref_count == 0;

    ABT_mutex_unlock(svr_ctx->shutdown_mutex);

    if (do_cleanup)
    {
        bake_server_cleanup(svr_ctx);
        g_svr_ctx = NULL;
    }

    return;
}

void bake_server_wait_for_shutdown()
{
    bake_server_context_t *svr_ctx = g_svr_ctx;
    int do_cleanup;

    assert(svr_ctx);

    ABT_mutex_lock(svr_ctx->shutdown_mutex);

    svr_ctx->ref_count++;
    while(!svr_ctx->shutdown_flag)
        ABT_cond_wait(svr_ctx->shutdown_cond, svr_ctx->shutdown_mutex);
    svr_ctx->ref_count--;
    do_cleanup = svr_ctx->ref_count == 0;

    ABT_mutex_unlock(svr_ctx->shutdown_mutex);

    if (do_cleanup)
    {
        bake_server_cleanup(svr_ctx);
        g_svr_ctx = NULL;
    }

    return;
}

/* service a remote RPC that instructs the BAKE server to shut down */
static void bake_shutdown_ult(hg_handle_t handle)
{
    hg_return_t hret;
    margo_instance_id mid;

    assert(g_svr_ctx);

    mid = margo_hg_handle_get_instance(handle);

    hret = margo_respond(handle, NULL);
    assert(hret == HG_SUCCESS);

    margo_destroy(handle);

    /* NOTE: we assume that the server daemon is using
     * bake_server_wait_for_shutdown() to suspend until this RPC executes, so
     * there is no need to send any extra signal to notify it.
     */
    bake_server_shutdown();

    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_shutdown_ult)

/* service a remote RPC that creates a BAKE region */
static void bake_create_ult(hg_handle_t handle)
{
    bake_create_out_t out;
    bake_create_in_t in;
    hg_return_t hret;
    pmemobj_region_id_t* prid;

    assert(g_svr_ctx);

    /* TODO: this check needs to be somewhere else */
    assert(sizeof(pmemobj_region_id_t) <= BAKE_REGION_ID_DATA_SIZE);
    
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
    out.ret = pmemobj_alloc(g_svr_ctx->pmem_pool, &prid->oid,
        in.region_size, 0, NULL, NULL);

    margo_free_input(handle, &in);
    margo_respond(handle, &out);
    margo_destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_create_ult)

/* service a remote RPC that writes to a BAKE region */
static void bake_write_ult(hg_handle_t handle)
{
    bake_write_out_t out;
    bake_write_in_t in;
    hg_return_t hret;
    hg_addr_t src_addr;
    char* buffer;
    hg_bulk_t bulk_handle;
    const struct hg_info *hgi;
    margo_instance_id mid;
    pmemobj_region_id_t* prid;

    assert(g_svr_ctx);

    memset(&out, 0, sizeof(out));

    hgi = margo_get_info(handle);
    assert(hgi);
    mid = margo_hg_info_get_instance(hgi);
    assert(mid != MARGO_INSTANCE_NULL);

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

    /* create bulk handle for local side of transfer */
    hret = margo_bulk_create(mid, 1, (void**)(&buffer), &in.bulk_size, 
        HG_BULK_WRITE_ONLY, &bulk_handle);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        margo_free_input(handle, &in);
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    if(in.remote_addr_str)
    {
        /* a proxy address was provided to pull write data from */
        hret = margo_addr_lookup(mid, in.remote_addr_str, &src_addr);
        if(hret != HG_SUCCESS)
        {
            out.ret = -1;
            margo_bulk_free(bulk_handle);
            margo_free_input(handle, &in);
            margo_respond(handle, &out);
            margo_destroy(handle);
            return;
        }
    }
    else
    {
        /* no proxy write, use the source of this request */
        src_addr = hgi->addr;
    }

    hret = margo_bulk_transfer(mid, HG_BULK_PULL, src_addr, in.bulk_handle,
        in.bulk_offset, bulk_handle, 0, in.bulk_size);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        if(in.remote_addr_str)
            margo_addr_free(mid, src_addr);
        margo_bulk_free(bulk_handle);
        margo_free_input(handle, &in);
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    out.ret = 0;

    if(in.remote_addr_str)
        margo_addr_free(mid, src_addr);
    margo_bulk_free(bulk_handle);
    margo_free_input(handle, &in);
    margo_respond(handle, &out);
    margo_destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_write_ult)

/* service a remote RPC that writes to a BAKE region in eager mode */
static void bake_eager_write_ult(hg_handle_t handle)
{
    bake_eager_write_out_t out;
    bake_eager_write_in_t in;
    hg_return_t hret;
    char* buffer;
    hg_bulk_t bulk_handle;
    pmemobj_region_id_t* prid;

    assert(g_svr_ctx);

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
DEFINE_MARGO_RPC_HANDLER(bake_eager_write_ult)

/* service a remote RPC that persists to a BAKE region */
static void bake_persist_ult(hg_handle_t handle)
{
    bake_persist_out_t out;
    bake_persist_in_t in;
    hg_return_t hret;
    char* buffer;
    pmemobj_region_id_t* prid;

    assert(g_svr_ctx);
    
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
    pmemobj_persist(g_svr_ctx->pmem_pool, buffer, prid->size);

    out.ret = 0;

    margo_free_input(handle, &in);
    margo_respond(handle, &out);
    margo_destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_persist_ult)

static void bake_create_write_persist_ult(hg_handle_t handle)
{
    bake_create_write_persist_out_t out;
    bake_create_write_persist_in_t in;
    hg_addr_t src_addr;
    char* buffer;
    hg_bulk_t bulk_handle;
    const struct hg_info *hgi;
    margo_instance_id mid;
    hg_return_t hret;
    int ret;
    pmemobj_region_id_t* prid;

    assert(g_svr_ctx);

    /* TODO: this check needs to be somewhere else */
    assert(sizeof(pmemobj_region_id_t) <= BAKE_REGION_ID_DATA_SIZE);

    memset(&out, 0, sizeof(out));

    hgi = margo_get_info(handle);
    assert(hgi);
    mid = margo_hg_info_get_instance(hgi);
    assert(mid != MARGO_INSTANCE_NULL);

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
    ret = pmemobj_alloc(g_svr_ctx->pmem_pool, &prid->oid,
        in.region_size, 0, NULL, NULL);
    if(ret != 0)
    {
        out.ret = -1;
        margo_free_input(handle, &in);
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

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

    /* create bulk handle for local side of transfer */
    hret = margo_bulk_create(mid, 1, (void**)(&buffer), &in.bulk_size,
        HG_BULK_WRITE_ONLY, &bulk_handle);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        margo_free_input(handle, &in);
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    if(in.remote_addr_str)
    {
        /* a proxy address was provided to pull write data from */
        hret = margo_addr_lookup(mid, in.remote_addr_str, &src_addr);
        if(hret != HG_SUCCESS)
        {
            out.ret = -1;
            margo_bulk_free(bulk_handle);
            margo_free_input(handle, &in);
            margo_respond(handle, &out);
            margo_destroy(handle);
            return;
        }
    }
    else
    {
        /* no proxy write, use the source of this request */
        src_addr = hgi->addr;
    }

    hret = margo_bulk_transfer(mid, HG_BULK_PULL, src_addr, in.bulk_handle,
        in.bulk_offset, bulk_handle, 0, in.bulk_size);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        if(in.remote_addr_str)
            margo_addr_free(mid, src_addr);
        margo_bulk_free(bulk_handle);
        margo_free_input(handle, &in);
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    /* TODO: should this have an abt shim in case it blocks? */
    pmemobj_persist(g_svr_ctx->pmem_pool, buffer, prid->size);

    out.ret = 0;

    if(in.remote_addr_str)
        margo_addr_free(mid, src_addr);
    margo_bulk_free(bulk_handle);
    margo_free_input(handle, &in);
    margo_respond(handle, &out);
    margo_destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_create_write_persist_ult)

/* service a remote RPC that retrieves the size of a BAKE region */
static void bake_get_size_ult(hg_handle_t handle)
{
    bake_get_size_out_t out;
    bake_get_size_in_t in;
    hg_return_t hret;
    pmemobj_region_id_t* prid;

    assert(g_svr_ctx);
    
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
DEFINE_MARGO_RPC_HANDLER(bake_get_size_ult)

/* service a remote RPC for a BAKE no-op */
static void bake_noop_ult(hg_handle_t handle)
{
    assert(g_svr_ctx);
    
    margo_respond(handle, NULL);
    margo_destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_noop_ult)

/* TODO consolidate with write handler; read and write are nearly identical */
/* service a remote RPC that reads from a BAKE region */
static void bake_read_ult(hg_handle_t handle)
{
    bake_read_out_t out;
    bake_read_in_t in;
    hg_return_t hret;
    hg_addr_t src_addr;
    char* buffer;
    hg_bulk_t bulk_handle;
    const struct hg_info *hgi;
    margo_instance_id mid;
    pmemobj_region_id_t* prid;

    assert(g_svr_ctx);
    
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

    /* create bulk handle for local side of transfer */
    hret = margo_bulk_create(mid, 1, (void**)(&buffer), &in.bulk_size, 
        HG_BULK_READ_ONLY, &bulk_handle);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        margo_free_input(handle, &in);
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    if(in.remote_addr_str)
    {
        /* a proxy address was provided to push read data to */
        hret = margo_addr_lookup(mid, in.remote_addr_str, &src_addr);
        if(hret != HG_SUCCESS)
        {
            out.ret = -1;
            margo_bulk_free(bulk_handle);
            margo_free_input(handle, &in);
            margo_respond(handle, &out);
            margo_destroy(handle);
            return;
        }
    }
    else
    {
        /* no proxy write, use the source of this request */
        src_addr = hgi->addr;
    }

    hret = margo_bulk_transfer(mid, HG_BULK_PUSH, src_addr, in.bulk_handle,
        in.bulk_offset, bulk_handle, 0, in.bulk_size);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        if(in.remote_addr_str)
            margo_addr_free(mid, src_addr);
        margo_bulk_free(bulk_handle);
        margo_free_input(handle, &in);
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    out.ret = 0;

    if(in.remote_addr_str)
        margo_addr_free(mid, src_addr);
    margo_bulk_free(bulk_handle);
    margo_free_input(handle, &in);
    margo_respond(handle, &out);
    margo_destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_read_ult)

/* service a remote RPC that reads from a BAKE region and eagerly sends
 * response */
static void bake_eager_read_ult(hg_handle_t handle)
{
    bake_eager_read_out_t out;
    bake_eager_read_in_t in;
    hg_return_t hret;
    char* buffer;
    hg_size_t size;
    pmemobj_region_id_t* prid;

    assert(g_svr_ctx);
    
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
DEFINE_MARGO_RPC_HANDLER(bake_eager_read_ult)

/* service a remote RPC that probes for a BAKE target id */
static void bake_probe_ult(hg_handle_t handle)
{
    bake_probe_out_t out;

    assert(g_svr_ctx);
    
    memset(&out, 0, sizeof(out));

    out.ret = 0;
    out.bti = g_svr_ctx->pmem_root->target_id;

    margo_respond(handle, &out);
    margo_destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_probe_ult)

static void bake_server_cleanup(bake_server_context_t *svr_ctx)
{
    pmemobj_close(svr_ctx->pmem_pool);
    ABT_mutex_free(&svr_ctx->shutdown_mutex);
    ABT_cond_free(&svr_ctx->shutdown_cond);
    free(svr_ctx);

    return;
}

