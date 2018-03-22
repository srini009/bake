/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include "bake-config.h"

#include <assert.h>
#include <libpmemobj.h>
#include <bake-server.h>
#include "uthash.h"
#include "bake-rpc.h"

/* definition of BAKE root data structure (just a uuid for now) */
typedef struct
{   
    bake_target_id_t pool_id;
} bake_root_t;

/* definition of internal BAKE region_id_t identifier for libpmemobj back end */
typedef struct
{
    PMEMoid oid;
    uint64_t size;
} pmemobj_region_id_t;

typedef struct
{
    PMEMobjpool* pmem_pool;
    bake_root_t* pmem_root;
    bake_target_id_t target_id;
    UT_hash_handle hh;
} bake_pmem_entry_t;

typedef struct bake_server_context_t
{
    uint64_t num_targets;
    bake_pmem_entry_t* targets;
} bake_server_context_t;

static void bake_server_finalize_cb(void *data);

int bake_makepool(
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
    uuid_generate(root->pool_id.id);
    pmemobj_persist(pool, root, sizeof(bake_root_t));
#if 0
    char target_string[64];
    uuid_unparse(root->id, target_string);
    fprintf(stderr, "created BAKE target ID: %s\n", target_string);
#endif

    pmemobj_close(pool);

    return(0);
}

int bake_provider_register(
        margo_instance_id mid,
        uint8_t mplex_id,
        ABT_pool abt_pool,
        bake_provider_t* provider)
{
    bake_server_context_t *tmp_svr_ctx;

    /* check if a provider with the same multiplex id already exists */
    {
        hg_id_t id;
        hg_bool_t flag;
        margo_registered_name_mplex(mid, "bake_probe_rpc", mplex_id, &id, &flag);
        if(flag == HG_TRUE) {
            fprintf(stderr, "bake_provider_register(): a provider with the same mplex id (%d) already exists\n", mplex_id);
            return -1;
        }
    }

    /* allocate the resulting structure */    
    tmp_svr_ctx = calloc(1,sizeof(*tmp_svr_ctx));
    if(!tmp_svr_ctx)
        return(-1);

    /* register RPCs */
    hg_id_t rpc_id;
    rpc_id = MARGO_REGISTER_MPLEX(mid, "bake_create_rpc",
            bake_create_in_t, bake_create_out_t, 
            bake_create_ult, mplex_id, abt_pool);
    margo_register_data_mplex(mid, rpc_id, mplex_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_MPLEX(mid, "bake_write_rpc",
            bake_write_in_t, bake_write_out_t, 
            bake_write_ult, mplex_id, abt_pool);
    margo_register_data_mplex(mid, rpc_id, mplex_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_MPLEX(mid, "bake_eager_write_rpc",
            bake_eager_write_in_t, bake_eager_write_out_t, 
            bake_eager_write_ult, mplex_id, abt_pool);
    margo_register_data_mplex(mid, rpc_id, mplex_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_MPLEX(mid, "bake_eager_read_rpc",
            bake_eager_read_in_t, bake_eager_read_out_t, 
            bake_eager_read_ult, mplex_id, abt_pool);
    margo_register_data_mplex(mid, rpc_id, mplex_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_MPLEX(mid, "bake_persist_rpc",
            bake_persist_in_t, bake_persist_out_t, 
            bake_persist_ult, mplex_id, abt_pool);
    margo_register_data_mplex(mid, rpc_id, mplex_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_MPLEX(mid, "bake_create_write_persist_rpc",
            bake_create_write_persist_in_t, bake_create_write_persist_out_t,
            bake_create_write_persist_ult, mplex_id, abt_pool);
    margo_register_data_mplex(mid, rpc_id, mplex_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_MPLEX(mid, "bake_get_size_rpc",
            bake_get_size_in_t, bake_get_size_out_t, 
            bake_get_size_ult, mplex_id, abt_pool);
    margo_register_data_mplex(mid, rpc_id, mplex_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_MPLEX(mid, "bake_read_rpc",
            bake_read_in_t, bake_read_out_t, 
            bake_read_ult, mplex_id, abt_pool);
    margo_register_data_mplex(mid, rpc_id, mplex_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_MPLEX(mid, "bake_probe_rpc",
            bake_probe_in_t, bake_probe_out_t, bake_probe_ult, 
            mplex_id, abt_pool);
    margo_register_data_mplex(mid, rpc_id, mplex_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_MPLEX(mid, "bake_noop_rpc",
            void, void, bake_noop_ult, mplex_id, abt_pool);
    margo_register_data_mplex(mid, rpc_id, mplex_id, (void*)tmp_svr_ctx, NULL);

    /* install the bake server finalize callback */
    margo_push_finalize_callback(mid, &bake_server_finalize_cb, tmp_svr_ctx);

    if(provider != BAKE_PROVIDER_IGNORE)
        *provider = tmp_svr_ctx;

    return(0);
}

int bake_provider_add_storage_target(
        bake_provider_t provider,
        const char *target_name,
        bake_target_id_t* target_id)
{
    bake_pmem_entry_t* new_entry = calloc(1, sizeof(*new_entry));

    new_entry->pmem_pool = pmemobj_open(target_name, NULL);
    if(!(new_entry->pmem_pool)) {
        fprintf(stderr, "pmemobj_open: %s\n", pmemobj_errormsg());
        free(new_entry);
        return -1;
    }

    /* check to make sure the root is properly set */
    PMEMoid root_oid = pmemobj_root(new_entry->pmem_pool, sizeof(bake_root_t));
    new_entry->pmem_root = pmemobj_direct(root_oid);
    bake_target_id_t key = new_entry->pmem_root->pool_id;
    new_entry->target_id = key;

    if(uuid_is_null(key.id))
    {
        fprintf(stderr, "Error: BAKE pool %s is not properly initialized\n", target_name);
        pmemobj_close(new_entry->pmem_pool);
        free(new_entry);
        return(-1);
    }

    /* insert in the provider's hash */
    HASH_ADD(hh, provider->targets, target_id, sizeof(bake_target_id_t), new_entry);
    /* check that it was inserted */
    bake_pmem_entry_t* check_entry = NULL;
    HASH_FIND(hh, provider->targets, &key, sizeof(bake_target_id_t), check_entry);
    if(check_entry != new_entry) {
        fprintf(stderr, "Error: BAKE could not insert new pmem pool into the hash\n");
        pmemobj_close(new_entry->pmem_pool);
        free(new_entry);
        return -1;
    }

    provider->num_targets += 1;
    *target_id = key;
    return 0;
}

static bake_pmem_entry_t* find_pmem_entry(
            bake_provider_t provider,
            bake_target_id_t target_id)
{
    bake_pmem_entry_t* entry = NULL;
    HASH_FIND(hh, provider->targets, &target_id, sizeof(bake_target_id_t), entry);
    return entry;
}

int bake_provider_remove_storage_target(
        bake_provider_t provider,
        bake_target_id_t target_id)
{
    bake_pmem_entry_t* entry = NULL;
    HASH_FIND(hh, provider->targets, &target_id, sizeof(bake_target_id_t), entry);
    if(!entry) return -1;
    pmemobj_close(entry->pmem_pool);
    HASH_DEL(provider->targets, entry);
    free(entry);
    return 0;
}

int bake_provider_remove_all_storage_targets(
        bake_provider_t provider)
{
    bake_pmem_entry_t *p, *tmp;
    HASH_ITER(hh, provider->targets, p, tmp) {
        HASH_DEL(provider->targets, p);
        pmemobj_close(p->pmem_pool);
        free(p);
    }
    provider->num_targets = 0;
    return 0;
}

int bake_provider_count_storage_targets(
        bake_provider_t provider,
        uint64_t* num_targets)
{
    *num_targets = provider->num_targets;
    return 0;
}

int bake_provider_list_storage_targets(
        bake_provider_t provider,
        bake_target_id_t* targets)
{
    bake_pmem_entry_t *p, *tmp;
    uint64_t i = 0;
    HASH_ITER(hh, provider->targets, p, tmp) {
        targets[i] = p->target_id;
        i += 1;
    }
    return 0;
}

/* service a remote RPC that creates a BAKE region */
static void bake_create_ult(hg_handle_t handle)
{
    bake_create_out_t out;
    bake_create_in_t in;
    hg_return_t hret;
    pmemobj_region_id_t* prid;

    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    assert(mid);
    const struct hg_info* info = margo_get_info(handle);
    bake_provider_t svr_ctx = 
        margo_registered_data_mplex(mid, info->id, info->target_id);
    if(!svr_ctx) {
        fprintf(stderr, "Error: BAKE create could not find provider\n"); 
        goto respond_with_error;
    }

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS)
        goto respond_with_error;

    /* find the pmem pool */
    bake_pmem_entry_t* entry = find_pmem_entry(svr_ctx, in.bti);
    if(entry == NULL) {
        fprintf(stderr, "Error: BAKE create could not find storage target\n");
        goto respond_with_error;
    }

    /* TODO: this check needs to be somewhere else */
    assert(sizeof(pmemobj_region_id_t) <= BAKE_REGION_ID_DATA_SIZE);

    memset(&out, 0, sizeof(out));

    prid = (pmemobj_region_id_t*)out.rid.data;
    prid->size = in.region_size;
    /* find the pmem pool */
    out.ret = pmemobj_alloc(entry->pmem_pool, &prid->oid,
            in.region_size, 0, NULL, NULL);

    margo_free_input(handle, &in);
    margo_respond(handle, &out);

finish:
    margo_destroy(handle);
    return;

respond_with_error:
    out.ret = -1;
    margo_respond(handle, &out);
    goto finish;
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

    memset(&out, 0, sizeof(out));

    mid = margo_hg_handle_get_instance(handle);
    assert(mid);
    hgi = margo_get_info(handle);
    bake_provider_t svr_ctx = 
        margo_registered_data_mplex(mid, hgi->id, hgi->target_id);
    if(!svr_ctx) {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

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

    memset(&out, 0, sizeof(out));

    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    assert(mid);
    const struct hg_info* info = margo_get_info(handle);
    bake_provider_t svr_ctx = 
        margo_registered_data_mplex(mid, info->id, info->target_id);
    if(!svr_ctx) {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

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

    memset(&out, 0, sizeof(out));

    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    assert(mid);
    const struct hg_info* info = margo_get_info(handle);
    bake_provider_t svr_ctx = 
        margo_registered_data_mplex(mid, info->id, info->target_id);
    if(!svr_ctx) {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

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
    PMEMobjpool* pmem_pool = pmemobj_pool_by_oid(prid->oid);
    pmemobj_persist(pmem_pool, buffer, prid->size);

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

    memset(&out, 0, sizeof(out));

    mid = margo_hg_handle_get_instance(handle);
    assert(mid);
    hgi = margo_get_info(handle);
    bake_provider_t svr_ctx = 
        margo_registered_data_mplex(mid, hgi->id, hgi->target_id);
    if(!svr_ctx) {
        fprintf(stderr, "Error: BAKE create_write_persist could not find provider\n");
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    /* TODO: this check needs to be somewhere else */
    assert(sizeof(pmemobj_region_id_t) <= BAKE_REGION_ID_DATA_SIZE);

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS)
    {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    /* find the pmem pool */
    bake_pmem_entry_t* entry = find_pmem_entry(svr_ctx, in.bti);
    if(entry == NULL) {
        fprintf(stderr, "Error: BAKE create_write_persist could not find storage target\n");
        out.ret = -1;
        margo_free_input(handle, &in);
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    prid = (pmemobj_region_id_t*)out.rid.data;
    prid->size = in.region_size;
    ret = pmemobj_alloc(entry->pmem_pool, &prid->oid,
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
    pmemobj_persist(entry->pmem_pool, buffer, prid->size);

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

    memset(&out, 0, sizeof(out));

    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    assert(mid);
    const struct hg_info* hgi = margo_get_info(handle);
    bake_provider_t svr_ctx = 
        margo_registered_data_mplex(mid, hgi->id, hgi->target_id);
    if(!svr_ctx) {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

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
    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    assert(mid);
    const struct hg_info* hgi = margo_get_info(handle);
    bake_provider_t svr_ctx = 
        margo_registered_data_mplex(mid, hgi->id, hgi->target_id);

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

    memset(&out, 0, sizeof(out));

    mid = margo_hg_handle_get_instance(handle);
    assert(mid);
    hgi = margo_get_info(handle);
    bake_provider_t svr_ctx = 
        margo_registered_data_mplex(mid, hgi->id, hgi->target_id);
    if(!svr_ctx) {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

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

    memset(&out, 0, sizeof(out));

    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    assert(mid);
    const struct hg_info* hgi = margo_get_info(handle);
    bake_provider_t svr_ctx = 
        margo_registered_data_mplex(mid, hgi->id, hgi->target_id);
    if(!svr_ctx) {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

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

    memset(&out, 0, sizeof(out));

    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    assert(mid);
    const struct hg_info* hgi = margo_get_info(handle);
    bake_provider_t svr_ctx = 
        margo_registered_data_mplex(mid, hgi->id, hgi->target_id);
    if(!svr_ctx) {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    out.ret = 0;
    // XXX this is where we should handle multiple targets
    uint64_t targets_count;
    bake_provider_count_storage_targets(svr_ctx, &targets_count);
    bake_target_id_t targets[targets_count];
    bake_provider_list_storage_targets(svr_ctx, targets);

    out.targets = targets;
    out.num_targets = targets_count;

    margo_respond(handle, &out);
    margo_destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_probe_ult)

static void bake_server_finalize_cb(void *data)
{
    bake_server_context_t *svr_ctx = (bake_server_context_t *)data;
    assert(svr_ctx);

    bake_provider_remove_all_storage_targets(svr_ctx);

    free(svr_ctx);

    return;
}

