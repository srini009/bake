/*
 * (C) 2016 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include "bake-config.h"

#include <assert.h>
#include <margo.h>
#include <bake-client.h>
#include "uthash.h"
#include "bake-rpc.h"

#define BAKE_EAGER_LIMIT 2048

/* Refers to a single Margo initialization, for now this is shared by
 * all remote BAKE targets.  In the future we probably need to support
 * multiple in case we run atop more than one transport at a time.
 */
struct bake_client
{
    margo_instance_id mid;  

    hg_id_t bake_probe_id;
    hg_id_t bake_shutdown_id; 
    hg_id_t bake_create_id;
    hg_id_t bake_eager_write_id;
    hg_id_t bake_eager_read_id;
    hg_id_t bake_write_id;
    hg_id_t bake_persist_id;
    hg_id_t bake_create_write_persist_id;
    hg_id_t bake_get_size_id;
    hg_id_t bake_read_id;
    hg_id_t bake_noop_id;
};

struct bake_provider_handle {
    struct bake_client* client;
    hg_addr_t           addr;
    uint8_t             mplex_id;
    uint64_t            refcount;
};

static int bake_client_register(bake_client_t client, margo_instance_id mid)
{
    client->mid = mid;

    /* register RPCs */
    client->bake_probe_id = 
        MARGO_REGISTER(mid, "bake_probe_rpc",
        void, bake_probe_out_t, NULL);
    client->bake_shutdown_id = 
        MARGO_REGISTER(mid, "bake_shutdown_rpc",
        void, void, NULL);
    client->bake_create_id = 
        MARGO_REGISTER(mid, "bake_create_rpc",
        bake_create_in_t, bake_create_out_t, NULL);
    client->bake_write_id = 
        MARGO_REGISTER(mid, "bake_write_rpc",
        bake_write_in_t, bake_write_out_t, NULL);
    client->bake_eager_write_id = 
        MARGO_REGISTER(mid, "bake_eager_write_rpc",
        bake_eager_write_in_t, bake_eager_write_out_t, NULL);
    client->bake_eager_read_id = 
        MARGO_REGISTER(mid, "bake_eager_read_rpc",
        bake_eager_read_in_t, bake_eager_read_out_t, NULL);
    client->bake_persist_id = 
        MARGO_REGISTER(mid, "bake_persist_rpc",
        bake_persist_in_t, bake_persist_out_t, NULL);
    client->bake_create_write_persist_id =
        MARGO_REGISTER(mid, "bake_create_write_persist_rpc",
         bake_create_write_persist_in_t, bake_create_write_persist_out_t, NULL);
    client->bake_get_size_id = 
        MARGO_REGISTER(mid, "bake_get_size_rpc",
        bake_get_size_in_t, bake_get_size_out_t, NULL);
    client->bake_read_id = 
        MARGO_REGISTER(mid, "bake_read_rpc",
        bake_read_in_t, bake_read_out_t, NULL);
    client->bake_noop_id = 
        MARGO_REGISTER(mid, "bake_noop_rpc",
        void, void, NULL);

    return(0);
}

int bake_client_init(margo_instance_id mid, bake_client_t* client)
{
    bake_client_t c = (bake_client_t)calloc(1, sizeof(*c));
    if(!c) return -1;

    int ret = bake_client_register(c, mid);
    if(ret != 0) return ret;

    *client = c;
    return 0;
}

int bake_client_finalize(bake_client_t client)
{
    free(client);
    return 0;
}

int bake_probe(
    bake_provider_handle_t provider,
    uint64_t max_targets,
    bake_target_id_t *bti,
    uint64_t* num_targets)
{
    hg_return_t hret;
    int ret;
    bake_probe_out_t out;
    hg_handle_t handle;

    /* create handle */
    hret = margo_create(
                provider->client->mid, 
                provider->addr, 
                provider->client->bake_probe_id, 
                &handle);

    if(hret != HG_SUCCESS) return -1;

    hret = margo_set_target_id(handle, provider->mplex_id);
    
    if(hret != HG_SUCCESS) {
        margo_destroy(handle);
        return -1;
    }

    hret = margo_forward(handle, NULL);
    if(hret != HG_SUCCESS) {
        margo_destroy(handle);
        return -1;
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS) {
        margo_destroy(handle);
        return -1;
    }

    ret = out.ret;

    margo_free_output(handle, &out);
    margo_destroy(handle);

    if(ret == HG_SUCCESS) {
        *bti = out.bti;
    }

    return ret;
}

int bake_provider_handle_create(
        bake_client_t client,
        hg_addr_t addr,
        uint8_t mplex_id,
        bake_provider_handle_t* handle)
{
    if(client == BAKE_CLIENT_NULL) return -1;

    bake_provider_handle_t provider = 
        (bake_provider_handle_t)calloc(1, sizeof(*provider));

    if(!provider) return -1;

    hg_return_t ret = margo_addr_dup(client->mid, addr, &(provider->addr));
    if(ret != HG_SUCCESS) {
        free(provider);
        return -1;
    }
    
    provider->client   = client;
    provider->mplex_id = mplex_id;
    provider->refcount = 1;

    *handle = provider;
    return 0;
}

int bake_provider_handle_ref_incr(bake_provider_handle_t handle)
{
    if(handle == BAKE_PROVIDER_HANDLE_NULL) return -1;
    handle->refcount += 1;
    return 0;
}

int bake_provider_handle_release(bake_provider_handle_t handle)
{
    if(handle == BAKE_PROVIDER_HANDLE_NULL) return -1;
    handle->refcount -= 1;
    if(handle->refcount == 0) {
        margo_addr_free(handle->client->mid, handle->addr);
        free(handle);
    }
    return 0;
}
  
int bake_shutdown_service(bake_client_t client, hg_addr_t addr)
{
    hg_return_t hret;
    hg_handle_t handle;

    hret = margo_create(client->mid, addr, 
            client->bake_shutdown_id, &handle);

    if(hret != HG_SUCCESS)
        return(-1);

    hret = margo_forward(handle, NULL);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        return(-1);
    }

    margo_destroy(handle);
    return(0);
}

static int bake_eager_write(
    bake_provider_handle_t provider,
    bake_region_id_t rid,
    uint64_t region_offset,
    void const *buf,
    uint64_t buf_size)
{
    hg_return_t hret;
    hg_handle_t handle;
    bake_eager_write_in_t in;
    bake_eager_write_out_t out;
    int ret;

    in.rid = rid;
    in.region_offset = region_offset;
    in.size = buf_size;
    in.buffer = (char*)buf;
  
    hret = margo_create(provider->client->mid, provider->addr, 
                provider->client->bake_eager_write_id, &handle);
    margo_set_target_id(handle, provider->mplex_id);

    if(hret != HG_SUCCESS)
        return(-1);

    hret = margo_forward(handle, &in);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        return(-1);
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        return(-1);
    }
    
    ret = out.ret;

    margo_free_output(handle, &out);
    margo_destroy(handle);

    return(ret);
}

int bake_write(
    bake_provider_handle_t provider,
    bake_region_id_t rid,
    uint64_t region_offset,
    void const *buf,
    uint64_t buf_size)
{
    hg_return_t hret;
    hg_handle_t handle;
    bake_write_in_t in;
    bake_write_out_t out;
    int ret;

    if(buf_size <= BAKE_EAGER_LIMIT)
        return(bake_eager_write(provider, rid, region_offset, buf, buf_size));

    in.rid = rid;
    in.region_offset = region_offset;
    in.bulk_offset = 0;
    in.bulk_size = buf_size;
    in.remote_addr_str = NULL; /* set remote_addr to NULL to disable proxy write */

    hret = margo_bulk_create(provider->client->mid, 1, (void**)(&buf), &buf_size, 
        HG_BULK_READ_ONLY, &in.bulk_handle);
    if(hret != HG_SUCCESS)
        return(-1);
   
    hret = margo_create(provider->client->mid, provider->addr, 
        provider->client->bake_write_id, &handle);
    margo_set_target_id(handle, provider->mplex_id);

    if(hret != HG_SUCCESS)
    {
        margo_bulk_free(in.bulk_handle);
        return(-1);
    }

    hret = margo_forward(handle, &in);
    if(hret != HG_SUCCESS)
    {
        margo_bulk_free(in.bulk_handle);
        margo_destroy(handle);
        return(-1);
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS)
    {
        margo_bulk_free(in.bulk_handle);
        margo_destroy(handle);
        return(-1);
    }
    
    ret = out.ret;

    margo_free_output(handle, &out);
    margo_bulk_free(in.bulk_handle);
    margo_destroy(handle);
    return(ret);
}

int bake_proxy_write(
    bake_provider_handle_t provider,
    bake_region_id_t rid,
    uint64_t region_offset,
    hg_bulk_t remote_bulk,
    uint64_t remote_offset,
    const char* remote_addr,
    uint64_t size)
{
    hg_return_t hret;
    hg_handle_t handle;
    bake_write_in_t in;
    bake_write_out_t out;
    int ret;

    in.rid = rid;
    in.region_offset = region_offset;
    in.bulk_handle = remote_bulk;
    in.bulk_offset = remote_offset;
    in.bulk_size = size;
    in.remote_addr_str = (char*)remote_addr;

    hret = margo_create(provider->client->mid, provider->addr,
        provider->client->bake_write_id, &handle);
    margo_set_target_id(handle, provider->mplex_id);

    if(hret != HG_SUCCESS)
        return(-1);

    hret = margo_forward(handle, &in);
    if(hret != HG_SUCCESS)
    {   
        margo_destroy(handle);
        return(-1);
    }
    
    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS)
    {   
        margo_destroy(handle);
        return(-1);
    }

    ret = out.ret;

    margo_free_output(handle, &out);
    margo_destroy(handle);
    return(ret);
}

int bake_create(
    bake_provider_handle_t provider,
    bake_target_id_t bti,
    uint64_t region_size,
    bake_region_id_t *rid)
{
    hg_return_t hret;
    hg_handle_t handle;
    bake_create_in_t in;
    bake_create_out_t out;
    int ret = 0;

    in.bti = bti;
    in.region_size = region_size;

    hret = margo_create(provider->client->mid, provider->addr,
            provider->client->bake_create_id, &handle);

    if(hret != HG_SUCCESS) {
        return(-1);
    }

    margo_set_target_id(handle, provider->mplex_id);

    hret = margo_forward(handle, &in);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        return(-1);
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        return(-1);
    }

    ret = out.ret;
    *rid = out.rid;

    margo_free_output(handle, &out);
    margo_destroy(handle);
    return(ret);
}


int bake_persist(
    bake_provider_handle_t provider,
    bake_region_id_t rid)
{
    hg_return_t hret;
    hg_handle_t handle;
    bake_persist_in_t in;
    bake_persist_out_t out;
    int ret;

    in.rid = rid;

    hret = margo_create(provider->client->mid, provider->addr,
            provider->client->bake_persist_id, &handle);
    margo_set_target_id(handle, provider->mplex_id);

    if(hret != HG_SUCCESS)
        return(-1);

    hret = margo_forward(handle, &in);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        return(-1);
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        return(-1);
    }

    ret = out.ret;

    margo_free_output(handle, &out);
    margo_destroy(handle);
    return(ret);
}

int bake_create_write_persist(
    bake_provider_handle_t provider,
    bake_target_id_t bti,
    uint64_t region_size,
    uint64_t region_offset,
    void const *buf,
    uint64_t buf_size,
    bake_region_id_t *rid)
{
    hg_return_t hret;
    hg_handle_t handle;
    bake_create_write_persist_in_t in;
    bake_create_write_persist_out_t out;
    int ret;

    /* XXX eager path? */

    in.bti = bti;
    in.region_size = region_size;
    in.region_offset = region_offset;
    in.bulk_offset = 0;
    in.bulk_size = buf_size;
    in.remote_addr_str = NULL; /* set remote_addr to NULL to disable proxy write */

    hret = margo_bulk_create(provider->client->mid, 1, (void**)(&buf), &buf_size,
        HG_BULK_READ_ONLY, &in.bulk_handle);
    if(hret != HG_SUCCESS)
        return(-1);

    hret = margo_create(provider->client->mid, provider->addr,
            provider->client->bake_create_write_persist_id, &handle);
    margo_set_target_id(handle, provider->mplex_id);

    if(hret != HG_SUCCESS)
    {
        margo_bulk_free(in.bulk_handle);
        return(-1);
    }

    hret = margo_forward(handle, &in);
    if(hret != HG_SUCCESS)
    {
        margo_bulk_free(in.bulk_handle);
        margo_destroy(handle);
        return(-1);
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS)
    {
        margo_bulk_free(in.bulk_handle);
        margo_destroy(handle);
        return(-1);
    }

    ret = out.ret;
    if(ret == 0)
        *rid = out.rid;

    margo_free_output(handle, &out);
    margo_bulk_free(in.bulk_handle);
    margo_destroy(handle);
    return(ret);
}

int bake_create_write_persist_proxy(
    bake_provider_handle_t provider,
    bake_target_id_t bti,
    uint64_t region_size,
    uint64_t region_offset,
    hg_bulk_t remote_bulk,
    uint64_t remote_offset,
    const char* remote_addr,
    uint64_t size,
    bake_region_id_t *rid)
{
    hg_return_t hret;
    hg_handle_t handle;
    bake_create_write_persist_in_t in;
    bake_create_write_persist_out_t out;
    int ret;

    in.bti = bti;
    in.region_size = region_size;
    in.region_offset = region_offset;
    in.bulk_handle = remote_bulk;
    in.bulk_offset = remote_offset;
    in.bulk_size = size;
    in.remote_addr_str = (char*)remote_addr;

    hret = margo_create(provider->client->mid, provider->addr,
            provider->client->bake_create_write_persist_id, &handle);
    margo_set_target_id(handle, provider->mplex_id);

    if(hret != HG_SUCCESS)
        return(-1);

    hret = margo_forward(handle, &in);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        return(-1);
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        return(-1);
    }

    ret = out.ret;
    if(ret == 0)
        *rid = out.rid;

    margo_free_output(handle, &out);
    margo_destroy(handle);
    return(ret);
}

int bake_get_size(
    bake_provider_handle_t provider,
    bake_region_id_t rid,
    uint64_t *region_size)
{
    hg_return_t hret;
    hg_handle_t handle;
    bake_get_size_in_t in;
    bake_get_size_out_t out;
    int ret;

    in.rid = rid;

    hret = margo_create(provider->client->mid, provider->addr,
        provider->client->bake_get_size_id, &handle);
    margo_set_target_id(handle, provider->mplex_id);

    if(hret != HG_SUCCESS)
        return(-1);

    hret = margo_forward(handle, &in);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        return(-1);
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        return(-1);
    }

    ret = out.ret;
    *region_size = out.size;

    margo_free_output(handle, &out);
    margo_destroy(handle);
    return(ret);
}

int bake_noop(bake_provider_handle_t provider)
{
    hg_return_t hret;
    hg_handle_t handle;

    hret = margo_create(provider->client->mid, provider->addr,
        provider->client->bake_noop_id, &handle);
    margo_set_target_id(handle, provider->mplex_id);

    if(hret != HG_SUCCESS)
        return(-1);

    hret = margo_forward(handle, NULL);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        return(-1);
    }

    margo_destroy(handle);
    return(0);
}

static int bake_eager_read(
    bake_provider_handle_t provider,
    bake_region_id_t rid,
    uint64_t region_offset,
    void *buf,
    uint64_t buf_size)
{
    hg_return_t hret;
    hg_handle_t handle;
    bake_eager_read_in_t in;
    bake_eager_read_out_t out;
    int ret;

    in.rid = rid;
    in.region_offset = region_offset;
    in.size = buf_size;

    hret = margo_create(provider->client->mid, provider->addr,
        provider->client->bake_eager_read_id, &handle);
    margo_set_target_id(handle, provider->mplex_id);

    if(hret != HG_SUCCESS)
        return(-1);
  
    hret = margo_forward(handle, &in);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        return(-1);
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        return(-1);
    }
    
    ret = out.ret;
    if(ret == 0)
        memcpy(buf, out.buffer, out.size);

    margo_free_output(handle, &out);
    margo_destroy(handle);
    return(ret);
}

int bake_read(
    bake_provider_handle_t provider,
    bake_region_id_t rid,
    uint64_t region_offset,
    void *buf,
    uint64_t buf_size)
{
    hg_return_t hret;
    hg_handle_t handle;
    bake_read_in_t in;
    bake_read_out_t out;
    int ret;

    if(buf_size <= BAKE_EAGER_LIMIT)
        return(bake_eager_read(provider, rid, region_offset, buf, buf_size));

    in.rid = rid;
    in.region_offset = region_offset;
    in.bulk_offset = 0;
    in.bulk_size = buf_size;
    in.remote_addr_str = NULL; /* set remote_addr to NULL to disable proxy read */

    hret = margo_bulk_create(provider->client->mid, 1, (void**)(&buf), &buf_size, 
        HG_BULK_WRITE_ONLY, &in.bulk_handle);
    if(hret != HG_SUCCESS)
        return(-1);
   
    hret = margo_create(provider->client->mid, provider->addr,
            provider->client->bake_read_id, &handle);
    margo_set_target_id(handle, provider->mplex_id);

    if(hret != HG_SUCCESS)
    {
        margo_bulk_free(in.bulk_handle);
        return(-1);
    }

    hret = margo_forward(handle, &in);
    if(hret != HG_SUCCESS)
    {
        margo_bulk_free(in.bulk_handle);
        margo_destroy(handle);
        return(-1);
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS)
    {
        margo_bulk_free(in.bulk_handle);
        margo_destroy(handle);
        return(-1);
    }
    
    ret = out.ret;

    margo_free_output(handle, &out);
    margo_bulk_free(in.bulk_handle);
    margo_destroy(handle);
    return(ret);
}

int bake_proxy_read(
    bake_provider_handle_t provider,
    bake_region_id_t rid,
    uint64_t region_offset,
    hg_bulk_t remote_bulk,
    uint64_t remote_offset,
    const char* remote_addr,
    uint64_t size)
{
    hg_return_t hret;
    hg_handle_t handle;
    bake_read_in_t in;
    bake_read_out_t out;
    int ret;

    in.rid = rid;
    in.region_offset = region_offset;
    in.bulk_handle = remote_bulk;
    in.bulk_offset = remote_offset;
    in.bulk_size = size; 
    in.remote_addr_str = (char*)remote_addr;

    hret = margo_create(provider->client->mid, provider->addr,
            provider->client->bake_read_id, &handle);
    margo_set_target_id(handle, provider->mplex_id);

    if(hret != HG_SUCCESS)
        return(-1);

    hret = margo_forward(handle, &in);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        return(-1);
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        return(-1);
    }

    ret = out.ret;

    margo_free_output(handle, &out);
    margo_destroy(handle);
    return(ret);
}

