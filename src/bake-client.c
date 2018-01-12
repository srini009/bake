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

struct bake_target {
    struct bake_client* client;
    bake_uuid_t pool_id;
    hg_addr_t   dest;
    uint8_t     mplex_id;
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

int bake_probe_instance(
    bake_client_t client,
    hg_addr_t dest_addr,
    uint8_t mplex_id,
    bake_target_id_t *bti)
{
    hg_return_t hret;
    int ret;
    bake_probe_out_t out;
    hg_handle_t handle;
    struct bake_target *new_target;

    new_target = calloc(1, sizeof(*new_target));
    if(!new_target)
        return(-1);

    new_target->client = client;
    new_target->mplex_id = mplex_id;

    hret = margo_addr_dup(client->mid, dest_addr, &new_target->dest);
    if(hret != HG_SUCCESS)
    {
        free(new_target);
        return(-1);
    }

    /* create handle */
    hret = margo_create(client->mid, new_target->dest, 
                    client->bake_probe_id, &handle);
    margo_set_target_id(handle, mplex_id);

    if(hret != HG_SUCCESS)
    {
        margo_addr_free(client->mid, new_target->dest);
        free(new_target);
        return(-1);
    }

    hret = margo_forward(handle, NULL);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        margo_addr_free(client->mid, new_target->dest);
        free(new_target);
        return(-1);
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        margo_addr_free(client->mid, new_target->dest);
        free(new_target);
        return(-1);
    }

    ret = out.ret;
    new_target->pool_id = out.pool_id;

    margo_free_output(handle, &out);
    margo_destroy(handle);

    if(ret != 0)
    {
        margo_addr_free(client->mid, new_target->dest);
        free(new_target);
    } else {
        *bti = new_target;
    }

    return(ret);
}
  
void bake_target_id_release(
    bake_target_id_t bti)
{
    margo_addr_free(bti->client->mid, bti->dest);
    free(bti);

    return;
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
    bake_target_id_t bti,
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

    in.pool_id = bti->pool_id;
    in.rid = rid;
    in.region_offset = region_offset;
    in.size = buf_size;
    in.buffer = (char*)buf;
  
    hret = margo_create(bti->client->mid, bti->dest, 
                bti->client->bake_eager_write_id, &handle);
    margo_set_target_id(handle, bti->mplex_id);

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
    bake_target_id_t bti,
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
        return(bake_eager_write(bti, rid, region_offset, buf, buf_size));

    in.pool_id = bti->pool_id;
    in.rid = rid;
    in.region_offset = region_offset;
    in.bulk_offset = 0;
    in.bulk_size = buf_size;
    in.remote_addr_str = NULL; /* set remote_addr to NULL to disable proxy write */

    hret = margo_bulk_create(bti->client->mid, 1, (void**)(&buf), &buf_size, 
        HG_BULK_READ_ONLY, &in.bulk_handle);
    if(hret != HG_SUCCESS)
        return(-1);
   
    hret = margo_create(bti->client->mid, bti->dest, 
        bti->client->bake_write_id, &handle);
    margo_set_target_id(handle, bti->mplex_id);

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
    bake_target_id_t bti,
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

    in.pool_id = bti->pool_id;
    in.rid = rid;
    in.region_offset = region_offset;
    in.bulk_handle = remote_bulk;
    in.bulk_offset = remote_offset;
    in.bulk_size = size;
    in.remote_addr_str = (char*)remote_addr;

    hret = margo_create(bti->client->mid, bti->dest,
        bti->client->bake_write_id, &handle);
    margo_set_target_id(handle, bti->mplex_id);

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
    bake_target_id_t bti,
    uint64_t region_size,
    bake_region_id_t *rid)
{
    hg_return_t hret;
    hg_handle_t handle;
    bake_create_in_t in;
    bake_create_out_t out;
    int ret = 0;

    in.pool_id = bti->pool_id;
    in.region_size = region_size;

    hret = margo_create(bti->client->mid, bti->dest,
        bti->client->bake_create_id, &handle);

    if(hret != HG_SUCCESS) {
        return(-1);
    }

    margo_set_target_id(handle, bti->mplex_id);

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
    bake_target_id_t bti,
    bake_region_id_t rid)
{
    hg_return_t hret;
    hg_handle_t handle;
    bake_persist_in_t in;
    bake_persist_out_t out;
    int ret;

    in.pool_id = bti->pool_id;
    in.rid = rid;

    hret = margo_create(bti->client->mid, bti->dest,
        bti->client->bake_persist_id, &handle);
    margo_set_target_id(handle, bti->mplex_id);

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

    in.pool_id = bti->pool_id;
    in.region_size = region_size;
    in.region_offset = region_offset;
    in.bulk_offset = 0;
    in.bulk_size = buf_size;
    in.remote_addr_str = NULL; /* set remote_addr to NULL to disable proxy write */

    hret = margo_bulk_create(bti->client->mid, 1, (void**)(&buf), &buf_size,
        HG_BULK_READ_ONLY, &in.bulk_handle);
    if(hret != HG_SUCCESS)
        return(-1);

    hret = margo_create(bti->client->mid, bti->dest,
        bti->client->bake_create_write_persist_id, &handle);
    margo_set_target_id(handle, bti->mplex_id);

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

    in.pool_id = bti->pool_id;
    in.region_size = region_size;
    in.region_offset = region_offset;
    in.bulk_handle = remote_bulk;
    in.bulk_offset = remote_offset;
    in.bulk_size = size;
    in.remote_addr_str = (char*)remote_addr;

    hret = margo_create(bti->client->mid, bti->dest,
        bti->client->bake_create_write_persist_id, &handle);
    margo_set_target_id(handle, bti->mplex_id);

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
    bake_target_id_t bti,
    bake_region_id_t rid,
    uint64_t *region_size)
{
    hg_return_t hret;
    hg_handle_t handle;
    bake_get_size_in_t in;
    bake_get_size_out_t out;
    int ret;

    in.pool_id = bti->pool_id;
    in.rid = rid;

    hret = margo_create(bti->client->mid, bti->dest,
        bti->client->bake_get_size_id, &handle);
    margo_set_target_id(handle, bti->mplex_id);

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

int bake_noop(
    bake_target_id_t bti)
{
    hg_return_t hret;
    hg_handle_t handle;

    hret = margo_create(bti->client->mid, bti->dest,
        bti->client->bake_noop_id, &handle);
    margo_set_target_id(handle, bti->mplex_id);

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
    bake_target_id_t bti,
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

    in.pool_id = bti->pool_id;
    in.rid = rid;
    in.region_offset = region_offset;
    in.size = buf_size;

    hret = margo_create(bti->client->mid, bti->dest,
        bti->client->bake_eager_read_id, &handle);
    margo_set_target_id(handle, bti->mplex_id);

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
    bake_target_id_t bti,
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
        return(bake_eager_read(bti, rid, region_offset, buf, buf_size));

    in.pool_id = bti->pool_id;
    in.rid = rid;
    in.region_offset = region_offset;
    in.bulk_offset = 0;
    in.bulk_size = buf_size;
    in.remote_addr_str = NULL; /* set remote_addr to NULL to disable proxy read */

    hret = margo_bulk_create(bti->client->mid, 1, (void**)(&buf), &buf_size, 
        HG_BULK_WRITE_ONLY, &in.bulk_handle);
    if(hret != HG_SUCCESS)
        return(-1);
   
    hret = margo_create(bti->client->mid, bti->dest,
        bti->client->bake_read_id, &handle);
    margo_set_target_id(handle, bti->mplex_id);

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
    bake_target_id_t bti,
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

    in.pool_id = bti->pool_id;
    in.rid = rid;
    in.region_offset = region_offset;
    in.bulk_handle = remote_bulk;
    in.bulk_offset = remote_offset;
    in.bulk_size = size; 
    in.remote_addr_str = (char*)remote_addr;

    hret = margo_create(bti->client->mid, bti->dest,
        bti->client->bake_read_id, &handle);
    margo_set_target_id(handle, bti->mplex_id);

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

