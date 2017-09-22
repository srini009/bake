/*
 * (C) 2016 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <assert.h>
#include <bake-bulk-client.h>
#include <margo.h>
#include "uthash.h"
#include "bake-bulk-rpc.h"

/* Refers to a single Margo initialization, for now this is shared by
 * all remote targets.  In the future we probably need to support multiple in
 * case we run atop more than one transport at a time.
 */
struct bake_margo_instance
{
    margo_instance_id mid;  
    int refct;

    hg_id_t bake_bulk_probe_id;
    hg_id_t bake_bulk_shutdown_id; 
    hg_id_t bake_bulk_create_id;
    hg_id_t bake_bulk_eager_write_id;
    hg_id_t bake_bulk_eager_read_id;
    hg_id_t bake_bulk_write_id;
    hg_id_t bake_bulk_persist_id;
    hg_id_t bake_bulk_get_size_id;
    hg_id_t bake_bulk_read_id;
    hg_id_t bake_bulk_noop_id;
};

/* Refers to an instance connected to a specific target */
struct bake_instance
{
    bake_target_id_t bti;   /* persistent identifier for this target */
    hg_addr_t dest;         /* resolved Mercury address */
    UT_hash_handle hh;
};

struct bake_instance *instance_hash = NULL;


struct bake_margo_instance g_margo_inst = {
    .mid = MARGO_INSTANCE_NULL,
    .refct = 0,
};

static int bake_bulk_eager_read(
    bake_target_id_t bti,
    bake_bulk_region_id_t rid,
    uint64_t region_offset,
    void *buf,
    uint64_t buf_size);


static int bake_margo_instance_init(const char *mercury_dest)
{
    char hg_na[64] = {0};
    int i;

    /* have we already started a Margo instance? */
    if(g_margo_inst.refct > 0)
    {
        g_margo_inst.refct++;
        return(0);
    }

    /* initialize Margo using the transport portion of the destination
     * address (i.e., the part before the first : character if present)
     */
    for(i=0; (i<63 && mercury_dest[i] != '\0' && mercury_dest[i] != ':'); i++)
        hg_na[i] = mercury_dest[i];

    g_margo_inst.mid = margo_init(hg_na, MARGO_CLIENT_MODE, 0, 0);
    if(g_margo_inst.mid == MARGO_INSTANCE_NULL)
    {
        return(-1);
    }
    g_margo_inst.refct = 1;

    /* register RPCs */
    g_margo_inst.bake_bulk_probe_id = 
        MARGO_REGISTER(g_margo_inst.mid, 
        "bake_bulk_probe_rpc", void, bake_bulk_probe_out_t, 
        NULL);
    g_margo_inst.bake_bulk_shutdown_id = 
        MARGO_REGISTER(g_margo_inst.mid, 
        "bake_bulk_shutdown_rpc", void, void, 
        NULL);
    g_margo_inst.bake_bulk_create_id = 
        MARGO_REGISTER(g_margo_inst.mid, 
        "bake_bulk_create_rpc", 
        bake_bulk_create_in_t,
        bake_bulk_create_out_t,
        NULL);
    g_margo_inst.bake_bulk_write_id = 
        MARGO_REGISTER(g_margo_inst.mid, 
        "bake_bulk_write_rpc", 
        bake_bulk_write_in_t,
        bake_bulk_write_out_t,
        NULL);
    g_margo_inst.bake_bulk_eager_write_id = 
        MARGO_REGISTER(g_margo_inst.mid, 
        "bake_bulk_eager_write_rpc", 
        bake_bulk_eager_write_in_t,
        bake_bulk_eager_write_out_t,
        NULL);
    g_margo_inst.bake_bulk_eager_read_id = 
        MARGO_REGISTER(g_margo_inst.mid, 
        "bake_bulk_eager_read_rpc", 
        bake_bulk_eager_read_in_t,
        bake_bulk_eager_read_out_t,
        NULL);
    g_margo_inst.bake_bulk_persist_id = 
        MARGO_REGISTER(g_margo_inst.mid, 
        "bake_bulk_persist_rpc", 
        bake_bulk_persist_in_t,
        bake_bulk_persist_out_t,
        NULL);
    g_margo_inst.bake_bulk_get_size_id = 
        MARGO_REGISTER(g_margo_inst.mid, 
        "bake_bulk_get_size_rpc", 
        bake_bulk_get_size_in_t,
        bake_bulk_get_size_out_t,
        NULL);
    g_margo_inst.bake_bulk_read_id = 
        MARGO_REGISTER(g_margo_inst.mid, 
        "bake_bulk_read_rpc", 
        bake_bulk_read_in_t,
        bake_bulk_read_out_t,
        NULL);
    g_margo_inst.bake_bulk_noop_id = 
        MARGO_REGISTER(g_margo_inst.mid, 
        "bake_bulk_noop_rpc", 
        void,
        void,
        NULL);

    return(0);
}

void bake_margo_instance_finalize(void)
{
    g_margo_inst.refct--;

    assert(g_margo_inst.refct > -1);

    if(g_margo_inst.refct == 0)
    {
        margo_finalize(g_margo_inst.mid);
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
    struct bake_instance *new_instance;

    ret = bake_margo_instance_init(mercury_dest);
    if(ret < 0)
        return(ret);

    new_instance = calloc(1, sizeof(*new_instance));
    if(!new_instance)
    {
        bake_margo_instance_finalize();
        return(-1);
    }

    hret = margo_addr_lookup(g_margo_inst.mid, mercury_dest, &new_instance->dest);
    if(hret != HG_SUCCESS)
    {
        free(new_instance);
        bake_margo_instance_finalize();
        return(-1);
    }

    /* create handle */
    hret = margo_create(g_margo_inst.mid, new_instance->dest, 
        g_margo_inst.bake_bulk_probe_id, &handle);
    if(hret != HG_SUCCESS)
    {
        free(new_instance);
        bake_margo_instance_finalize();
        return(-1);
    }

    hret = margo_forward(handle, NULL);
    if(hret != HG_SUCCESS)
    {
        free(new_instance);
        margo_destroy(handle);
        bake_margo_instance_finalize();
        return(-1);
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS)
    {
        free(new_instance);
        margo_destroy(handle);
        bake_margo_instance_finalize();
        return(-1);
    }

    ret = out.ret;
    *bti = out.bti;
    new_instance->bti = out.bti;

    margo_free_output(handle, &out);
    margo_destroy(handle);

    if(ret != 0)
    {
        free(new_instance);
        bake_margo_instance_finalize();
    }
    else
    {
        /* TODO: safety check that it isn't already there.  Here or earlier? */
        HASH_ADD(hh, instance_hash, bti, sizeof(new_instance->bti), new_instance);
    }

    return(ret);
}
  
void bake_release_instance(
    bake_target_id_t bti)
{
    struct bake_instance *instance = NULL;

    HASH_FIND(hh, instance_hash, &bti, sizeof(bti), instance);
    if(!instance)
        return;
    
    HASH_DELETE(hh, instance_hash, instance);
    margo_addr_free(g_margo_inst.mid, instance->dest);
    free(instance);
    bake_margo_instance_finalize();

    return;
}

int bake_shutdown_service(bake_target_id_t bti)
{
    hg_return_t hret;
    struct bake_instance *instance = NULL;
    hg_handle_t handle;

    HASH_FIND(hh, instance_hash, &bti, sizeof(bti), instance);
    if(!instance)
        return(-1);

    hret = margo_create(g_margo_inst.mid, instance->dest, 
        g_margo_inst.bake_bulk_shutdown_id, &handle);
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

static int bake_bulk_eager_write(
    bake_target_id_t bti,
    bake_bulk_region_id_t rid,
    uint64_t region_offset,
    void const *buf,
    uint64_t buf_size)
{
    hg_return_t hret;
    hg_handle_t handle;
    bake_bulk_eager_write_in_t in;
    bake_bulk_eager_write_out_t out;
    int ret;
    struct bake_instance *instance = NULL;

    HASH_FIND(hh, instance_hash, &bti, sizeof(bti), instance);
    if(!instance)
        return(-1);

    in.bti = bti;
    in.rid = rid;
    in.region_offset = region_offset;
    in.size = buf_size;
    in.buffer = (char*)buf;
  
    hret = margo_create(g_margo_inst.mid, instance->dest, 
        g_margo_inst.bake_bulk_eager_write_id, &handle);
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

#define BAKE_BULK_EAGER_LIMIT 2048

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
    struct bake_instance *instance = NULL;

    if(buf_size <= BAKE_BULK_EAGER_LIMIT)
        return(bake_bulk_eager_write(bti, rid, region_offset, buf, buf_size));

    HASH_FIND(hh, instance_hash, &bti, sizeof(bti), instance);
    if(!instance)
        return(-1);

    in.bti = bti;
    in.rid = rid;
    in.region_offset = region_offset;

    hret = margo_bulk_create(g_margo_inst.mid, 1, (void**)(&buf), &buf_size, 
        HG_BULK_READ_ONLY, &in.bulk_handle);
    if(hret != HG_SUCCESS)
        return(-1);
   
    hret = margo_create(g_margo_inst.mid, instance->dest, 
        g_margo_inst.bake_bulk_write_id, &handle);
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
    struct bake_instance *instance = NULL;

    HASH_FIND(hh, instance_hash, &bti, sizeof(bti), instance);
    if(!instance)
        return(-1);

    in.bti = bti;
    in.region_size = region_size;

    hret = margo_create(g_margo_inst.mid, instance->dest,
        g_margo_inst.bake_bulk_create_id, &handle);
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
    *rid = out.rid;

    margo_free_output(handle, &out);
    margo_destroy(handle);
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
    struct bake_instance *instance = NULL;

    HASH_FIND(hh, instance_hash, &bti, sizeof(bti), instance);
    if(!instance)
        return(-1);

    in.bti = bti;
    in.rid = rid;

    hret = margo_create(g_margo_inst.mid, instance->dest,
        g_margo_inst.bake_bulk_persist_id, &handle);
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
    struct bake_instance *instance = NULL;

    HASH_FIND(hh, instance_hash, &bti, sizeof(bti), instance);
    if(!instance)
        return(-1);

    in.bti = bti;
    in.rid = rid;

    hret = margo_create(g_margo_inst.mid, instance->dest,
        g_margo_inst.bake_bulk_get_size_id, &handle);
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

int bake_bulk_noop(
    bake_target_id_t bti)
{
    hg_return_t hret;
    hg_handle_t handle;
    struct bake_instance *instance = NULL;

    HASH_FIND(hh, instance_hash, &bti, sizeof(bti), instance);
    if(!instance)
        return(-1);

    hret = margo_create(g_margo_inst.mid, instance->dest,
        g_margo_inst.bake_bulk_noop_id, &handle);
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
    struct bake_instance *instance = NULL;

    if(buf_size <= BAKE_BULK_EAGER_LIMIT)
        return(bake_bulk_eager_read(bti, rid, region_offset, buf, buf_size));

    HASH_FIND(hh, instance_hash, &bti, sizeof(bti), instance);
    if(!instance)
        return(-1);

    in.bti = bti;
    in.rid = rid;
    in.region_offset = region_offset;

    hret = margo_bulk_create(g_margo_inst.mid, 1, (void**)(&buf), &buf_size, 
        HG_BULK_WRITE_ONLY, &in.bulk_handle);
    if(hret != HG_SUCCESS)
        return(-1);
   
    hret = margo_create(g_margo_inst.mid, instance->dest,
        g_margo_inst.bake_bulk_read_id, &handle);
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


static int bake_bulk_eager_read(
    bake_target_id_t bti,
    bake_bulk_region_id_t rid,
    uint64_t region_offset,
    void *buf,
    uint64_t buf_size)
{
    hg_return_t hret;
    hg_handle_t handle;
    bake_bulk_eager_read_in_t in;
    bake_bulk_eager_read_out_t out;
    int ret;
    struct bake_instance *instance = NULL;

    HASH_FIND(hh, instance_hash, &bti, sizeof(bti), instance);
    if(!instance)
        return(-1);

    in.bti = bti;
    in.rid = rid;
    in.region_offset = region_offset;
    in.size = buf_size;

    hret = margo_create(g_margo_inst.mid, instance->dest,
        g_margo_inst.bake_bulk_eager_read_id, &handle);
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

