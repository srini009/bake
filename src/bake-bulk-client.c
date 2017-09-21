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
    hg_id_t bake_bulk_eager_write_id;
    hg_id_t bake_bulk_eager_read_id;
    hg_id_t bake_bulk_write_id;
    hg_id_t bake_bulk_persist_id;
    hg_id_t bake_bulk_get_size_id;
    hg_id_t bake_bulk_read_id;
    hg_id_t bake_bulk_noop_id;
};

struct bake_handle_cache_el
{
    hg_id_t id;
    hg_handle_t handle;
    UT_hash_handle hh;
};

/* Refers to an instance connected to a specific target */
struct bake_instance
{
    bake_target_id_t bti;   /* persistent identifier for this target */
    hg_addr_t dest;         /* resolved Mercury address */
    struct bake_handle_cache_el *handle_hash;
    UT_hash_handle hh;
};

struct bake_instance *instance_hash = NULL;


struct hg_instance g_hginst = {
    .mid = MARGO_INSTANCE_NULL,
    .hg_class = NULL,
    .hg_context = NULL,
    .refct = 0,
};

static int bake_bulk_eager_read(
    bake_target_id_t bti,
    bake_bulk_region_id_t rid,
    uint64_t region_offset,
    void *buf,
    uint64_t buf_size);
static struct bake_handle_cache_el *get_handle(struct bake_instance *instance, hg_id_t id);
static void put_handle(struct bake_instance *instance, struct bake_handle_cache_el *el);

static int hg_instance_init(const char *mercury_dest)
{
    char hg_na[64] = {0};
    int i;

    /* have we already started a Mercury instance? */
    if(g_hginst.refct > 0)
    {
        g_hginst.refct++;
        return(0);
    }

    /* boilerplate HG initialization steps */
    /***************************************/

    /* initialize Mercury using the transport portion of the destination
     * address (i.e., the part before the first : character if present)
     */
    for(i=0; (i<63 && mercury_dest[i] != '\0' && mercury_dest[i] != ':'); i++)
        hg_na[i] = mercury_dest[i];

    g_hginst.hg_class = HG_Init(hg_na, HG_FALSE);
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
    g_hginst.bake_bulk_eager_write_id = 
        MERCURY_REGISTER(g_hginst.hg_class, 
        "bake_bulk_eager_write_rpc", 
        bake_bulk_eager_write_in_t,
        bake_bulk_eager_write_out_t,
        NULL);
    g_hginst.bake_bulk_eager_read_id = 
        MERCURY_REGISTER(g_hginst.hg_class, 
        "bake_bulk_eager_read_rpc", 
        bake_bulk_eager_read_in_t,
        bake_bulk_eager_read_out_t,
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
    g_hginst.bake_bulk_noop_id = 
        MERCURY_REGISTER(g_hginst.hg_class, 
        "bake_bulk_noop_rpc", 
        void,
        void,
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
    struct bake_instance *new_instance;

    ret = hg_instance_init(mercury_dest);
    if(ret < 0)
        return(ret);

    new_instance = calloc(1, sizeof(*new_instance));
    if(!new_instance)
    {
        hg_instance_finalize();
        return(-1);
    }

    hret = margo_addr_lookup(g_hginst.mid, mercury_dest, &new_instance->dest);
    if(hret != HG_SUCCESS)
    {
        free(new_instance);
        hg_instance_finalize();
        return(-1);
    }

    /* create handle */
    hret = HG_Create(g_hginst.hg_context, new_instance->dest, 
        g_hginst.bake_bulk_probe_id, &handle);
    if(hret != HG_SUCCESS)
    {
        free(new_instance);
        hg_instance_finalize();
        return(-1);
    }

    hret = margo_forward(g_hginst.mid, handle, NULL);
    if(hret != HG_SUCCESS)
    {
        free(new_instance);
        HG_Destroy(handle);
        hg_instance_finalize();
        return(-1);
    }

    hret = HG_Get_output(handle, &out);
    if(hret != HG_SUCCESS)
    {
        free(new_instance);
        HG_Destroy(handle);
        hg_instance_finalize();
        return(-1);
    }

    ret = out.ret;
    *bti = out.bti;
    new_instance->bti = out.bti;

    HG_Free_output(handle, &out);
    HG_Destroy(handle);

    if(ret != 0)
    {
        free(new_instance);
        hg_instance_finalize();
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
    HG_Addr_free(g_hginst.hg_class, instance->dest);
    free(instance);
    hg_instance_finalize();

    return;
}

int bake_shutdown_service(bake_target_id_t bti)
{
    hg_return_t hret;
    struct bake_instance *instance = NULL;
    struct bake_handle_cache_el *el = NULL;

    HASH_FIND(hh, instance_hash, &bti, sizeof(bti), instance);
    if(!instance)
        return(-1);

    el = get_handle(instance, g_hginst.bake_bulk_shutdown_id);
    assert(el);

    hret = margo_forward(g_hginst.mid, el->handle, NULL);
    if(hret != HG_SUCCESS)
    {
        put_handle(instance, el);
        return(-1);
    }

    put_handle(instance, el);
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
    bake_bulk_eager_write_in_t in;
    bake_bulk_eager_write_out_t out;
    int ret;
    struct bake_instance *instance = NULL;
    struct bake_handle_cache_el *el = NULL;

    HASH_FIND(hh, instance_hash, &bti, sizeof(bti), instance);
    if(!instance)
        return(-1);

    in.bti = bti;
    in.rid = rid;
    in.region_offset = region_offset;
    in.size = buf_size;
    in.buffer = (char*)buf;
  
    el = get_handle(instance, g_hginst.bake_bulk_eager_write_id);
    assert(el);

    hret = margo_forward(g_hginst.mid, el->handle, &in);
    if(hret != HG_SUCCESS)
    {
        put_handle(instance, el);
        return(-1);
    }

    hret = HG_Get_output(el->handle, &out);
    if(hret != HG_SUCCESS)
    {
        put_handle(instance, el);
        return(-1);
    }
    
    ret = out.ret;

    HG_Free_output(el->handle, &out);
    put_handle(instance, el);

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
    bake_bulk_write_in_t in;
    bake_bulk_write_out_t out;
    int ret;
    struct bake_instance *instance = NULL;
    struct bake_handle_cache_el *el = NULL;

    if(buf_size <= BAKE_BULK_EAGER_LIMIT)
    {
        return(bake_bulk_eager_write(bti, rid, region_offset, buf, buf_size));
    }

    HASH_FIND(hh, instance_hash, &bti, sizeof(bti), instance);
    if(!instance)
        return(-1);

    in.bti = bti;
    in.rid = rid;
    in.region_offset = region_offset;

    hret = HG_Bulk_create(g_hginst.hg_class, 1, (void**)(&buf), &buf_size, 
        HG_BULK_READ_ONLY, &in.bulk_handle);
    if(hret != HG_SUCCESS)
    {
        return(-1);
    }
   
    el = get_handle(instance, g_hginst.bake_bulk_write_id);
    assert(el);

    hret = margo_forward(g_hginst.mid, el->handle, &in);
    if(hret != HG_SUCCESS)
    {
        HG_Bulk_free(in.bulk_handle);
        put_handle(instance, el);
        return(-1);
    }

    hret = HG_Get_output(el->handle, &out);
    if(hret != HG_SUCCESS)
    {
        HG_Bulk_free(in.bulk_handle);
        put_handle(instance, el);
        return(-1);
    }
    
    ret = out.ret;

    HG_Free_output(el->handle, &out);
    HG_Bulk_free(in.bulk_handle);
    put_handle(instance, el);
    return(ret);
}

int bake_bulk_create(
    bake_target_id_t bti,
    uint64_t region_size,
    bake_bulk_region_id_t *rid)
{
    hg_return_t hret;
    bake_bulk_create_in_t in;
    bake_bulk_create_out_t out;
    int ret;
    struct bake_instance *instance = NULL;
    struct bake_handle_cache_el *el = NULL;

    HASH_FIND(hh, instance_hash, &bti, sizeof(bti), instance);
    if(!instance)
        return(-1);

    in.bti = bti;
    in.region_size = region_size;

    el = get_handle(instance, g_hginst.bake_bulk_create_id);
    assert(el);

    hret = margo_forward(g_hginst.mid, el->handle, &in);
    if(hret != HG_SUCCESS)
    {
        put_handle(instance, el);
        return(-1);
    }

    hret = HG_Get_output(el->handle, &out);
    if(hret != HG_SUCCESS)
    {
        put_handle(instance, el);
        return(-1);
    }

    ret = out.ret;
    *rid = out.rid;

    HG_Free_output(el->handle, &out);
    put_handle(instance, el);
    return(ret);
}


int bake_bulk_persist(
    bake_target_id_t bti,
    bake_bulk_region_id_t rid)
{
    hg_return_t hret;
    bake_bulk_persist_in_t in;
    bake_bulk_persist_out_t out;
    int ret;
    struct bake_instance *instance = NULL;
    struct bake_handle_cache_el *el = NULL;

    HASH_FIND(hh, instance_hash, &bti, sizeof(bti), instance);
    if(!instance)
        return(-1);

    in.bti = bti;
    in.rid = rid;

    el = get_handle(instance, g_hginst.bake_bulk_persist_id);
    assert(el);

    hret = margo_forward(g_hginst.mid, el->handle, &in);
    if(hret != HG_SUCCESS)
    {
        put_handle(instance, el);
        return(-1);
    }

    hret = HG_Get_output(el->handle, &out);
    if(hret != HG_SUCCESS)
    {
        put_handle(instance, el);
        return(-1);
    }

    ret = out.ret;

    HG_Free_output(el->handle, &out);
    put_handle(instance, el);
    return(ret);
}

int bake_bulk_get_size(
    bake_target_id_t bti,
    bake_bulk_region_id_t rid,
    uint64_t *region_size)
{
    hg_return_t hret;
    bake_bulk_get_size_in_t in;
    bake_bulk_get_size_out_t out;
    int ret;
    struct bake_instance *instance = NULL;
    struct bake_handle_cache_el *el = NULL;

    HASH_FIND(hh, instance_hash, &bti, sizeof(bti), instance);
    if(!instance)
        return(-1);

    in.bti = bti;
    in.rid = rid;

    el = get_handle(instance, g_hginst.bake_bulk_get_size_id);
    assert(el);

    hret = margo_forward(g_hginst.mid, el->handle, &in);
    if(hret != HG_SUCCESS)
    {
        put_handle(instance, el);
        return(-1);
    }

    hret = HG_Get_output(el->handle, &out);
    if(hret != HG_SUCCESS)
    {
        put_handle(instance, el);
        return(-1);
    }

    ret = out.ret;
    *region_size = out.size;

    HG_Free_output(el->handle, &out);
    put_handle(instance, el);
    return(ret);
}

int bake_bulk_noop(
    bake_target_id_t bti)
{
    hg_return_t hret;
    struct bake_instance *instance = NULL;
    struct bake_handle_cache_el *el = NULL;

    HASH_FIND(hh, instance_hash, &bti, sizeof(bti), instance);
    if(!instance)
        return(-1);

    el = get_handle(instance, g_hginst.bake_bulk_noop_id);
    assert(el);

    hret = margo_forward(g_hginst.mid, el->handle, NULL);
    if(hret != HG_SUCCESS)
    {
        put_handle(instance, el);
        return(-1);
    }

    put_handle(instance, el);
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
    bake_bulk_read_in_t in;
    bake_bulk_read_out_t out;
    int ret;
    struct bake_instance *instance = NULL;
    struct bake_handle_cache_el *el = NULL;

    if(buf_size <= BAKE_BULK_EAGER_LIMIT)
    {
        return(bake_bulk_eager_read(bti, rid, region_offset, buf, buf_size));
    }

    HASH_FIND(hh, instance_hash, &bti, sizeof(bti), instance);
    if(!instance)
        return(-1);

    in.bti = bti;
    in.rid = rid;
    in.region_offset = region_offset;

    hret = HG_Bulk_create(g_hginst.hg_class, 1, (void**)(&buf), &buf_size, 
        HG_BULK_WRITE_ONLY, &in.bulk_handle);
    if(hret != HG_SUCCESS)
    {
        return(-1);
    }
   
    el = get_handle(instance, g_hginst.bake_bulk_read_id);
    assert(el);

    hret = margo_forward(g_hginst.mid, el->handle, &in);
    if(hret != HG_SUCCESS)
    {
        HG_Bulk_free(in.bulk_handle);
        put_handle(instance, el);
        return(-1);
    }

    hret = HG_Get_output(el->handle, &out);
    if(hret != HG_SUCCESS)
    {
        HG_Bulk_free(in.bulk_handle);
        put_handle(instance, el);
        return(-1);
    }
    
    ret = out.ret;

    HG_Free_output(el->handle, &out);
    HG_Bulk_free(in.bulk_handle);
    put_handle(instance, el);
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
    bake_bulk_eager_read_in_t in;
    bake_bulk_eager_read_out_t out;
    int ret;
    struct bake_instance *instance = NULL;
    struct bake_handle_cache_el *el = NULL;

    HASH_FIND(hh, instance_hash, &bti, sizeof(bti), instance);
    if(!instance)
        return(-1);

    in.bti = bti;
    in.rid = rid;
    in.region_offset = region_offset;
    in.size = buf_size;
   
    el = get_handle(instance, g_hginst.bake_bulk_eager_read_id);
    assert(el);

    hret = margo_forward(g_hginst.mid, el->handle, &in);
    if(hret != HG_SUCCESS)
    {
        put_handle(instance, el);
        return(-1);
    }

    hret = HG_Get_output(el->handle, &out);
    if(hret != HG_SUCCESS)
    {
        put_handle(instance, el);
        return(-1);
    }
    
    ret = out.ret;
    if(ret == 0)
        memcpy(buf, out.buffer, out.size);

    HG_Free_output(el->handle, &out);
    put_handle(instance, el);
    return(ret);
}

static struct bake_handle_cache_el *get_handle(struct bake_instance *instance, hg_id_t id)
{
    struct bake_handle_cache_el *el = NULL;
    hg_return_t hret;

    HASH_FIND(hh, instance->handle_hash, &id, sizeof(id), el);
    if(el)
    {
        HASH_DELETE(hh, instance->handle_hash, el);
        return(el);
    }

    el = malloc(sizeof(*el));
    if(!el)
        return(NULL);
    el->id = id;

    /* create handle */
    hret = HG_Create(g_hginst.hg_context, instance->dest, 
        id, &el->handle);
    if(hret != HG_SUCCESS)
    {
        free(el);
        return(NULL);
    }

    return(el);
}


static void put_handle(struct bake_instance *instance, struct bake_handle_cache_el *el)
{
    HASH_ADD(hh, instance->handle_hash, id, sizeof(el->id), el);
}