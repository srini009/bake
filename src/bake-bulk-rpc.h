/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#ifndef __BAKE_BULK_RPC
#define __BAKE_BULK_RPC

#include <margo.h>
#include <bake-bulk.h>

/* encoders for bake-specific types */
static inline hg_return_t hg_proc_bake_target_id_t(hg_proc_t proc, bake_target_id_t *bti);
static inline hg_return_t hg_proc_bake_bulk_region_id_t(hg_proc_t proc, bake_bulk_region_id_t *rid);

/* shutdown */
DECLARE_MARGO_RPC_HANDLER(bake_bulk_shutdown_ult)

/* bulk create */
MERCURY_GEN_PROC(bake_bulk_create_in_t,
    ((bake_target_id_t)(bti))\
    ((uint64_t)(region_size)))
MERCURY_GEN_PROC(bake_bulk_create_out_t,
    ((int32_t)(ret))\
    ((bake_bulk_region_id_t)(rid)))
DECLARE_MARGO_RPC_HANDLER(bake_bulk_create_ult)

/* bulk write */
MERCURY_GEN_PROC(bake_bulk_write_in_t,
    ((bake_target_id_t)(bti))\
    ((bake_bulk_region_id_t)(rid))\
    ((uint64_t)(region_offset))\
    ((hg_bulk_t)(bulk_handle)))
MERCURY_GEN_PROC(bake_bulk_write_out_t,
    ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(bake_bulk_write_ult)

/* bulk persist */
MERCURY_GEN_PROC(bake_bulk_persist_in_t,
    ((bake_target_id_t)(bti))\
    ((bake_bulk_region_id_t)(rid)))
MERCURY_GEN_PROC(bake_bulk_persist_out_t,
    ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(bake_bulk_persist_ult)



/* TODO: where should the encoder defs live?  Not in bake-bulk-rpc.c because 
 * we don't really need the rpc handlers to be linked into clients...
 */
static inline hg_return_t hg_proc_bake_bulk_region_id_t(hg_proc_t proc, bake_bulk_region_id_t *rid)
{
    /* TODO: update later depending on final region_id_t type */
    int i;
    hg_return_t ret;

    for(i=0; i<BAKE_BULK_REGION_ID_SIZE; i++)
    {
        ret = hg_proc_hg_uint8_t(proc, (uint8_t*)&rid->data[i]);
        if(ret != HG_SUCCESS)
            return(ret);
    }
    return(HG_SUCCESS);
}

static inline hg_return_t hg_proc_bake_target_id_t(hg_proc_t proc, bake_target_id_t *bti)
{
    /* TODO: will probably have to update this later when we have a better
     * idea of what the target identifier will look like.
     */
    return(hg_proc_uint64_t(proc, bti));
}

#endif /* __BAKE_BULK_RPC */
