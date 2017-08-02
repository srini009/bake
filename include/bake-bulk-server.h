/*
 * (C) 2016 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#ifndef __BAKE_BULK_SERVER_H
#define __BAKE_BULK_SERVER_H

#include <margo.h>
#include <libpmemobj.h>
#include "bake-bulk.h"

struct bake_bulk_root
{
    bake_target_id_t target_id;
};

/**
 * Register a bake server instance for a given Margo instance.
 *
 * @param[in] mid Margo instance identifier
 * @param[in] bb_pmem_pool libpmem pool to use for the bake storage service
 * @param[in] bb_pmem_root libpmem root for the bake pool
 */
void bake_server_register(
    margo_instance_id mid,
    PMEMobjpool *bb_pmem_pool,
    struct bake_bulk_root *bb_pmem_root);

#endif /* __BAKE_BULK_SERVER_H */
