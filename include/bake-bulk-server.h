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

struct bake_pool_info
{
    PMEMobjpool           *bb_pmem_pool;
    struct bake_bulk_root *bb_pmem_root;
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
    struct bake_pool_info *pool_info);

/**
 * Convienence function to set up a PMEM backend.
 *
 * @param[in] poolname path to pmem backend file
 *
 * returns a pointer to an initialized `struct bake_pool_info` on sucess,
 * NULL if anything goes wrong
 */
struct bake_pool_info *bake_server_makepool(
	char *poolname);
#endif /* __BAKE_BULK_SERVER_H */
