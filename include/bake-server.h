/*
 * (C) 2016 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#ifndef __BAKE_SERVER_H
#define __BAKE_SERVER_H

#include <margo.h>
#include <libpmemobj.h>
#include "bake.h"

#ifdef __cplusplus
extern "C" {
#endif

#define BAKE_ABT_POOL_DEFAULT ABT_POOL_NULL
#define BAKE_MPLEX_ID_DEFAULT 0
#define BAKE_PROVIDER_IGNORE NULL

typedef struct bake_server_context_t* bake_provider_t;

/**
 * Creates a BAKE pool to use for backend PMEM storage.
 * 
 * NOTE: This function must be called on a pool before the pool
 * can be passed to 'bake_provider_register'.
 *
 * @param[in] pool_name path to PMEM backend file
 * @param[in] pool_size size of the created pool
 * @param[in] pool_mode mode of the created pool
 * @returns 0 on success, -1 otherwise
 */
int bake_makepool(
    const char *pool_name,
    size_t pool_size,
    mode_t pool_mode);

/**
 * Initializes a BAKE server instance.
 *
 * @param[in] mid Margo instance identifier
 * @param[in] mplex_id Multiplex id
 * @param[in] pool Pool on which to run the RPC handlers
 * @param[in] pool_name path to PMEM backend file
 * @param[out] provider resulting provider
 * @returns 0 on success, -1 otherwise
 */
int bake_provider_register(
    margo_instance_id mid,
    uint32_t mplex_id,
    ABT_pool pool,
    const char *pool_name,
    bake_provider_t* provider);

#ifdef __cplusplus
}
#endif

#endif /* __BAKE_SERVER_H */
