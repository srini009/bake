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

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Creates a bake-bulk pool to use for backend PMEM storage.
 * NOTE: this function must be called on a pool before the pool
 *       can be passed to 'bake_server_init'
 *
 * @param[in] pool_name path to pmem backend file
 * @param[in] pool_size size of the created pool
 * @param[in] pool_mode mode of the created pool
 * @returns 0 on success, -1 otherwise
 */
int bake_server_makepool(
    const char *pool_name,
    size_t pool_size,
    mode_t pool_mode);

/**
 * Register a bake server instance for a given Margo instance.
 *
 * @param[in] mid Margo instance identifier
 * @param[in] pool_name path to pmem backend file
 * @returns 0 on success, -1 otherwise
 */
int bake_server_init(
    margo_instance_id mid,
    const char *pool_name);

/**
 * Shuts down a bake server and frees all associated resources.
 */
void bake_server_shutdown(void);

/**
 * Suspends the server process until some other entity calls bake_server_shutdown.
 */
void bake_server_wait_for_shutdown(void);

#ifdef __cplusplus
}
#endif
#endif /* __BAKE_BULK_SERVER_H */
