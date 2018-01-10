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

/**
 * Creates a BAKE pool to use for backend PMEM storage.
 * 
 * NOTE: This function must be called on a pool before the pool
 * can be passed to 'bake_server_init'.
 *
 * @param[in] pool_name path to PMEM backend file
 * @param[in] pool_size size of the created pool
 * @param[in] pool_mode mode of the created pool
 * @returns 0 on success, -1 otherwise
 */
int bake_server_makepool(
    const char *pool_name,
    size_t pool_size,
    mode_t pool_mode);

/**
 * Initializes a BAKE server instance.
 *
 * @param[in] mid Margo instance identifier
 * @param[in] pool_name path to PMEM backend file
 * @returns 0 on success, -1 otherwise
 */
int bake_server_init(
    margo_instance_id mid,
    const char *pool_name);

#ifdef __cplusplus
}
#endif

#endif /* __BAKE_SERVER_H */
