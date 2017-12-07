/*
 * (C) 2016 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#ifndef __BAKE_BULK_CLIENT_H
#define __BAKE_BULK_CLIENT_H

#include <stdint.h>
#include "margo.h"
#include "bake-bulk.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Obtain identifying information for a bake target through the provided
 * remote mercury address.
 *
 * @param [in] mid margo instance
 * @param [in] dest_addr destination Mercury address
 * @param [out] bti BAKE target identifier
 * @returns 0 on success, -1 on failure
 */
int bake_probe_instance(
    margo_instance_id mid,
    hg_addr_t dest_addr,
    bake_target_id_t *bti);
  
/**
 * Create a bounded-size bulk data region.  The resulting region can be
 * written using bulk write operations, and can be persisted (once writes are
 * complete) with a a bulk persist operation.  The region is not valid for
 * read access until persisted.
 *
 * @param [in] bti BAKE target identifier
 * @param [in] region_size size of region to be created
 * @param [out] rid identifier for new region
 * @returns 0 on success, -1 on failure
 */
int bake_bulk_create(
    bake_target_id_t bti,
    uint64_t region_size,
    bake_bulk_region_id_t *rid);
 
/**
 * Writes into a region that was previously created with bake_bulk_create().
 * Result is not guaranteed to be persistent until explicit
 * bake_bulk_persist() call.
 *
 * Results are undefined if multiple writers (from same process or different
 * processes) perform overlapping writes.
 *
 * @param [in] bti BAKE target identifier
 * @param [in] rid identifier for region
 * @param [in] region_offset offset into the target region to write
 * @param [in] buf local memory buffer to write
 * @param [in] buf_size size of local memory buffer to write
 * @returns 0 on success, -1 on failure
 */
int bake_bulk_write(
    bake_target_id_t bti,
    bake_bulk_region_id_t rid,
    uint64_t region_offset,
    void const *buf,
    uint64_t buf_size);

/**
 *
 */
int bake_bulk_proxy_write(
    bake_target_id_t bti,
    bake_bulk_region_id_t rid,
    uint64_t region_offset,
    hg_bulk_t remote_bulk,
    uint64_t remote_offset,
    hg_addr_t remote_addr,
    uint64_t size);

/**
 * Persist a bulk region. The region is considered immutable at this point 
 * and reads may be performed on the region.
 *
 * @param [in] bti BAKE target identifier
 * @param [in] rid identifier for region
 * @returns 0 on success, -1 on failure
 */
int bake_bulk_persist(
    bake_target_id_t bti,
    bake_bulk_region_id_t rid);
  
/**
 * Check the size of an existing region. 
 *
 * @param [in] bti BAKE target identifier
 * @param [in] rid identifier for region
 * @param [out] size sizes of region
 * @returns 0 on success, -1 on failure
 */
int bake_bulk_get_size(
    bake_target_id_t bti,
    bake_bulk_region_id_t rid,
    uint64_t *region_size);

/**
 * Reads from a region that was previously persisted with bake_bulk_persist().
 *
 * NOTE: for now at least, this call does not support "short" reads.  It
 * either succeeds in reading the requested size or not.
 *
 * @param [in] bti BAKE target identifier
 * @param [in] rid region identifier
 * @param [in] region_offset offset into the target region to read from
 * @param [in] buf local memory buffer read into
 * @param [in] buf_size size of local memory buffer to read into
 * @returns 0 on success, -1 on failure
 */
int bake_bulk_read(
    bake_target_id_t bti,
    bake_bulk_region_id_t rid,
    uint64_t region_offset,
    void *buf,
    uint64_t buf_size);

/**
 * Release local resources associated with access to a target; does not
 * modify the target in any way.
 *
 * @param [in] bti BAKE target_identifier
 */
void bake_release_instance(
    bake_target_id_t bti);

/**
 * Utility function to shut down a remote service
 *
 * @param [in] bti Bake target identifier
 * @returns 0 on success, -1 on fialure 
 */
int bake_shutdown_service(bake_target_id_t bti);

/* NOTE: code below is a copy of the bulk portion of the proposed BAKE API.
 * Commented out for now but leaving it in place for reference
 */

/**
 * Issue a no-op 
 *
 * @param [in] bti BAKE target identifier
 * @returns 0 on success, -1 on failure
 */
int bake_bulk_noop(
    bake_target_id_t bti);

#ifdef __cplusplus
}
#endif

#endif /* __BAKE_BULK__CLIENT_H */
