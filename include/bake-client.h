/*
 * (C) 2016 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#ifndef __BAKE_CLIENT_H
#define __BAKE_CLIENT_H

#include <stdint.h>
#include "margo.h"
#include "bake.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Obtains identifying information for a BAKE target through the provided
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
 * Creates a bounded-size BAKE data region.  The resulting region can be
 * written using BAKE write operations, and can be persisted (once writes
 * are complete) with a a BAKE persist operation.  The region is not valid
 * for read access until persisted.
 *
 * @param [in] bti BAKE target identifier
 * @param [in] region_size size of region to be created
 * @param [out] rid identifier for new region
 * @returns 0 on success, -1 on failure
 */
int bake_create(
    bake_target_id_t bti,
    uint64_t region_size,
    bake_region_id_t *rid);
 
/**
 * Writes into a BAKE region that was previously created with bake_create().
 * Result is not guaranteed to be persistent until explicit
 * bake_persist() call.
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
int bake_write(
    bake_target_id_t bti,
    bake_region_id_t rid,
    uint64_t region_offset,
    void const *buf,
    uint64_t buf_size);

/**
 * Writes data into a previously created BAKE region like bake_write(),
 * except the write is performed on behalf of some remote entity.
 *
 * @param [in] bti BAKE target identifier
 * @param [in] rid identifier for region
 * @param [in] region_offset offset into the target region to write
 * @param [in] remote_bulk bulk_handle for remote data region to write from
 * @param [in] remote_offset offset in the remote bulk handle to write from
 * @param [in] remote_addr address string of the remote target to write from
 * @param [in] size size to write from remote bulk handle
 * @returns 0 on success, -1 on failure
 */
int bake_proxy_write(
    bake_target_id_t bti,
    bake_region_id_t rid,
    uint64_t region_offset,
    hg_bulk_t remote_bulk,
    uint64_t remote_offset,
    const char* remote_addr,
    uint64_t size);

/**
 * Persists a BAKE region. The region is considered immutable at this point 
 * and reads may be performed on the region.
 *
 * @param [in] bti BAKE target identifier
 * @param [in] rid identifier for region
 * @returns 0 on success, -1 on failure
 */
int bake_persist(
    bake_target_id_t bti,
    bake_region_id_t rid);

/**
 * Creates a bounded-size BAKE region, writes data into it, and persists
 * the reason all in one call/RPC (and thus 1 RTT).
 *
 * @param [in] bti BAKE target identifier
 * @param [in] region_size size of region to be created
 * @param [in] region_offset offset into the target region to write
 * @param [in] buf local memory buffer to write
 * @param [in] buf_size size of local memory buffer to write
 * @param [out] rid identifier for new region
 * @returns 0 on success, -1 on failure
 */
int bake_create_write_persist(
    bake_target_id_t bti,
    uint64_t region_size,
    uint64_t region_offset,
    void const *buf,
    uint64_t buf_size,
    bake_region_id_t *rid);

/**
 * Checks the size of an existing BAKE region. 
 *
 * @param [in] bti BAKE target identifier
 * @param [in] rid identifier for region
 * @param [out] size size of region
 * @returns 0 on success, -1 on failure
 */
int bake_get_size(
    bake_target_id_t bti,
    bake_region_id_t rid,
    uint64_t *size);

/**
 * Reads from a BAKE region that was previously persisted with bake_persist().
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
int bake_read(
    bake_target_id_t bti,
    bake_region_id_t rid,
    uint64_t region_offset,
    void *buf,
    uint64_t buf_size);

/**
 * Reads data from a previously persisted BAKE region like bake_read(),
 * except the read is performed on behalf of some remote entity.
 *
 * @param [in] bti BAKE target identifier
 * @param [in] rid identifier for region
 * @param [in] region_offset offset into the target region to write
 * @param [in] remote_bulk bulk_handle for remote data region to read to
 * @param [in] remote_offset offset in the remote bulk handle to read to
 * @param [in] remote_addr address string of the remote target to read to
 * @param [in] size size to read to remote bulk handle
 * @returns 0 on success, -1 on failure
 */
int bake_proxy_read(
    bake_target_id_t bti,
    bake_region_id_t rid,
    uint64_t region_offset,
    hg_bulk_t remote_bulk,
    uint64_t remote_offset,
    const char* remote_addr,
    uint64_t size);

/**
 * Releases local resources associated with access to a BAKE target;
 * does not modify the target in any way.
 *
 * @param [in] bti BAKE target_identifier
 */
void bake_release_instance(
    bake_target_id_t bti);

/**
 * Shuts down a remote BAKE service (given a target ID).
 *
 * @param [in] bti BAKE target identifier
 * @returns 0 on success, -1 on fialure 
 */
int bake_shutdown_service(
    bake_target_id_t bti);

/**
 * Issues a BAKE no-op operation.
 *
 * @param [in] bti BAKE target identifier
 * @returns 0 on success, -1 on failure
 */
int bake_noop(
    bake_target_id_t bti);

#ifdef __cplusplus
}
#endif

#endif /* __BAKE_CLIENT_H */
