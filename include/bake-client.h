/*
 * (C) 2016 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#ifndef __BAKE_CLIENT_H
#define __BAKE_CLIENT_H

#include <stdint.h>
#include <margo.h>
#include <bake.h>

#ifdef __cplusplus
extern "C" {
#endif

#define BAKE_CLIENT_NULL ((bake_client_t)NULL)
#define BAKE_PROVIDER_HANDLE_NULL ((bake_provider_handle_t)NULL)

typedef struct bake_client* bake_client_t;
typedef struct bake_provider_handle* bake_provider_handle_t;

/**
 * Creates a BAKE client attached to the given margo instance.
 * This will effectively register the RPC needed by BAKE into
 * the margo instance. The client must be freed with
 * bake_client_finalize.
 *
 * @param[in] mid margo instance
 * @param[out] client resulting bake client object
 *
 * @return 0 on success, -1 on failure
 */
int bake_client_init(margo_instance_id mid, bake_client_t* client);

/**
 * Finalizes a BAKE client.
 * WARNING: This function must not be called after Margo has been
 * finalized. If you need to finalize a BAKE client when Margo is
 * finalized, use margo_push_finalize_callback.
 *
 * @param client BAKE client to destroy
 *
 * @return 0 on success, -1 on failure
 */
int bake_client_finalize(bake_client_t client);

/**
 * Creates a provider handle to point to a particular BAKE provider.
 *
 * @param client client managing the provider handle
 * @param addr address of the provider
 * @param provider_id id of the provider
 * @param handle resulting handle
 *
 * @return 0 on success, -1 on failure
 */
int bake_provider_handle_create(
        bake_client_t client,
        hg_addr_t addr,
        uint16_t provider_id,
        bake_provider_handle_t* handle);

/**
 * Increment the reference counter of the provider handle
 *
 * @param handle provider handle
 *
 * @return 0 on success, -1 on failure
 */
int bake_provider_handle_ref_incr(bake_provider_handle_t handle);

/**
 * Get the limit (in bytes) bellow which this provider handle will use
 * eager mode (i.e. packing data into the RPC instead of using RDMA). 
 *
 * @param[in] handle provider handle
 * @param[out] limit limit
 *
 * @return 0 on success, -1 on failure
 */
int bake_provider_handle_get_eager_limit(bake_provider_handle_t handle, uint64_t* limit);

/**
 * Set the limit (in bytes) bellow which this provider handle will use
 * eager mode (i.e. packing data into the RPC instead of using RDMA).
 *
 * @param[in] handle provider handle
 * @param[in] limit limit
 *
 * @return 0 on success, -1 on failure
 */
int bake_provider_handle_set_eager_limit(bake_provider_handle_t handle, uint64_t limit);

/**
 * Decrement the reference counter of the provider handle,
 * effectively freeing the provider handle when the reference count
 * is down to 0.
 *
 * @param handle provider handle
 *
 * @return 0 on success, -1 on failure
 */
int bake_provider_handle_release(bake_provider_handle_t handle);

/**
 * Obtains available BAKE targets from a give provider.
 * If bake_target_id_t is NULL, max_targets is ignored and the
 * function returns the number of targets available in num_targets.
 *
 * @param [in] provider provider handle
 * @param [in] max_targets maximum number of targets to retrieve
 * @param [out] bti array of BAKE target identifiers with enough space for max_targets
 * @param [out] num_targets number of targets returned (at most max_targets)
 * @returns 0 on success, -1 on failure
 */
int bake_probe(
        bake_provider_handle_t provider,
        uint64_t max_targets,
        bake_target_id_t* bti,
        uint64_t* num_targets);
  
/**
 * Creates a bounded-size BAKE data region. The resulting region can be
 * written using BAKE write operations, and can be persisted (once writes
 * are complete) with a a BAKE persist operation.  The region is not valid
 * for read access until persisted.
 *
 * @param [in] provider provider handle
 * @param [in] bti BAKE target identifier
 * @param [in] region_size size of region to be created
 * @param [out] rid identifier for new region
 * @returns 0 on success, -1 on failure
 */
int bake_create(
        bake_provider_handle_t provider,
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
 * @param [in] provider provider handle
 * @param [in] rid identifier for region
 * @param [in] region_offset offset into the target region to write
 * @param [in] buf local memory buffer to write
 * @param [in] buf_size size of local memory buffer to write
 * @returns 0 on success, -1 on failure
 */
int bake_write(
        bake_provider_handle_t provider,
        bake_region_id_t rid,
        uint64_t region_offset,
        void const *buf,
        uint64_t buf_size);

/**
 * Writes data into a previously created BAKE region like bake_write(),
 * except the write is performed on behalf of some remote entity.
 *
 * @param [in] provider provider handle
 * @param [in] rid identifier for region
 * @param [in] region_offset offset into the target region to write
 * @param [in] remote_bulk bulk_handle for remote data region to write from
 * @param [in] remote_offset offset in the remote bulk handle to write from
 * @param [in] remote_addr address string of the remote target to write from
 * @param [in] size size to write from remote bulk handle
 * @returns 0 on success, -1 on failure
 */
int bake_proxy_write(
        bake_provider_handle_t provider,
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
 * @param [in] provider provider handle
 * @param [in] rid identifier for region
 * @returns 0 on success, -1 on failure
 */
int bake_persist(
        bake_provider_handle_t provider,
        bake_region_id_t rid);

/**
 * Creates a bounded-size BAKE region, writes data into it, and persists
 * the reason all in one call/RPC (and thus 1 RTT).
 *
 * @param [in] provider provider handle
 * @param [in] bti BAKE target identifier
 * @param [in] region_size size of region to be created
 * @param [in] region_offset offset into the target region to write
 * @param [in] buf local memory buffer to write
 * @param [in] buf_size size of local memory buffer to write
 * @param [out] rid identifier for new region
 * @returns 0 on success, -1 on failure
 */
int bake_create_write_persist(
        bake_provider_handle_t provider,
        bake_target_id_t bti,
        uint64_t region_size,
        uint64_t region_offset,
        void const *buf,
        uint64_t buf_size,
        bake_region_id_t *rid);

/**
 *
 * @param [in] provider provider handle
 * @param [in] bti BAKE target identifier
 * @param [in] region_size size of region to be created
 * @param [in] region_offset offset into the target region to write
 * @param [in] remote_bulk bulk_handle for remote data region to write from
 * @param [in] remote_offset offset in the remote bulk handle to write from
 * @param [in] remote_addr address string of the remote target to write from
 * @param [in] size size to write from remote bulk handle
 * @param [out] rid identifier for new region
 * @returns 0 on success, -1 on failure
 */
int bake_create_write_persist_proxy(
        bake_provider_handle_t provider,
        bake_target_id_t bti,
        uint64_t region_size,
        uint64_t region_offset,
        hg_bulk_t remote_bulk,
        uint64_t remote_offset,
        const char* remote_addr,
        uint64_t size,
        bake_region_id_t *rid);

/**
 * Checks the size of an existing BAKE region. 
 *
 * @param [in] provider provider handle
 * @param [in] rid identifier for region
 * @param [out] size size of region
 * @returns 0 on success, -1 on failure
 */
int bake_get_size(
        bake_provider_handle_t provider,
        bake_region_id_t rid,
        uint64_t *size);

/**
 * Reads from a BAKE region that was previously persisted with bake_persist().
 *
 * NOTE: for now at least, this call does not support "short" reads.  It
 * either succeeds in reading the requested size or not.
 *
 * @param [in] provider provider handle
 * @param [in] rid region identifier
 * @param [in] region_offset offset into the target region to read from
 * @param [in] buf local memory buffer read into
 * @param [in] buf_size size of local memory buffer to read into
 * @returns 0 on success, -1 on failure
 */
int bake_read(
        bake_provider_handle_t provider,
        bake_region_id_t rid,
        uint64_t region_offset,
        void *buf,
        uint64_t buf_size);

/**
 * Reads data from a previously persisted BAKE region like bake_read(),
 * except the read is performed on behalf of some remote entity.
 *
 * @param [in] provider provider handle
 * @param [in] rid identifier for region
 * @param [in] region_offset offset into the target region to write
 * @param [in] remote_bulk bulk_handle for remote data region to read to
 * @param [in] remote_offset offset in the remote bulk handle to read to
 * @param [in] remote_addr address string of the remote target to read to
 * @param [in] size size to read to remote bulk handle
 * @returns 0 on success, -1 on failure
 */
int bake_proxy_read(
        bake_provider_handle_t provider,
        bake_region_id_t rid,
        uint64_t region_offset,
        hg_bulk_t remote_bulk,
        uint64_t remote_offset,
        const char* remote_addr,
        uint64_t size);

/**
 * Shuts down a remote BAKE service (given an address).
 * This will shutdown all the providers on the target address.
 * 
 * @param [in] client BAKE client
 * @param [in] addr address of the server 
 * @returns 0 on success, -1 on failure 
 */
int bake_shutdown_service(
        bake_client_t client, hg_addr_t addr);

/**
 * Issues a BAKE no-op operation.
 *
 * @param [in] provider provider handle
 * @returns 0 on success, -1 on failure
 */
int bake_noop(bake_provider_handle_t provider);

#ifdef __cplusplus
}
#endif

#endif /* __BAKE_CLIENT_H */
