/*
 * (C) 2019 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __BAKE_CLIENT_HPP
#define __BAKE_CLIENT_HPP

#include <string>
#include <vector>
#include <bake.hpp>
#include <bake-client.h>

namespace bake {

#define _CHECK_RET(__ret) \
    if(__ret != BAKE_SUCCESS) throw exception(__ret)

class provider_handle;

class client {

    bake_client_t m_client = BAKE_CLIENT_NULL;
    bool          m_owns_client = true;

    friend class provider_handle;

    public:

    client() = default;

    client(margo_instance_id mid) {
        int ret = bake_client_init(mid, &m_client);
        _CHECK_RET(ret);
    }

    client(bake_client_t c, bool transfer_ownership=false)
    : m_client(c)
    , m_owns_client(transfer_ownership) {}

    client(const client&) = delete;

    client(client&& c)
    : m_client(c.m_client)
    , m_owns_client(c.m_owns_client) {
        c.m_client = BAKE_CLIENT_NULL;
        c.m_owns_client = false;
    }

    client& operator=(const client&) = delete;

    client& operator=(client&& c) {
        if(&c == this) return *this;
        if(m_client != BAKE_CLIENT_NULL && m_owns_client) {
            int ret = bake_client_finalize(m_client);
            _CHECK_RET(ret);
        }
        m_client = c.m_client;
        m_owns_client = c.m_owns_client;
        c.m_client = BAKE_CLIENT_NULL;
        c.m_owns_client = false;
        return *this;
    }

    ~client() {
        if(m_client != BAKE_CLIENT_NULL && m_owns_client)
            bake_client_finalize(m_client);
    }

    operator bool() const {
        return m_client != BAKE_CLIENT_NULL;
    }

    std::vector<target> probe(
            const provider_handle& ph,
            uint64_t max_targets=0) const;

    region create(
            const provider_handle& ph,
            const target& tgt,
            uint64_t region_size) const;

    void write(
            const provider_handle& ph,
            const region& rid,
            uint64_t region_offset,
            void const *buf,
            uint64_t buf_size) const;

    void write(
            const provider_handle& ph,
            const region& rid,
            uint64_t region_offset,
            hg_bulk_t remote_bulk,
            uint64_t remote_offset,
            const std::string& remote_addr,
            uint64_t size) const;

    void persist(
            const provider_handle& ph,
            const region& rid,
            uint64_t offset,
            size_t size) const;

    region create_write_persist(
            const provider_handle& ph,
            const target& tid,
            void const *buf,
            size_t buf_size) const;

    region create_write_persist(
            const provider_handle& ph,
            const target& tid,
            hg_bulk_t remote_bulk,
            uint64_t remote_offset,
            const std::string& remote_addr,
            size_t size) const;

    size_t get_size(
            const provider_handle& ph,
            const region& rid) const;

    void* get_data(
            const provider_handle& ph,
            const region& rid) const;

    size_t read(
            const provider_handle& ph,
            const region& rid,
            uint64_t region_offset,
            void* buffer,
            size_t buf_size) const;

    size_t read(
            const provider_handle& ph,
            const region& rid,
            uint64_t region_offset,
            hg_bulk_t remote_bulk,
            uint64_t remote_offset,
            const std::string& remote_addr,
            size_t size) const;

    region migrate(
            const provider_handle& soure,
            const region& source_rid,
            size_t region_size,
            bool remove_source,
            const std::string& dest_addr,
            uint16_t dest_provider_id,
            const target& dest_tid) const;
            
    void migrate(
            const provider_handle& soure,
            const target& tid,
            bool remove_source,
            const std::string& dest_addr,
            uint16_t dest_provider_id,
            const std::string& dest_root) const;

    void remove(
            const provider_handle& ph,
            const region& rid) const;

    void shutdown(hg_addr_t addr) const {
        int ret = bake_shutdown_service(m_client, addr);
        _CHECK_RET(ret);
    }
};

class provider_handle {

    friend class client;

    bake_provider_handle_t m_ph = BAKE_PROVIDER_HANDLE_NULL;

    public:

    provider_handle() = default;

    provider_handle(
            const client& clt,
            hg_addr_t addr,
            uint16_t provider_id=0) {
        int ret = bake_provider_handle_create(clt.m_client,
                addr, provider_id, &m_ph);
        _CHECK_RET(ret);
    }

    provider_handle(const provider_handle& other) 
    : m_ph(other.m_ph) {
        if(m_ph != BAKE_PROVIDER_HANDLE_NULL) {
            int ret = bake_provider_handle_ref_incr(m_ph);
            _CHECK_RET(ret);
        }
    }

    provider_handle(provider_handle&& other)
    : m_ph(other.m_ph) {
        other.m_ph = BAKE_PROVIDER_HANDLE_NULL;
    }

    provider_handle& operator=(const provider_handle& other) {
        if(&other == this) return *this;
        if(m_ph != BAKE_PROVIDER_HANDLE_NULL) {
            int ret = bake_provider_handle_release(m_ph);
            _CHECK_RET(ret);
        }
        m_ph = other.m_ph;
        int ret = bake_provider_handle_ref_incr(m_ph);
        _CHECK_RET(ret);
        return *this;
    }

    provider_handle& operator=(provider_handle& other) {
        if(&other == this) return *this;
        if(m_ph != BAKE_PROVIDER_HANDLE_NULL) {
            int ret = bake_provider_handle_release(m_ph);
            _CHECK_RET(ret);
        }
        m_ph = other.m_ph;
        other.m_ph = BAKE_PROVIDER_HANDLE_NULL;
        return *this;
    }

    ~provider_handle() {
        if(m_ph != BAKE_PROVIDER_HANDLE_NULL)
            bake_provider_handle_release(m_ph);
    }

    uint64_t get_eager_limit() const {
        uint64_t result;
        int ret = bake_provider_handle_get_eager_limit(m_ph, &result);
        _CHECK_RET(ret);
        return result;
    }

    void set_eager_limit(uint64_t limit) {
        int ret = bake_provider_handle_set_eager_limit(m_ph, limit);
        _CHECK_RET(ret);
    }
};

inline std::vector<target> client::probe(
        const provider_handle& ph,
        uint64_t max_targets) const {
    std::vector<bake_target_id_t> tids(max_targets);
    uint64_t num_tgts;
    int ret;
    if(max_targets != 0) {
        ret = bake_probe(ph.m_ph, max_targets, tids.data(), &num_tgts);
        _CHECK_RET(ret);
        tids.resize(num_tgts);
    } else {
        tids.resize(1);
        max_targets = 1;
        while(true) {
            bake_probe(ph.m_ph, max_targets, tids.data(), &num_tgts);
            _CHECK_RET(ret);
            if(num_tgts <  max_targets)
                break;
            max_targets *= 2;
            tids.resize(max_targets);
        }
    }
    std::vector<target> result(num_tgts);
    for(unsigned i=0; i < num_tgts; i++) {
        result[i].m_tid = tids[i];
    }
    return result;
}

inline region client::create(
            const provider_handle& ph,
            const target& tgt,
            uint64_t region_size) const {
    region r;
    int ret = bake_create(ph.m_ph, tgt.m_tid, region_size, &(r.m_rid));
    _CHECK_RET(ret);
    return r;
}

inline void client::write(
            const provider_handle& ph,
            const region& rid,
            uint64_t region_offset,
            void const *buf,
            uint64_t buf_size) const {
    int ret = bake_write(ph.m_ph, rid.m_rid, region_offset, buf, buf_size);
    _CHECK_RET(ret);
}

inline void client::write(
            const provider_handle& ph,
            const region& rid,
            uint64_t region_offset,
            hg_bulk_t remote_bulk,
            uint64_t remote_offset,
            const std::string& remote_addr,
            uint64_t size) const {
    int ret = bake_proxy_write(
            ph.m_ph,
            rid.m_rid,
            region_offset,
            remote_bulk,
            remote_offset,
            remote_addr.c_str(),
            size);
    _CHECK_RET(ret);
}

inline void client::persist(
            const provider_handle& ph,
            const region& rid,
            uint64_t offset,
            size_t size) const {
    int ret = bake_persist(
            ph.m_ph,
            rid.m_rid,
            offset,
            size);
    _CHECK_RET(ret);
}

inline region client::create_write_persist(
            const provider_handle& ph,
            const target& tid,
            void const *buf,
            size_t buf_size) const {
    region r;
    int ret = bake_create_write_persist(
            ph.m_ph,
            tid.m_tid,
            buf,
            buf_size,
            &r.m_rid);
    _CHECK_RET(ret);
    return r;
}

inline region client::create_write_persist(
            const provider_handle& ph,
            const target& tid,
            hg_bulk_t remote_bulk,
            uint64_t remote_offset,
            const std::string& remote_addr,
            size_t size) const {
    region r;
    int ret = bake_create_write_persist_proxy(
            ph.m_ph,
            tid.m_tid,
            remote_bulk,
            remote_offset,
            remote_addr.c_str(),
            size,
            &(r.m_rid));
    _CHECK_RET(ret);
    return r;
}

inline size_t client::get_size(
            const provider_handle& ph,
            const region& rid) const {
    size_t size;
    int ret = bake_get_size(ph.m_ph, rid.m_rid, &size);
    _CHECK_RET(ret);
    return size;
}

inline void* client::get_data(
            const provider_handle& ph,
            const region& rid) const {
    void* ptr;
    int ret = bake_get_data(ph.m_ph, rid.m_rid, &ptr);
    _CHECK_RET(ret);
    return ptr;
}

inline size_t client::read(
            const provider_handle& ph,
            const region& rid,
            uint64_t region_offset,
            void* buffer,
            size_t buf_size) const {
    size_t bytes_read;
    int ret = bake_read(
            ph.m_ph,
            rid.m_rid,
            region_offset,
            buffer,
            buf_size,
            &bytes_read);
    _CHECK_RET(ret);
    return bytes_read;
}

inline size_t client::read(
            const provider_handle& ph,
            const region& rid,
            uint64_t region_offset,
            hg_bulk_t remote_bulk,
            uint64_t remote_offset,
            const std::string& remote_addr,
            size_t size) const {
    size_t bytes_read;
    int ret = bake_proxy_read(
            ph.m_ph,
            rid.m_rid,
            region_offset,
            remote_bulk,
            remote_offset,
            remote_addr.c_str(),
            size,
            &bytes_read);
    _CHECK_RET(ret);
    return bytes_read;
}

inline region client::migrate(
            const provider_handle& source,
            const region& source_rid,
            size_t region_size,
            bool remove_source,
            const std::string& dest_addr,
            uint16_t dest_provider_id,
            const target& dest_tid) const {
    region r;
    int ret = bake_migrate_region(source.m_ph,
            source_rid.m_rid,
            region_size,
            remove_source,
            dest_addr.c_str(),
            dest_provider_id,
            dest_tid.m_tid,
            &(r.m_rid));
    _CHECK_RET(ret);
    return r;
}
            
inline void client::migrate(
            const provider_handle& source,
            const target& tid,
            bool remove_source,
            const std::string& dest_addr,
            uint16_t dest_provider_id,
            const std::string& dest_root) const {
    int ret = bake_migrate_target(
            source.m_ph,
            tid.m_tid,
            remove_source,
            dest_addr.c_str(),
            dest_provider_id,
            dest_root.c_str());
    _CHECK_RET(ret);
}

inline void client::remove(
            const provider_handle& ph,
            const region& rid) const {
    int ret = bake_remove(ph.m_ph, rid.m_rid);
    _CHECK_RET(ret);
}

}

#undef _CHECK_RET

#endif
