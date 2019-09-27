/*
 * (C) 2019 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __BAKE_SERVER_HPP
#define __BAKE_SERVER_HPP

#include <string>
#include <vector>
#include <bake.hpp>
#include <bake-server.h>

#define _CHECK_RET(__ret) \
        if(__ret != BAKE_SUCCESS) throw exception(__ret)

namespace bake {

inline void makepool(
        const std::string& pool_name,
        size_t pool_size,
        mode_t pool_mode) {
    int ret = bake_makepool(pool_name.c_str(), pool_size, pool_mode);
    _CHECK_RET(ret);
}

class provider {

    margo_instance_id m_mid = MARGO_INSTANCE_NULL;
    bake_provider_t m_provider = NULL;

    provider(
            margo_instance_id mid,
            uint16_t provider_id = 0,
            ABT_pool pool = ABT_POOL_NULL)
    : m_mid(mid) {
        int ret = bake_provider_register(mid, provider_id, pool, &m_provider);
        _CHECK_RET(ret);

    }

    static void finalize_callback(void* args) {
        auto* p = static_cast<provider*>(args);
        delete p;
    }

    public:

    static provider* create(margo_instance_id mid,
                            uint16_t provider_id = 0,
                            ABT_pool pool = BAKE_ABT_POOL_DEFAULT) {
        auto p = new provider(mid, provider_id, pool);
        margo_provider_push_finalize_callback(mid, p, &finalize_callback, p);
        return p;
    }

    provider(const provider&) = delete;
    provider(provider&& other) = delete;
    provider& operator=(const provider&) = delete;
    provider& operator=(provider&& other) = delete;

    ~provider() {
        margo_provider_pop_finalize_callback(m_mid, this);
        bake_provider_destroy(m_provider);
    }

    target add_storage_target(const std::string& target_name) {
        target t;
        int ret = bake_provider_add_storage_target(
                m_provider,
                target_name.c_str(),
                &(t.m_tid));
        _CHECK_RET(ret);
        return t;
    }

    void remove_storage_target(const target& t) {
        int ret = bake_provider_remove_storage_target(m_provider, t.m_tid);
        _CHECK_RET(ret);
    }

    void remove_all_storage_targets() {
        int ret = bake_provider_remove_all_storage_targets(m_provider);
        _CHECK_RET(ret);
    }

    uint64_t count_storage_targets() const {
        uint64_t count;
        int ret = bake_provider_count_storage_targets(m_provider, &count);
        _CHECK_RET(ret);
        return count;
    }

    std::vector<target> list_storage_targets() const {
        uint64_t count = count_storage_targets();
        std::vector<target> result(count);
        std::vector<bake_target_id_t> tgts(count);
        int ret = bake_provider_list_storage_targets(m_provider, tgts.data());
        _CHECK_RET(ret);
        for(unsigned i=0; i < count; i++) {
            result[i].m_tid = tgts[i];
        }
        return result;
    }
};

}

#undef _CHECK_RET

#endif
