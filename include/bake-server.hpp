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

/**
 * @brief Creates a pool at a given path, with a given size and mode.
 *
 * @param pool_name Pool name.
 * @param pool_size Pool size.
 * @param pool_mode Mode.
 */
inline void makepool(
        const std::string& pool_name,
        size_t pool_size,
        mode_t pool_mode) {
    int ret = bake_makepool(pool_name.c_str(), pool_size, pool_mode);
    _CHECK_RET(ret);
}

/**
 * @brief The provider class is the C++ equivalent to a bake_provider_t.
 */
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

    /**
     * @brief Factory method to create an instance of provider.
     *
     * @param mid Margo instance id.
     * @param provider_id Provider id.
     * @param pool Argobots pool.
     *
     * @return Pointer to newly created provider.
     */
    static provider* create(margo_instance_id mid,
                            uint16_t provider_id = 0,
                            ABT_pool pool = BAKE_ABT_POOL_DEFAULT) {
        auto p = new provider(mid, provider_id, pool);
        margo_provider_push_finalize_callback(mid, p, &finalize_callback, p);
        return p;
    }

    /**
     * @brief Deleted copy constructor.
     */
    provider(const provider&) = delete;

    /**
     * @brief Deleted move constructor.
     */
    provider(provider&& other) = delete;

    /**
     * @brief Deleted copy-assignment operator.
     */
    provider& operator=(const provider&) = delete;

    /**
     * @brief Deleted move-assignment operator.
     */
    provider& operator=(provider&& other) = delete;

    /**
     * @brief Destructor.
     */
    ~provider() {
        margo_provider_pop_finalize_callback(m_mid, this);
        bake_provider_destroy(m_provider);
    }

    /**
     * @brief Adds a storage target to the provider.
     * The target must have been created beforehand.
     *
     * @param target_name Path to the target.
     *
     * @return a target object.
     */
    target add_storage_target(const std::string& target_name) {
        target t;
        int ret = bake_provider_add_storage_target(
                m_provider,
                target_name.c_str(),
                &(t.m_tid));
        _CHECK_RET(ret);
        return t;
    }

    /**
     * @brief Removes the storage target from the provider.
     * This does not removes the storage target from the device, it
     * simply makes it unaccessible through this provider.
     *
     * @param t target to remove.
     */
    void remove_storage_target(const target& t) {
        int ret = bake_provider_remove_storage_target(m_provider, t.m_tid);
        _CHECK_RET(ret);
    }

    /**
     * @brief Removes all the storage targets managed by the provider.
     */
    void remove_all_storage_targets() {
        int ret = bake_provider_remove_all_storage_targets(m_provider);
        _CHECK_RET(ret);
    }

    /**
     * @brief Count the number of storage targets managed by the provider.
     *
     * @return number of storage targets.
     */
    uint64_t count_storage_targets() const {
        uint64_t count;
        int ret = bake_provider_count_storage_targets(m_provider, &count);
        _CHECK_RET(ret);
        return count;
    }

    /**
     * @brief Lists all the storage targets managed by the provider.
     *
     * @return Vector of targets.
     */
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

    void set_config(const std::string& key, const std::string& value) {
        int ret = bake_provider_set_conf(m_provider, key.c_str(), value.c_str());
        _CHECK_RET(ret);
    }
};

}

#undef _CHECK_RET

#endif
