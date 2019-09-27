/*
 * (C) 2019 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __BAKE_HPP
#define __BAKE_HPP

#include <exception>
#include <string>
#include <bake.h>

namespace bake {

const char* const bake_error_messages[] = {
    "",
    "Allocation error",
    "Invalid argument",
    "Mercury error",
    "Argobots error",
    "PMEM error",
    "Unknown target",
    "Unknown provider",
    "Unknown region",
    "Access out of bound",
    "REMI error",
    "Operation not supported"
};

class client;
class provider;

/**
 * @brief target instances are equivalent to bake_target_id_t.
 */
class target {

    friend class client;
    friend class provider;

    bake_target_id_t m_tid;

    public:

    target()                         = default;
    target(const std::string& str) {
        bake_target_id_from_string(str.c_str(), &m_tid);
    }
    target(const target&)            = default;
    target(target&&)                 = default;
    target& operator=(const target&) = default;
    target& operator=(target&&)      = default;
    ~target()                        = default;

    operator std::string() const {
        char str[37];
        bake_target_id_to_string(m_tid, str, 37);
        return std::string(str);
    }

    template<typename A>
    void save(A& ar) const {
        ar.write(&m_tid);
    }

    template<typename A>
    void load(A& ar) {
        ar.read(&m_tid);
    }
};

class region {

    friend class client;

    bake_region_id_t m_rid;

    public:

    region() = default;
    region(const std::string& str) {
        bake_region_id_from_string(str.c_str(), &m_rid);
    }
    region(const region&)            = default;
    region(region&&)                 = default;
    region& operator=(const region&) = default;
    region& operator=(region&&)      = default;
    ~region()                        = default;

    operator std::string() const {
        char str[128];
        bake_region_id_to_string(m_rid, str, 128);
        return std::string(str);
    }

    template<typename A>
    void save(A& ar) const {
        ar.write(&m_rid);
    }

    template<typename A>
    void load(A& ar) {
        ar.read(&m_rid);
    }
};

class exception : public std::exception {

    int m_error;
    std::string m_msg;

    public:

    exception(int error)
    : m_error(error) {
        if(error < 0 && error > BAKE_ERR_END) {
            m_msg = std::string("[BAKE] ") + bake_error_messages[-error];
        } else {
            m_msg = std::string("[BAKE] Unknown error code "+std::to_string(error));
        }
    }

    const char* what() const noexcept override {
        return m_msg.c_str();
    }

    int error() const {
        return m_error;
    }
};

}

#endif
