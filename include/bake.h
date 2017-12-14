/*
 * (C) 2016 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#ifndef __BAKE_H
#define __BAKE_H

#include <uuid/uuid.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Persistent, universal, opaque identifier for a BAKE target.
 * Remains constant if instance is opened, closed, or migrated.
 */
typedef struct {
    uuid_t id;
} bake_target_id_t;

/**
 * Persistent, opaque identifier for a region within a BAKE target.
 */
#define BAKE_REGION_ID_DATA_SIZE 24
typedef struct {
    uint32_t type;
    char data[BAKE_REGION_ID_DATA_SIZE];
} bake_region_id_t;

#ifdef __cplusplus
}
#endif

#endif /* __BAKE_H */
