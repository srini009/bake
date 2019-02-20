/*
 * (C) 2019 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#include "bake-config.h"
#include "bake.h"
#include <stdio.h>

static char * bake_err_str(int ret)
{
    switch(ret) {
        case BAKE_SUCCESS:
            return "Success";
            break;
        case BAKE_ERR_ALLOCATION:
            return "Error allocating something";
            break;
        case BAKE_ERR_INVALID_ARG:
            return "An argument is invalid";
            break;
        case BAKE_ERR_MERCURY:
            return "An error happend calling a Mercury function";
            break;
        case BAKE_ERR_ARGOBOTS:
            return "An error happened calling an Argobots function";
            break;
        case BAKE_ERR_PMEM:
            return "An error happend calling a PMDK function";
            break;
        case BAKE_ERR_UNKNOWN_TARGET:
            return "Target refered to by id is not know to provider";
            break;
        case BAKE_ERR_UNKNOWN_PROVIDER:
            return "Provider id could not be matched with a provider";
            break;
        case BAKE_ERR_UNKNOWN_REGION:
            return "Region id could not be found";
            break;
        case BAKE_ERR_OUT_OF_BOUNDS:
            return "Attempting an out of bound access";
            break;
        case BAKE_ERR_REMI:
            return "Error related to REMI";
            break;
        case BAKE_ERR_OP_UNSUPPORTED:
            return "Operation not supported";
            break;
        default:
            return "Unknown error";
            break;
    }
}
void bake_perror(char *s, int err)
{
    char error_string[256];
    char *p;
    int ret;
    ret = snprintf(error_string, 256,"%s", s);
    p = error_string+ret;
    snprintf(p, 256-ret, " (%d) %s", err, bake_err_str(err) );
    error_string[255] = '\0';
    fprintf(stderr, "%s\n", error_string);
}
