/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <assert.h>
#include "bake-bulk-rpc.h"

/* service a remote RPC that instructs the server daemon to shut down */
static void bake_bulk_shutdown_ult(void *_arg)
{
    hg_handle_t *handle = _arg;

    hg_return_t hret;
    struct hg_info *hgi;
    margo_instance_id mid;

    printf("Got RPC request to shutdown\n");

    hgi = HG_Get_info(*handle);
    assert(hgi);
    mid = margo_hg_class_to_instance(hgi->hg_class);

    hret = margo_respond(mid, *handle, NULL);
    assert(hret == HG_SUCCESS);

    HG_Destroy(*handle);

    /* NOTE: we assume that the server daemon is using
     * margo_wait_for_finalize() to suspend until this RPC executes, so there
     * is no need to send any extra signal to notify it.
     */
    margo_finalize(mid);

    return;
}
DEFINE_MARGO_RPC_HANDLER(bake_bulk_shutdown_ult)
