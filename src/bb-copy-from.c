/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>

#include "bake-bulk-client.h"

/* client program that will copy a bake bulk region out to a POSIX file */

int main(int argc, char **argv) 
{
    int i;
    char cli_addr_prefix[64] = {0};
    char *svr_addr_str;
    hg_addr_t svr_addr;
    margo_instance_id mid;
    bake_target_id_t bti;
    hg_return_t hret;
    int ret;
    bake_bulk_region_id_t rid;
    int fd;
    char* local_region;
    int region_fd;
    uint64_t check_size;
 
    if(argc != 4)
    {
        fprintf(stderr, "Usage: bb-copy-from <server addr> <identifier file> <output file>\n");
        fprintf(stderr, "  Example: ./bb-copy-from tcp://localhost:1234 /tmp/bb-copy-rid.0GjOlu /tmp/output.dat\n");
        return(-1);
    }
    svr_addr_str = argv[1];

    /* initialize Margo using the transport portion of the server
     * address (i.e., the part before the first : character if present)
     */
    for(i=0; (i<63 && svr_addr_str[i] != '\0' && svr_addr_str[i] != ':'); i++)
        cli_addr_prefix[i] = svr_addr_str[i];

    mid = margo_init(cli_addr_prefix, MARGO_CLIENT_MODE, 0, -1);
    if(mid == MARGO_INSTANCE_NULL)
    {
        fprintf(stderr, "Error: margo_init()\n");
        return -1;
    }

    hret = margo_addr_lookup(mid, svr_addr_str, &svr_addr);
    if(hret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: margo_addr_lookup()\n");
        margo_finalize(mid);
        return(-1);
    }

    ret = bake_probe_instance(mid, svr_addr, &bti);
    if(ret < 0)
    {
        fprintf(stderr, "Error: bake_probe_instance()\n");
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }

    region_fd = open(argv[2], O_RDONLY);
    if(region_fd < 0)
    {
        perror("open rid");
        bake_release_instance(bti);
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }

    ret = read(region_fd, &rid, sizeof(rid));
    if(ret != sizeof(rid))
    {
        perror("read");
        close(region_fd);
        bake_release_instance(bti);
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }
    close(region_fd);

    ret = bake_bulk_get_size(bti, rid, &check_size);
    if(ret != 0)
    {
        fprintf(stderr, "Error: bake_bulk_get_size()\n");
        bake_release_instance(bti);
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }

    fd = open(argv[3], O_RDWR|O_TRUNC|O_CREAT, S_IRUSR|S_IWUSR);
    if(fd < 0)
    {
        perror("open output");
        bake_release_instance(bti);
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }

    ret = ftruncate(fd, check_size);
    if(ret < 0)
    {
        perror("ftruncate");
        close(fd);
        bake_release_instance(bti);
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }

    local_region = mmap(NULL, check_size, PROT_WRITE, MAP_SHARED, fd, 0);
    if(local_region == MAP_FAILED)
    {
        perror("mmap");
        close(fd);
        bake_release_instance(bti);
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        return(-1);
    }

    /* transfer data */
    ret = bake_bulk_read(
        bti,
        rid,
        0,
        local_region,
        check_size);
    if(ret != 0)
    {
        munmap(local_region, check_size);
        close(fd);
        bake_release_instance(bti);
        margo_addr_free(mid, svr_addr);
        margo_finalize(mid);
        fprintf(stderr, "Error: bake_bulk_read()\n");
        return(-1);
    }

    munmap(local_region, check_size);
    close(fd);
    bake_release_instance(bti);
    margo_addr_free(mid, svr_addr);
    margo_finalize(mid);

    return(0);
}

