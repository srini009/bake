/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <margo.h>
#include <libpmemobj.h>
#include <bake-bulk-server.h>

struct options
{
    char *listen_addr;
    char *pmem_pool;
    char *host_file;
};

static void usage(int argc, char **argv)
{
    fprintf(stderr, "Usage: bake-bulk-server-daemon [OPTIONS] <listen_addr> <pmem_pool>\n");
    fprintf(stderr, "       listen_addr is the Mercury address to listen on\n");
    fprintf(stderr, "       pmem_pool is the path to the pmemobj pool\n");
    fprintf(stderr, "       [-f filename] to write the server address to a file\n");
    fprintf(stderr, "Example: ./bake-bulk-server-daemon tcp://localhost:1234 /dev/shm/foo.dat\n");
    return;
}

static void parse_args(int argc, char **argv, struct options *opts)
{
    int opt;

    memset(opts, 0, sizeof(*opts));

    /* get options */
    while((opt = getopt(argc, argv, "f:")) != -1)
    {
        switch(opt)
        {
            case 'f':
                opts->host_file = optarg;
                break;
            default:
                usage(argc, argv);
                exit(EXIT_FAILURE);
        }
    }

    /* get required arguments after options */
    if((argc - optind) != 2)
    {
        usage(argc, argv);
        exit(EXIT_FAILURE);
    }
    opts->listen_addr = argv[optind++];
    opts->pmem_pool = argv[optind++];

    return;
}

int main(int argc, char **argv) 
{
    struct options opts;
    margo_instance_id mid;
    int ret;

    parse_args(argc, argv, &opts);

    /* start margo */
    /* use the main xstream for driving progress and executing rpc handlers */
    mid = margo_init(opts.listen_addr, MARGO_SERVER_MODE, 0, -1);
    if(mid == MARGO_INSTANCE_NULL)
    {
        fprintf(stderr, "Error: margo_init()\n");
        return(-1);
    }

    if(opts.host_file)
    {
        /* write the server address to file if requested */
        FILE *fp;
        hg_addr_t self_addr;
        char self_addr_str[128];
        hg_size_t self_addr_str_sz = 128;
        hg_return_t hret;

        /* figure out what address this server is listening on */
        hret = margo_addr_self(mid, &self_addr);
        if(hret != HG_SUCCESS)
        {
            fprintf(stderr, "Error: margo_addr_self()\n");
            margo_finalize(mid);
            return(-1);
        }
        hret = margo_addr_to_string(mid, self_addr_str, &self_addr_str_sz, self_addr);
        if(hret != HG_SUCCESS)
        {
            fprintf(stderr, "Error: margo_addr_to_string()\n");
            margo_addr_free(mid, self_addr);
            margo_finalize(mid);
            return(-1);
        }
        margo_addr_free(mid, self_addr);

        fp = fopen(opts.host_file, "w");
        if(!fp)
        {
            perror("fopen");
            margo_finalize(mid);
            return(-1);
        }

        fprintf(fp, "%s", self_addr_str);
        fclose(fp);
    }

    /* register the bake bulk server */
    ret = bake_server_init(mid, opts.pmem_pool);
    if(ret != 0)
    {
        fprintf(stderr, "Error: bake_server_init()\n");
        margo_finalize(mid);
        return(-1);
    }

    /* suspend until the bake server gets a shutdown signal from the client */
    bake_server_wait_for_shutdown();
    margo_finalize(mid);

    return(0);
}

