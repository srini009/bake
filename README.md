# BAKE

## Installation

BAKE can easily be installed using Spack:

`spack install bake`

This will install BAKE and its dependencies (margo, uuid, and libpmem,
as well as their respective dependencies). To compile and install BAKE manually,
please refer to the end of this document.

## Architecture

Like most Mochi services, BAKE relies on a client/provider architecture.
A provider, identified by its _address_ and _multiplex id_, manages one or more
_BAKE targets_, referenced externally by their _target id_.

## Setting up a BAKE target

BAKE requires the backend storage file to be created beforehand using
`bake-mkpool`. For instance:

`bake-mkpool -s 500M /dev/shm/foo.dat`

creates a 500 MB file at _/dev/shm/foo.dat_ to be used by BAKE as a target.

The bake-mkpool command creates a BAKE pool used to store raw data for
a particular BAKE target. This is essentially a wrapper command around
pmemobj utilities for creating an empty pool that additionally stores
some BAKE-specific metadata in the created pool. Pools used by BAKE
providers must be created using this command.

## Starting a daemon

BAKE ships with a default daemon program that can setup providers and attach
to storage targets. This daemon can be started as follows:

`bake-server-daemon [options] <listen_address> <bake_pool_1> <bake_pool_2> ...`

The program takes a set of options followed by an address at which to listen for
incoming RPCs, and a list of
BAKE targets already created using `bake-mkpool`.

For example:

`bake-server-daemon -f bake.addr -m providers bmi+tcp://localhost:1234 /dev/shm/foo.dat /dev/shm/bar.dat`

The following options are accepted:
* `-f` provides the name of the file in which to write the address of the daemon.
* `-m` provides the mode (_providers_ or _targets_).

The _providers_ mode indicates that, if multiple BAKE targets are used (as above),
these targets should be managed by multiple providers, accessible through 
different multiplex ids 1, 2, ... _N_ where _N_ is the number of storage targets
to manage. The _targets_ mode indicates that a single provider should be used to
manage all the storage targets.

## Client API example

```c
#include <bake-client.h>

int main(int argc, char **argv)
{
    char *svr_addr_str; // string address of the BAKE server
    hg_addr_t svr_addr; // Mercury address of the BAKE server
    margo_instance_id mid; // Margo instance id
    bake_client_t bcl; // BAKE client
    bake_provider_handle_t bph; // BAKE handle to provider
    uint8_t mplex_id; // multiplex id of the provider
    uint32_t target_number; // target to use
    bake_region_id_t rid; // BAKE region id handle
	bake_target_id_t* bti; // array of target ids 
	
	/* ... setup variables ... */
	
	/* Initialize Margo */
	mid = margo_init(..., MARGO_CLIENT_MODE, 0, -1);
	/* Lookup the server */
	margo_addr_lookup(mid, svr_addr_str, &svr_addr);
	/* Creates the BAKE client */
	bake_client_init(mid, &bcl);
	/* Creates the provider handle */
	bake_provider_handle_create(bcl, svr_addr, mplex_id, &bph);
	/* Asks the provider for up to target_number target ids */
	uint32_t num_targets = 0;
	bti = calloc(num_targets, sizeof(*bti));
	bake_probe(bph, target_number, bti, &num_targets);
	if(num_targets < target_number) {
		fprintf(stderr, "Error: provider has only %d storage targets\n", num_targets);
	}
	/* Create a region */
	size_t size = ...; // size of the region to create
	bake_create(bph, bti[target_number-1], size, &rid);
	/* Write data into the region at offset 0 */
	char* buf = ...;
	bake_write(bph, rid, 0, buf, size);
	/* Make all modifications persistent */
	bake_persist(bph, rid);
	/* Get size of region */
	size_t check_size;
	bake_get_size(bph, rid, &check_size);
	/* Release provider handle */
	bake_provider_handle_release(bph);
	/* Release BAKE client */
	bake_client_finalize(bcl);
	/* Cleanup Margo resources */
	margo_addr_free(mid, svr_addr);
	margo_finalize(mid);
	return 0;
}
```

Note that a `bake_region_id_t` object can be written (into a file or a socket)
and stored or sent to another program. These region ids are what uniquely
reference a region within a given target.

The rest of the client-side API can be found in `bake-client.h`.

## Provider API

The bake-server-daemon source is a good example of how to create providers and
attach storage targets to them. The provider-side API is located in
_bake-server.h_, and consists of mainly two functions:

```c
int bake_provider_register(
        margo_instance_id mid,
        uint8_t mplex_id,
        ABT_pool pool,
        bake_provider_t* provider);
```

This creates a provider at the given multiplex id, using a given Argobots pool.

```c
int bake_provider_add_storage_target(
        bake_provider_t provider,
        const char *target_name,
        bake_target_id_t* target_id);
```

This makes the provider manage the given storage target.

Other functions are available to remove a storage target (or all storage
targets) from a provider.

## Benchmark execution example

* `./bake-latency-bench sm:///tmp/cci/sm/carns-x1/1/1 100000 4 8`

This example runs a sequence of latency benchmarks.  Other utilities
installed with BAKE will perform other rudimentary operations.

The first argument is the address of the server.  We are using CCI/SM in this
case, which means that the URL is a path to the connection information of the
server in /tmp.  The base (not changeable) portion of the path is
/tmp/cci/HOSTNAME.  The remainder of the path was specified by the server
daemon when it started.

The second argument is the number of benchmark iterations.

The third and fourth arguments specify the range of sizes to use for read and
write operations in the benchmark.

## Misc tips

Memory allocation seems to account for a significant portion of
the latency as of this writing.  The tcmalloc library will lower and
stabilize latency somewhat as a partial short-term solution.  On ubuntu
you can use tcmalloc by running this in the terminal before the server
and client commands:

export LD_PRELOAD=/usr/lib/libtcmalloc.so.4

## Manual installation

BAKE depends on the following libraries:

* uuid (install uuid-dev package on ubuntu)
* NVML/libpmem (see instructions below)
* margo (see instructions [here](https://xgitlab.cels.anl.gov/sds/margo))

Yu can compile and install the latest git revision of NVML as follows:

* `git clone https://github.com/pmem/nvml.git`
* `cd nvml`
* `make`
* `make install prefix=/home/carns/working/install/`

`make install` will require `pandoc` or you can ignore the error and not build
the documentation.

To compile BAKE:

* `./prepare.sh`
* `mkdir build`
* `cd build`
* `../configure --prefix=/home/carns/working/install`
* `make`

If any dependencies are installed in a nonstandard location, then
modify the configure step listed above to include the following argument:

* `PKG_CONFIG_PATH=/home/carns/working/install/lib/pkgconfig`
