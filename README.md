# bake-bulk

## Dependencies

* uuid (install uuid-dev package on ubuntu)
* NVML/libpmem (see instructions below)
* margo (see instructions at https://xgitlab.cels.anl.gov/sds/margo)

You can compile and install the latest git revision of NVML as follows:

* `git clone https://github.com/pmem/nvml.git`
* `cd nvml`
* `make`
* `make install prefix=/home/carns/working/install/`

`make install` will require `pandoc` or you can ignore the error and not build
the documentation.

## Compilation

* `./prepare.sh`
* `mkdir build`
* `cd build`
* `../configure --prefix=/home/carns/working/install`
* `make`

If any dependencies are installed in a nonstandard location, then
modify the configure step listed above to include the following argument:

* `PKG_CONFIG_PATH=/home/carns/working/install/lib/pkgconfig`


## Server daemon execution example (using tmpfs memory as backing store)

* `truncate -s 500M /dev/shm/foo.dat`
* `pmempool create obj /dev/shm/foo.dat`
* `bake-bulk-server sm://1/1 /dev/shm/foo.dat`

### Explanation

The truncate command creates an empty 500 MiB file in /dev/shm,
which will act as a ramdisk for storage in this case.  You can skip this step
if you are using a true NVRAM device file.

The pmempool command formats the storage device as a pmem target for
libpmemobj.

The bake-bulk-server command starts the server daemon.  

The first argument to bake-bulk-server is the address for Mercury to
listen on.  In this case we are using the CCI/SM transport.  For other
transports this would more likely just be an address and port number
(e.g. "tcp://localhost:1234").  CCI/SM endpoints are identified by two
integers.  By default ("sm://"), the first integer is the server's process
id and the second is 0.  The defaults can be overriden on the command
line (e.g. "sm://1/1" fixes the listening address to 1/1).  CCI/SM uses
the integers in the address in conjunction with the server's hostname
to create subdirectories in /tmp/cci/sm for IPC connection information.
CCI/SM will create all necessary subdirectories in /tmp/cci.  For example,
if the command is run on host "carns-x1" with "sm://1/1" then CCI/SM
will create a /tmp/cci/sm/carns-x1/1/1 directory containing connection
information for the bake-bulk-server process.

The second argument to bake-bulk-server is the path to the libpmem-formatted
storage device.

## Benchmark execution example

* `./bb-latency-bench sm:///tmp/cci/sm/carns-x1/1/1 100000 4 8`

This example runs a sequence of latency benchmarks.  Other bb- utilities
installed with bake-bulk will perform other rudimentary operations.

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
