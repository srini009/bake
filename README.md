# bake-bulk

Example execution:

$ ./bake-bulk-server sm://1/1 /dev/shm/foo.dat

$ ./bb-latency-bench sm:///tmp/cci/sm/carns-x1/1/1 100000 4 8

The tcmalloc library will lower and stabilize latency somewhat.  On ubuntu
you can use tcmalloc by running this in the terminal before the server and
client commands:

export LD_PRELOAD=/usr/lib/libtcmalloc.so.4
