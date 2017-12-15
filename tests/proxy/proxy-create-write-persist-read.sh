#!/bin/bash -x

if [ -z $srcdir ]; then
    echo srcdir variable not set.
    exit 1
fi
source $srcdir/tests/proxy/proxy-test-util.sh

# start 1 proxy server with 2 second wait, 20s timeout
test_start_servers 1 2 20

# start 1 proxy server with 2 second wait, 20s timeout
test_start_proxy_servers $svr1 "-b" 1 2 20

sleep 1

#####################

# tear down
run_to 10 tests/proxy/proxy-test $proxy_svr1
if [ $? -ne 0 ]; then
    wait
    exit 1
fi

wait

echo cleaning up $TMPBASE
rm -rf $TMPBASE

exit 0
