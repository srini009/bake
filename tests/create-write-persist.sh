#!/bin/bash -x

set -e
set -o pipefail

if [ -z $srcdir ]; then
    echo srcdir variable not set.
    exit 1
fi
source $srcdir/tests/test-util.sh

# start 1 server with 2 second wait, 20s timeout
test_start_servers 1 2 20

sleep 1

#####################

# run test
run_to 10 tests/create-write-persist-test $srcdir/tests/lorem.txt $svr1 1
if [ $? -ne 0 ]; then
    wait
    exit 1
fi

wait

# check that the underlying pool has the object we created
# XXX note this assumes pmem pools -- may want to write our
# own wrapper for this functionality at some point
num_objs=`pmempool info -ns $TMPBASE/svr-1.dat | grep "Number of objects" | head -n 1 | cut -d: -f2 | awk '{$1=$1};1'`
if [ $num_objs -ne 1 ]; then
    echo "Invalid number of objects in BAKE pool"
    exit 1
fi

echo cleaning up $TMPBASE
rm -rf $TMPBASE

exit 0
