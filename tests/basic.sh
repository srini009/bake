#!/bin/bash -x

if [ -z $srcdir ]; then
    echo srcdir variable not set.
    exit 1
fi
source $srcdir/tests/test-util.sh

# start 1 server with 2 second wait, 20s timeout
test_start_servers 1 2 20

# actual test case
#####################

# run_to 10 src/SOME_EXAMPLE_PROGRAM $svr1
# if [ $? -ne 0 ]; then
#     run_to 10 src/bb-shutdown $svr1 &> /dev/null 
#     wait
#     exit 1
# fi
sleep 1

#####################

# tear down
run_to 10 src/bb-shutdown $svr1 &> /dev/null 
if [ $? -ne 0 ]; then
    wait
    exit 1
fi

wait

echo cleaning up $TMPBASE
rm -rf $TMPBASE

exit 0
