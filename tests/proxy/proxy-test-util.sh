#!/bin/bash -x

source $srcdir/tests/test-util.sh

function test_start_proxy_servers ()
{
    bake_svr_addr=${1}
    nservers=${2:-4}
    startwait=${3:-15}
    maxtime=${4:-120}

    # start daemons
    for i in `seq $nservers`
    do
        run_to ${maxtime} tests/proxy/bb-proxy-server-daemon -f $TMPBASE/proxy-svr-1.addr na+sm $bake_svr_addr &
        if [ $? -ne 0 ]; then
            # TODO: this doesn't actually work; can't check return code of
            # something executing in background.  We have to rely on the
            # return codes of the actual client side tests to tell if
            # everything started properly
            exit 1
        fi
    done

    # wait for servers to start
    sleep ${startwait}

    proxy_svr1=`cat $TMPBASE/proxy-svr-1.addr`
}
