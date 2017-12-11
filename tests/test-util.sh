#
# General test script utilities
#

if [ -z "$TIMEOUT" ] ; then
    echo expected TIMEOUT variable defined to its respective command
    exit 1
fi

if [ -z "$MKTEMP" ] ; then
    echo expected MKTEMP variable defined to its respective command
    exit 1
fi

TMPDIR=/dev/shm
export TMPDIR
mkdir -p $TMPDIR
TMPBASE=$(${MKTEMP} --tmpdir -d test-XXXXXX)

echo "tmpbase: $TMPBASE"

if [ ! -d $TMPBASE ];
then
  echo "Temp directory doesn't exist: $TMPBASE"
  exit 3
fi

function run_to ()
{
    maxtime=${1}s
    shift
    ${TIMEOUT} --signal=9 $maxtime "$@"
}

function test_start_servers ()
{
    nservers=${1:-4}
    startwait=${2:-15}
    maxtime=${3:-120}

    # start daemons
    for i in `seq $nservers`
    do
        src/bake-bulk-mkpool -s 100M $TMPBASE/svr-$i.dat
        if [ $? -ne 0 ]; then
            exit 1
        fi

        run_to ${maxtime} src/bake-bulk-server-daemon -f $TMPBASE/svr-$i.addr na+sm $TMPBASE/svr-$i.dat &
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

    svr1=`cat $TMPBASE/svr-1.addr`
}
