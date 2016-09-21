#
# General test script utilities
#

TMPDIR=/dev/shm
export TMPDIR
mkdir -p $TMPDIR
TMPBASE=$(mktemp --tmpdir -d test-XXXXXX)

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
    timeout --signal=9 $maxtime "$@"
}

function test_start_servers ()
{
    nservers=${1:-4}
    startwait=${2:-15}
    maxtime=${3:-120}s
    repfactor=${4:-0}
    pid=$$
    startport=3344
    endport=`expr 3344 + $nservers - 1`

    # start daemons
    for i in `seq $startport $endport`
    do
        truncate -s 100M $TMPBASE/foo-$i.dat
        if [ $? -ne 0 ]; then
            exit 1
        fi
        pmempool create obj $TMPBASE/foo-$i.dat
        if [ $? -ne 0 ]; then
            exit 1
        fi

        timeout --signal=9 ${maxtime} src/bake-bulk-server tcp://$i $TMPBASE/foo-$i.dat &
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

    svr1="tcp://localhost:$startport"
}
