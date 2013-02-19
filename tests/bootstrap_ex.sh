#!/bin/bash

# an example script for creating a web-ka bootstrapping system

# some preliminary env variable setting
${WEB_KA:=$(cd $(dirname "$BASH_SOURCE")/.. && pwd)}
tests=$WEB_KA/tests
data=$tests/data
tools=$WEB_KA/tools

function create_bootstrapper {
    # create database of instances from $data/reverb_wikipedia_1000.txt
    cmd1="python $tools/instances2matrix.py --host=localhost --port=1979 --reset test reverb_wikipedia_1000 $data/reverb/wikipedia_1000.txt"

    # calculate PMI for reverb_wikipedia_1000_* collections
    cmd2="python $tools/matrix2pmi.py --host=localhost --port=1979 --reset --start F_all test reverb_wikipedia_1000"

    # bootstrap new entities with espresso algorithm
    cmd3="python $tools/espresso.py --host=localhost --port=1979 --n-best=10 --scorer=ReliabilityScorer --reset --start=1 --stop=10 test reverb_wikipedia_1000_2 promotes $data/seeds/promotes.dev"

    echo $cmd1 && eval $cmd1 && \
	echo $cmd2 && eval $cmd2 && \
	echo $cmd3 && eval $cmd3
}

function with_mongod {
    task="$*"

    # set the path to mongodb.conf because mongodb doesn't allow relative paths
    cmd4="sed 's#\$WEB_KA#"$WEB_KA"#g' < $tests/mongo/mongodb.conf.in > $tests/mongo/mongodb.conf"
    
    # start mongod on port 1979 with database in $tests/mongo/db
    cmd5="mkdir -p $tests/mongo/db && mkdir -p $tests/mongo/log && mongod --config $tests/mongo/mongodb.conf &"

    # execute task with mongod running
    echo $cmd4 && eval $cmd4 && \
	echo $cmd5 && eval $cmd5 && \
	mongod_pid=$! && eval $task

    # shutdown mongod
    cmd6="kill $mongod_pid"
    echo $cmd6 && eval $cmd6
}

mkdir -p $tests/log && \
    time with_mongod create_bootstrapper 2>&1 | 
tee $tests/log/bootstrap.$$.log
