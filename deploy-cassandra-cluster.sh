#!/bin/bash
# DATE       AUTHOR      COMMENT
# ---------- ----------- -----------------------------------------------------
# 2014-08-22 Yosub       Initial version
# 2014-09-10 Yosub       Inspect Cassandra's running status after deploy

CASSANDRA_SRC_DIR_NAME=apache-cassandra-2.0.8-src-0713
CASSANDRA_SRC_TAR_FILE=cassandra-src.tar.gz
CASSANDRA_HOME=/opt/cassandra

if [ $# -lt 5 ]
then
    echo $"Usage: $0 --basedir=<base directory> --mode=<mode> --cluster_size=<cluster size> --active_cluster_size=<active cluster size>"
    exit 2
fi

for i in "$@"
do
case $i in
    --basedir=*)
    REMOTE_BASE_DIR="${i#*=}"
    shift
    ;;
    --mode=*)
    MODE="${i#*=}"
    shift
    ;;
    --cluster_size=*)
    CLUSTER_SIZE="${i#*=}"
    shift
    ;;
    --active_cluster_size=*)
    ACTIVE_CLUSTER_SIZE="${i#*=}"
    shift
    ;;
    --rebuild=*)
    REBUILD="${i#*=}"
    shift
    ;;
    *)
    # unknown option
    ;;
esac
done

REMOTE_SCRIPT_DIR=${REMOTE_BASE_DIR}/morphous-cassandra-emulab-script
REMOTE_REDEPLOY_SCRIPT=${REMOTE_SCRIPT_DIR}/redeploy-node-script.sh

echo "## Deploying Cassandra cluster with mode: " ${MODE}

sudo pkill -f morphous-script

# Set JAVA_HOME to build with Ant
export JAVA_HOME=/usr/lib/jvm/jdk1.7.0

if [ "$REBUILD" == "true" ]; then
    cd ${REMOTE_BASE_DIR}
    if [ -f "$REMOTE_BASE_DIR/$CASSANDRA_SRC_TAR_FILE" ]; then
        echo "## Unzipping Cassandra source"
        # Delete preexisting Cassandra source directory
        rm -rf ${REMOTE_BASE_DIR}/${CASSANDRA_SRC_DIR_NAME}
        tar -xzf ${CASSANDRA_SRC_TAR_FILE}
        # Clean up tar file
        rm ${CASSANDRA_SRC_TAR_FILE}
    else
        echo "## Downloading Cassandra source from Git repository"
        git clone https://github.com/YosubShin/morphous-cassandra.git
        mv morphous-cassandra ${CASSANDRA_SRC_DIR_NAME}
    fi

    cd ${REMOTE_BASE_DIR}/${CASSANDRA_SRC_DIR_NAME}

    echo "## Building Cassandra source"
    rm -rf build
    ant clean build

    echo "## Ant Build is over. Invoking redeploy script on remote nodes"
fi

# Invoke redeploy script for each node, parallelize after node-0
for (( i=0; i < CLUSTER_SIZE; i++))
do
if [ $i == 0 ]; then
    echo "## Invoking redeploy script on node-$i as the primary seed node"
    ssh -o "StrictHostKeyChecking no" node-$i "sudo $REMOTE_REDEPLOY_SCRIPT --basedir=${REMOTE_BASE_DIR} --node_address=node-$i --mode=${MODE} --seed_address=node-0"
    echo "## Finished invoking redeploy script on node-$i"
elif [ "$i" -lt "$ACTIVE_CLUSTER_SIZE" ]; then
    (
    echo "## Invoking redeploy script on node-$i with seed being node-0"
    ssh -o "StrictHostKeyChecking no" node-$i "sudo $REMOTE_REDEPLOY_SCRIPT --basedir=${REMOTE_BASE_DIR} --node_address=node-$i --mode=${MODE} --seed_address=node-0"
    echo "## Finished invoking redeploy script on node-$i"
    ) &
elif [ "$i" -eq "$ACTIVE_CLUSTER_SIZE" ]; then
    echo "## Invoking redeploy script on node-$i as the secondary seed node"
    ssh -o "StrictHostKeyChecking no" node-$i "sudo $REMOTE_REDEPLOY_SCRIPT --basedir=${REMOTE_BASE_DIR} --node_address=node-$i --mode=${MODE} --seed_address=node-${ACTIVE_CLUSTER_SIZE}"
    echo "## Finished invoking redeploy script on node-$i"
else
    (
    echo "## Invoking redeploy script on node-$i with seed being node-${ACTIVE_CLUSTER_SIZE}"
    ssh -o "StrictHostKeyChecking no" node-$i "sudo $REMOTE_REDEPLOY_SCRIPT --basedir=${REMOTE_BASE_DIR} --node_address=node-$i --mode=${MODE} --seed_address=node-${ACTIVE_CLUSTER_SIZE}"
    echo "## Finished invoking redeploy script on node-$i"
    ) &
fi
done

wait

sleep 10

${CASSANDRA_HOME}/bin/nodetool status
