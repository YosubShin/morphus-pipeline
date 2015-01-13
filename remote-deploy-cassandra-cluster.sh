#!/bin/bash
# DATE       AUTHOR      COMMENT
# ---------- ----------- -----------------------------------------------------
# 2014-08-19 Yosub       Initial version

CASSANDRA_SRC_DIR_BASE=/Users/Daniel/Dropbox/Illinois/research/repos
CASSANDRA_SRC_DIR_NAME=apache-cassandra-2.0.8-src-0713
CASSANDRA_SRC_DIR=${CASSANDRA_SRC_DIR_BASE}/${CASSANDRA_SRC_DIR_NAME}
CASSANDRA_SRC_TAR_FILE=cassandra-src.tar.gz
SSH_USER=yossupp
SSH_URL=node-0.cassandra-morphous.ISS.emulab.net
REMOTE_BASE_DIR=/proj/ISS/shin14/repos
REMOTE_SCRIPT_DIR=${REMOTE_BASE_DIR}/morphous-cassandra-emulab-script
REMOTE_REDEPLOY_SCRIPT=${REMOTE_SCRIPT_DIR}/redeploy-node-script.sh
REMOTE_DEPLOY_CLUSTER_SCRIPT=${REMOTE_SCRIPT_DIR}/deploy-cassandra-cluster.sh
CASSANDRA_HOME=/opt/cassandra

if [ $# -lt 4 ]
then
    echo $"Usage: $0 --mode=<mode> --cluster_size=<cluster size> --active_cluster_size=<active cluster size> --rebuild=<rebuild>"
    exit 2
fi

for i in "$@"
do
case $i in
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

echo "## Remote deploying Cassandra cluster with mode=${MODE}, cluster_size=${CLUSTER_SIZE}, active_cluster_size=${ACTIVE_CLUSTER_SIZE}, rebuild=${REBUILD}"

echo "## Copying remote scripts to remote host"
scp deploy-cassandra-cluster.sh $SSH_USER@$SSH_URL:$REMOTE_SCRIPT_DIR/
scp redeploy-node-script.sh $SSH_USER@$SSH_URL:$REMOTE_SCRIPT_DIR/
scp init-node-script-ubuntu12.04.sh $SSH_USER@$SSH_URL:$REMOTE_SCRIPT_DIR/

if [ "$REBUILD" == "true" ]
then

echo "## Archiving Cassandra source files..."
cd $CASSANDRA_SRC_DIR_BASE
tar -czf $CASSANDRA_SRC_TAR_FILE -C $CASSANDRA_SRC_DIR_BASE $CASSANDRA_SRC_DIR_NAME

echo "## Deleting remote host's Cassandra source files"
ssh -T $SSH_USER@$SSH_URL /bin/bash << EOF
# Clear out previously existing Cassandra source files
sudo rm -rf $REMOTE_BASE_DIR/$CASSANDRA_SRC_DIR_NAME
sudo rm $REMOTE_BASE_DIR/$CASSANDRA_SRC_TAR_FILE
EOF

echo "## Uploading archived Cassandra source to remote host"
scp $CASSANDRA_SRC_TAR_FILE $SSH_USER@$SSH_URL:$REMOTE_BASE_DIR

# Clean up tar file
rm $CASSANDRA_SRC_DIR_BASE/$CASSANDRA_SRC_TAR_FILE

fi

echo "## Executing cluster redeploy script on remote host"
ssh -T $SSH_USER@$SSH_URL "/bin/bash $REMOTE_DEPLOY_CLUSTER_SCRIPT --basedir=$REMOTE_BASE_DIR --mode=$MODE --cluster_size=$CLUSTER_SIZE --active_cluster_size=$ACTIVE_CLUSTER_SIZE --rebuild=$REBUILD"
