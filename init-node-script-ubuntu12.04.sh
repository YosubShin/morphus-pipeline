#!/bin/bash
# DATE       AUTHOR      COMMENT
# ---------- ----------- -----------------------------------------------------
# 2014-08-01 Yosub       Initial version
# 2014-08-14 Yosub       Embedded cassandra.yaml
# 2014-08-22 Yosub       Modularize
# 2014-08-26 Yosub       Fix minor bugs
# 2015-01-09 Yosub       Overhaul upon update of Emulab cluster

if [ $# -lt 3 ]
then
    echo $"Usage: $0 --basedir=<base directory> --cluster_size=<cluster size> --node_address=<node address>"
    exit 2
fi

for i in "$@"
do
case $i in
    --node_address=*)
    NODE_ADDRESS="${i#*=}"
    shift
    ;;
    --cluster_size=*)
    CLUSTER_SIZE="${i#*=}"
    shift
    ;;
    --basedir=*)
    REMOTE_BASE_DIR="${i#*=}"
    shift
    ;;
    *)
            # unknown option
    ;;
esac
done

# Install necessary binaries
echo "## Installing necessary binaries ..."
sudo apt-get update
sudo apt-get install ant -y
sudo apt-get install pssh -y

# Install Oracle Java 7
echo "## Installing Java ..."

JAVA_INSTALL_FILE=jdk-7u65-linux-x64.tar.gz
JAVA_INSTALL_DIR=/tmp
JAVA_INSTALL_PATH=$JAVA_INSTALL_DIR/$JAVA_INSTALL_FILE
if ! [ -f "$JAVA_INSTALL_PATH" ]
then
    echo "Java install file does not exists"
    sudo wget --directory-prefix=$JAVA_INSTALL_DIR --output-document=$JAVA_INSTALL_PATH --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/7u65-b17/$JAVA_INSTALL_FILE
else
    echo "Java install file already exists."
fi
sudo tar -xvf $JAVA_INSTALL_PATH -C $JAVA_INSTALL_DIR
sudo mkdir /usr/lib/jvm
sudo mv $JAVA_INSTALL_DIR/jdk1.7* /usr/lib/jvm/jdk1.7.0
sudo update-alternatives --install "/usr/bin/java" "java" "/usr/lib/jvm/jdk1.7.0/bin/java" 1
sudo update-alternatives --install "/usr/bin/javac" "javac" "/usr/lib/jvm/jdk1.7.0/bin/javac" 1
sudo update-alternatives --install "/usr/bin/javaws" "javaws" "/usr/lib/jvm/jdk1.7.0/bin/javaws" 1
sudo chmod a+x /usr/bin/java
sudo chmod a+x /usr/bin/javac
sudo chmod a+x /usr/bin/javaws

sudo update-alternatives --set java /usr/lib/jvm/jdk1.7.0/bin/java

if [ $NODE_ADDRESS == "node-0" ]; then
    echo "## Executing deploy Cassandra script..."
    ${REMOTE_BASE_DIR}/morphous-cassandra-emulab-script/deploy-cassandra-cluster.sh --basedir=$REMOTE_BASE_DIR --mode=$MODE --cluster_size=$CLUSTER_SIZE --active_cluster_size=$CLUSTER_SIZE --rebuild=false
fi
