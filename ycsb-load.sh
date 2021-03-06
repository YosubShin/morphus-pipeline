#!/bin/bash
# DATE       AUTHOR      COMMENT
# ---------- ----------- -----------------------------------------------------
# 2015-03-07 Yosub Shin  Initial Version

for i in "$@"
do
case $i in
    --cassandra_path=*)
    CASSANDRA_PATH="${i#*=}"
    shift
    ;;
    --ycsb_path=*)
    YCSB_PATH="${i#*=}"
    shift
    ;;
    --base_path=*)
    BASE_PATH="${i#*=}"
    shift
    ;;
    --num_records=*)
    NUM_RECORDS="${i#*=}"
    shift
    ;;
    --workload=*)
    WORKLOAD="${i#*=}"
    shift
    ;;
    --replication_factor=*)
    REPLICATION_FACTOR="${i#*=}"
    shift
    ;;
    --seed_host=*)
    SEED_HOST="${i#*=}"
    shift
    ;;
    --hosts=*)
    HOSTS="${i#*=}"
    shift
    ;;
    --num_threads=*)
    NUM_THREADS="${i#*=}"
    shift
    ;;
    --read_proportion=*)
    READ_PROPORTION="${i#*=}"
    shift
    ;;
    --insert_proportion=*)
    INSERT_PROPORTION="${i#*=}"
    shift
    ;;
    --update_proportion=*)
    UPDATE_PROPORTION="${i#*=}"
    shift
    ;;
    --max_execution_time=*)
    MAX_EXECUTION_TIME="${i#*=}"
    shift
    ;;
    --random_salt=*)
    RANDOM_SALT="${i#*=}"
    shift
    ;;
    --read_consistency_level=*)
    READ_CONSISTENCY_LEVEL="${i#*=}"
    shift
    ;;
    --write_consistency_level=*)
    WRITE_CONSISTENCY_LEVEL="${i#*=}"
    shift
    ;;
    --num_pre_reconfig_ops_prior_to=*)
    NUM_PRE_RECONFIG_OPS_PRIOR_TO="${i#*=}"
    shift
    ;;
    --pre_reconfig_read_proportion=*)
    PRE_RECONFIG_READ_PROPORTION="${i#*=}"
    shift
    ;;
    --pre_reconfig_insert_proportion=*)
    PRE_RECONFIG_INSERT_PROPORTION="${i#*=}"
    shift
    ;;
    --pre_reconfig_update_proportion=*)
    PRE_RECONFIG_UPDATE_PROPORTION="${i#*=}"
    shift
    ;;
    *)
            # unknown option
    ;;
esac
done

cat > /tmp/cql_input.txt <<EOF
create keyspace ycsb WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': ${REPLICATION_FACTOR} };
create table ycsb.usertable (
    y_id varchar primary key,
    field0 varchar,
    field1 varchar,
    field2 varchar,
    field3 varchar,
    field4 varchar,
    field5 varchar,
    field6 varchar,
    field7 varchar,
    field8 varchar,
    field9 varchar);
EOF
#    field9 varchar) WITH caching = 'none';

# Setup keyspace and column family in Cassandra for YCSB workload
${CASSANDRA_PATH}/bin/cqlsh --file=/tmp/cql_input.txt ${SEED_HOST}

sleep 10

# Create output directory if not exists
if [ ! -f ${BASE_PATH} ]; then
    mkdir ${BASE_PATH}
fi

OPERATION_COUNT="$((${MAX_EXECUTION_TIME} * 10000))"

# Create YCSB workload file
cat > ${BASE_PATH}/workload.txt <<EOF
recordcount=${NUM_RECORDS}

operationcount=${OPERATION_COUNT}
maxexecutiontime=${MAX_EXECUTION_TIME}
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=${READ_PROPORTION}
updateproportion=${UPDATE_PROPORTION}
scanproportion=0
insertproportion=${INSERT_PROPORTION}

requestdistribution=${WORKLOAD}

threadcount=${NUM_THREADS}

# For CQL client
host=${HOSTS}
port=9042

cassandra.isalteredprimarykey=false
cassandra.randomsalt=${RANDOM_SALT}

timeseries.granularity=1

cassandra.readconsistencylevel=${READ_CONSISTENCY_LEVEL}
cassandra.writeconsistencylevel=${WRITE_CONSISTENCY_LEVEL}

EOF

# Create YCSB workload file for altered schema
cat > ${BASE_PATH}/workload-altered.txt <<EOF
recordcount=${NUM_RECORDS}

operationcount=${OPERATION_COUNT}
maxexecutiontime=${MAX_EXECUTION_TIME}
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=${READ_PROPORTION}
updateproportion=${UPDATE_PROPORTION}
scanproportion=0
insertproportion=${INSERT_PROPORTION}

requestdistribution=${WORKLOAD}

threadcount=${NUM_THREADS}

# For CQL client
host=${HOSTS}
port=9042

cassandra.isalteredprimarykey=true
cassandra.randomsalt=${RANDOM_SALT}

timeseries.granularity=1

cassandra.readconsistencylevel=${READ_CONSISTENCY_LEVEL}
cassandra.writeconsistencylevel=${WRITE_CONSISTENCY_LEVEL}

EOF

# Load YCSB Workload
${YCSB_PATH}/bin/ycsb load cassandra-cql -s -P ${BASE_PATH}/workload.txt -p maxexecutiontime=2000 -p cassandra.writeconsistencylevel=ONE > ${BASE_PATH}/load-output.txt
