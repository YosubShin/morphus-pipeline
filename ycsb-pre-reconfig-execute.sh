#!/bin/bash
# DATE       AUTHOR      COMMENT
# ---------- ----------- -----------------------------------------------------
# 2015-03-07 Yosub Shin  Initial Version

for i in "$@"
do
case $i in
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
    --num_pre_reconfig_ops_prior_to=*)
    NUM_PRE_RECONFIG_OPS_PRIOR_TO="${i#*=}"
    shift
    ;;
    *)
            # unknown option
    ;;
esac
done

cat > ${BASE_PATH}/workload-preexecution.txt <<EOF
recordcount=${NUM_RECORDS}

operationcount=${NUM_PRE_RECONFIG_OPS_PRIOR_TO}
maxexecutiontime=${MAX_EXECUTION_TIME}
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=${READ_PROPORTION}
updateproportion=${UPDATE_PROPORTION}
scanproportion=0
insertproportion=${INSERT_PROPORTION}

requestdistribution=uniform

threadcount=${NUM_THREADS}

# For CQL client
host=${HOSTS}
port=9042

cassandra.isalteredprimarykey=false
cassandra.randomsalt=${RANDOM_SALT}

cassandra.writeconsistencylevel=ONE

EOF

echo "Executing updates before running reconfiguration. Num update operations=${NUM_PRE_RECONFIG_OPS_PRIOR_TO}"

${YCSB_PATH}/bin/ycsb run cassandra-cql -s -P ${BASE_PATH}/workload-preexecution.txt > ${BASE_PATH}/output-preexecution.txt
