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
    --throughput=*)
    THROUGHPUT="${i#*=}"
    shift
    ;;
    --host=*)
    HOST="${i#*=}"
    shift
    ;;
    --profile=*)
    PROFILE="${i#*=}"
    shift
    ;;
    --delay_in_millisec=*)
    DELAY_IN_MILLISEC="${i#*=}"
    shift
    ;;
    --altered=*)
    ALTERED="${i#*=}"
    shift
    ;;
    --measurement_type=*)
    MEASUREMENT_TYPE="${i#*=}"
    shift
    ;;
    *)
            # unknown option
    ;;
esac
done

if [ "emulab" = "$PROFILE" ]; then
ulimit -n 32768
fi

if [ "${ALTERED}" = "False" ]; then
WORKLOAD_FILENAME="workload.txt"
RESULT_POSTFIX="-original"
else
WORKLOAD_FILENAME="workload-altered.txt"
RESULT_POSTFIX="-altered"
fi

if [ "$THROUGHPUT" -eq "0" ]; then
THROUGHPUT_PROPERTY=""
else
THROUGHPUT_PROPERTY="-target ${THROUGHPUT}"
fi

# Execute YCSB Workload
${YCSB_PATH}/bin/ycsb run cassandra-cql -s ${THROUGHPUT_PROPERTY} -P ${BASE_PATH}/${WORKLOAD_FILENAME} -p warmupexecutiontime=${DELAY_IN_MILLISEC} -p measurementtype=${MEASUREMENT_TYPE} 2> ${BASE_PATH}/stderr-execution-output-${HOST}${RESULT_POSTFIX}.txt 1> ${BASE_PATH}/execution-output-${HOST}${RESULT_POSTFIX}.txt
