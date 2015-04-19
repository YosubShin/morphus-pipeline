#!/bin/bash
# DATE       AUTHOR      COMMENT
# ---------- ----------- -----------------------------------------------------
# 2015-03-20 Yosub       Initial version

for i in "$@"
do
case $i in
    --java_path=*)
    JAVA_PATH="${i#*=}"
    shift
    ;;
    --orig_cassandra_path=*)
    ORIG_CASSANDRA_PATH="${i#*=}"
    shift
    ;;
    --cassandra_home=*)
    CASSANDRA_HOME="${i#*=}"
    shift
    ;;
    --dst_host=*)
    DST_HOST="${i#*=}"
    shift
    ;;
    --seed_host=*)
    SEED_HOST="${i#*=}"
    shift
    ;;
    *)
            # unknown option
    ;;
esac
done

# Inside heredoc, \$'s delimited with \\ is not expanded locally and will be executed in remote machine
#ssh ${DST_HOST} <<EOF2
##!/bin/bash

cat <<EOF2 | ssh -t ${DST_HOST} /bin/bash

JAVA_HOME=${JAVA_PATH}
PATH=\$PATH:${JAVA_PATH}

echo "# Cleaning up cassandra..."
rm -rf ${CASSANDRA_HOME}

echo "# Making a new Cassandra directory and copying Cassandra binary..."
mkdir ${CASSANDRA_HOME} ${CASSANDRA_HOME}/data ${CASSANDRA_HOME}/log ${CASSANDRA_HOME}/commitlog ${CASSANDRA_HOME}/saved_caches
cp -r ${ORIG_CASSANDRA_PATH} ${CASSANDRA_HOME}/cassandra

echo "# Updating cassandra.yaml config file to customize for the host ${DST_HOST}..."
bash -c "cat > ${CASSANDRA_HOME}/cassandra/conf/cassandra.yaml" <<-EOF

    cluster_name: 'Test Cluster'
    num_tokens: 256
    hinted_handoff_enabled: true
    max_hint_window_in_ms: 10800000
    hinted_handoff_throttle_in_kb: 1024
    max_hints_delivery_threads: 2
    batchlog_replay_throttle_in_kb: 1024
    authenticator: AllowAllAuthenticator
    authorizer: AllowAllAuthorizer
    permissions_validity_in_ms: 2000
    partitioner: org.apache.cassandra.dht.Murmur3Partitioner
    data_file_directories:
        - ${CASSANDRA_HOME}/data
    commitlog_directory: ${CASSANDRA_HOME}/commitlog
    disk_failure_policy: stop
    commit_failure_policy: stop
    key_cache_size_in_mb:
    key_cache_save_period: 14400
    row_cache_size_in_mb: 0
    row_cache_save_period: 0
    saved_caches_directory: ${CASSANDRA_HOME}/saved_caches
    commitlog_sync: periodic
    commitlog_sync_period_in_ms: 10000
    commitlog_segment_size_in_mb: 32
    seed_provider:
      - class_name: org.apache.cassandra.locator.SimpleSeedProvider
        parameters:
          - seeds: "${SEED_HOST}"
    concurrent_reads: 32
    concurrent_writes: 32
    memtable_flush_queue_size: 4
    trickle_fsync: false
    trickle_fsync_interval_in_kb: 10240
    storage_port: 7000
    ssl_storage_port: 7001
    listen_address: ${DST_HOST}
    start_native_transport: true
    native_transport_port: 9042
    start_rpc: true
    rpc_address: ${DST_HOST}
    rpc_port: 9160
    rpc_keepalive: true
    rpc_server_type: sync
    thrift_framed_transport_size_in_mb: 15
    incremental_backups: false
    snapshot_before_compaction: false
    auto_snapshot: true
    tombstone_warn_threshold: 1000
    tombstone_failure_threshold: 100000
    column_index_size_in_kb: 64
    batch_size_warn_threshold_in_kb: 5
    in_memory_compaction_limit_in_mb: 64
    multithreaded_compaction: false
    compaction_throughput_mb_per_sec: 16
    compaction_preheat_key_cache: true
    read_request_timeout_in_ms: 10000
    range_request_timeout_in_ms: 10000
    write_request_timeout_in_ms: 10000
    cas_contention_timeout_in_ms: 1000
    truncate_request_timeout_in_ms: 60000
    request_timeout_in_ms: 10000
    cross_node_timeout: false
    endpoint_snitch: SimpleSnitch
    dynamic_snitch_update_interval_in_ms: 100
    dynamic_snitch_reset_interval_in_ms: 600000
    dynamic_snitch_badness_threshold: 0.1
    request_scheduler: org.apache.cassandra.scheduler.NoScheduler
    server_encryption_options:
        internode_encryption: none
        keystore: conf/.keystore
        keystore_password: cassandra
        truststore: conf/.truststore
        truststore_password: cassandra

    client_encryption_options:
        enabled: false
        keystore: conf/.keystore
        keystore_password: cassandra

    internode_compression: all
    inter_dc_tcp_nodelay: false
    preheat_kernel_page_cache: false

EOF

echo "# Updating log4j config file"

bash -c "cat > ${CASSANDRA_HOME}/cassandra/conf/log4j-server.properties" <<-EOF

    log4j.rootLogger=INFO,stdout,R
    log4j.appender.stdout=org.apache.log4j.ConsoleAppender
    log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
    log4j.appender.stdout.layout.ConversionPattern=%5p %d{HH:mm:ss,SSS} %m%n
    log4j.appender.R=org.apache.log4j.RollingFileAppender
    log4j.appender.R.maxFileSize=20MB
    log4j.appender.R.maxBackupIndex=50
    log4j.appender.R.layout=org.apache.log4j.PatternLayout
    log4j.appender.R.layout.ConversionPattern=%5p [%t] %d{ISO8601} %F (line %L) %m%n
    log4j.appender.R.File=${CASSANDRA_HOME}/log/system.log
    log4j.logger.org.apache.cassandra=INFO
    log4j.logger.edu.uiuc.dprg.morphous=DEBUG
    log4j.logger.org.apache.thrift.server.TNonblockingServer=ERROR

EOF

echo "# Running Cassandra"
${CASSANDRA_HOME}/cassandra/bin/cassandra

EOF2
