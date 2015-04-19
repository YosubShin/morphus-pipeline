import os
from time import strftime, sleep
import ConfigParser
from threading import Thread
import thread
import sys
import profile
import logging
import cassandra_log_parser as ps
import time

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s: '
                           '%(filename)s: '
                           '%(levelname)s: '
                           '%(funcName)s(): '
                           '%(lineno)d:\t'
                           '%(message)s')
logger = logging.getLogger(__name__)

private_config = ConfigParser.SafeConfigParser()
private_config.read('private.ini')


class YcsbExecuteThread(Thread):
    def __init__(self, pf, host, target_throughput, result_path, output, mutex, delay_in_millisec, altered,
                 measurement_type):
        Thread.__init__(self)
        self.pf = pf
        self.host = host
        self.target_throughput = target_throughput
        self.result_path = result_path
        self.output = output
        self.mutex = mutex
        self.delay_in_millisec = delay_in_millisec
        self.altered = altered
        self.measurement_type = measurement_type

    def run(self):
        logger.debug('Running YCSB executor thread at host %s with %d ms delay' % (self.host, self.delay_in_millisec))
        ycsb_path = self.pf.config.get('path', 'ycsb_path')
        src_path = self.pf.config.get('path', 'src_path')
        ret = os.system('ssh %s \'sh %s/ycsb-execute.sh --ycsb_path=%s --base_path=%s '
                        '--throughput=%d --host=%s --profile=%s --delay_in_millisec=%d --altered=%s '
                        '--measurement_type=%s\''
                        % (self.host, src_path, ycsb_path, self.result_path,
                           int(self.target_throughput or 0), self.host, self.pf.get_name(), self.delay_in_millisec,
                           str(self.altered), self.measurement_type))
        self.mutex.acquire()
        self.output.append(ret)
        self.mutex.release()
        logger.debug('Finished running YCSB executor thread at host %s' % self.host)


def run_experiment(pf, hosts, overall_target_throughput, workload_type, total_num_records, replication_factor,
                   num_cassandra_nodes, num_ycsb_nodes, total_num_ycsb_threads, workload_proportions,
                   measurement_type, should_inject_operations, should_reconfigure, should_compact=True):
    cassandra_path = pf.config.get('path', 'cassandra_path')
    cassandra_home_base_path = pf.config.get('path', 'cassandra_home_base_path')
    ycsb_path = pf.config.get('path', 'ycsb_path')
    java_path = pf.config.get('path', 'java_path')
    result_base_path = pf.config.get('path', 'result_base_path')

    result_dir_name = strftime('%m-%d-%H%M')
    result_path = '%s/%s' % (result_base_path, result_dir_name)
    logger.debug('Executing w/ pf=%s, num_hosts=%d, overall_target_throughput=%d, workload_type=%s, '
                 'num_records=%d, replication_factor=%d, num_cassandra_nodes=%d, result_dir_name=%s, '
                 'num_ycsb_nodes=%d, total_num_ycsb_threads=%d, workload_proportions=%s, measurement_type=%s, '
                 'should_inject_operations=%s, should_reconfigure=%s' %
                 (pf.get_name(), len(hosts), int(overall_target_throughput or -1), workload_type,
                  total_num_records, replication_factor, num_cassandra_nodes, result_dir_name,
                  num_ycsb_nodes, total_num_ycsb_threads, str(workload_proportions), measurement_type,
                  should_inject_operations, should_reconfigure))

    assert num_cassandra_nodes <= pf.get_max_num_cassandra_nodes()
    assert num_ycsb_nodes <= pf.get_max_num_ycsb_nodes()
    assert num_cassandra_nodes + num_ycsb_nodes <= len(hosts)

    # Kill cassandra on all hosts
    for host in hosts:
        logger.debug('Killing Casandra at host %s' % host)
        os.system('ssh %s ps aux | grep cassandra | grep -v grep | grep java | awk \'{print $2}\' | '
                  'xargs ssh %s kill -9' % (host, host))

    sleep(10)

    seed_host = hosts[0]
    # Cleanup, make directories, and run cassandra
    for host in hosts[0:num_cassandra_nodes]:
        logger.debug('Deploying cassandra at host %s' % host)
        cassandra_home = '%s/%s' % (cassandra_home_base_path, host)
        ret = os.system('sh deploy-cassandra-cluster.sh --orig_cassandra_path=%s --cassandra_home=%s '
                        '--seed_host=%s --dst_host=%s --java_path=%s' %
                        (cassandra_path, cassandra_home, seed_host, host, java_path))
        sleep(5)

    # Running YCSB load script
    logger.debug('Running YCSB load script')
    src_path = pf.config.get('path', 'src_path')
    cassandra_nodes_hosts = ' '.join(hosts[0:num_cassandra_nodes])

    if workload_proportions is not None and \
            workload_proportions.has_key('read') and \
            workload_proportions.has_key('insert') and \
            workload_proportions.has_key('update'):
        pass
    else:
        workload_proportions = {'read': 10, 'update': 0, 'insert': 0}

    num_ycsb_threads = total_num_ycsb_threads / num_ycsb_nodes
    max_execution_time = 60 + 70 * total_num_records / 1000000
    ret = os.system('ssh %s \'sh %s/ycsb-load.sh '
                    '--cassandra_path=%s --ycsb_path=%s '
                    '--base_path=%s --num_records=%d --workload=%s '
                    '--replication_factor=%d --seed_host=%s --hosts=%s --num_threads=%d '
                    '--read_proportion=%d --insert_proportion=%d --update_proportion=%d --max_execution_time=%d \''
                    % (hosts[num_cassandra_nodes], src_path, cassandra_path, ycsb_path,
                       result_path, total_num_records, workload_type,
                       replication_factor, seed_host, cassandra_nodes_hosts, num_ycsb_threads,
                       workload_proportions['read'], workload_proportions['insert'], workload_proportions['update'],
                       max_execution_time))
    if ret != 0:
        raise Exception('Unable to finish YCSB script')

    # Save configuration files for this experiment
    meta = ConfigParser.ConfigParser()
    meta.add_section('config')
    meta.add_section('result')
    meta.set('config', 'profile', pf.get_name())
    meta.set('config', 'num_hosts', len(hosts))
    if overall_target_throughput is not None:
        meta.set('config', 'target_throughput', overall_target_throughput)
    meta.set('config', 'workload_type', workload_type)
    meta.set('config', 'num_records', total_num_records)
    meta.set('config', 'replication_factor', replication_factor)
    meta.set('config', 'num_cassandra_nodes', num_cassandra_nodes)
    meta.set('config', 'num_ycsb_nodes', num_ycsb_nodes)
    meta.set('config', 'total_num_ycsb_threads', total_num_ycsb_threads)
    meta.set('config', 'result_dir_name', result_dir_name)
    meta.set('config', 'workload_proportions', str(workload_proportions))
    meta.set('config', 'measurement_type', measurement_type)
    meta.set('config', 'should_inject_operations', should_inject_operations)
    meta.set('config', 'should_reconfigure', should_reconfigure)

    threads = []
    output = []
    mutex = thread.allocate_lock()

    sleep(10)

    if should_inject_operations:
        # Run YCSB executor threads in parallel at each host
        logger.debug('Running YCSB execute workload at each host in parallel...')
        base_warmup_time_in_millisec = 5000
        # Delay rate : 3 seconds / 100 threads
        interval_in_millisec = num_ycsb_threads * 30
        # Multiply by 2, because we have two separate workloads for each schema
        delay_in_millisec = num_ycsb_nodes * interval_in_millisec * 2 + base_warmup_time_in_millisec

        if overall_target_throughput is not None:
            target_throughput = overall_target_throughput / num_ycsb_nodes
        else:
            target_throughput = None

        # Run YCSB workload for original schema
        for host in hosts[num_cassandra_nodes:num_cassandra_nodes + num_ycsb_nodes]:
            current_thread = YcsbExecuteThread(pf, host, target_throughput, result_path, output, mutex, delay_in_millisec,
                                               False, measurement_type)
            threads.append(current_thread)
            current_thread.start()
            delay_in_millisec -= interval_in_millisec
            sleep(interval_in_millisec * 0.001)

        # Run YCSB workload for altered schema
        for host in hosts[num_cassandra_nodes:num_cassandra_nodes + num_ycsb_nodes]:
            current_thread = YcsbExecuteThread(pf, host, target_throughput, result_path, output, mutex, delay_in_millisec,
                                               True, measurement_type)
            threads.append(current_thread)
            current_thread.start()
            delay_in_millisec -= interval_in_millisec
            sleep(interval_in_millisec * 0.001)

    sleep(30)
    if should_reconfigure:
        logger.debug('Running Morphus script at host %s' % hosts[0])
        os.system('%s/bin/nodetool -h %s -m \'{"column":"%s", "compact":"%s"}\' morphous %s %s' %
                  (cassandra_path, hosts[0], 'field0', str(should_compact).lower(), 'ycsb', 'usertable'))

    if not should_inject_operations and should_reconfigure:
        sleep_for = max_execution_time
        default_num_cassandra_nodes = int(pf.config.get('experiment', 'default_num_cassandra_nodes'))
        default_total_num_records = int(pf.config.get('experiment', 'default_total_num_records'))
        default_replication_factor = int(pf.config.get('experiment', 'default_replication_factor'))
        if num_cassandra_nodes != default_num_cassandra_nodes:
            sleep_for = 60 + 70 * default_num_cassandra_nodes / num_cassandra_nodes
        elif total_num_records > default_total_num_records:
            sleep_for = 60 + 70 * total_num_records / default_total_num_records
        elif replication_factor > default_replication_factor:
            sleep_for = 60 + 70 * replication_factor / default_replication_factor
        logger.debug('No operations are being injected, and instead sleep for %s seconds' % sleep_for)
        sleep(sleep_for)
    else:
        for t in threads:
            t.join()

        logger.debug('Threads finished executing with outputs: %s...' % output)

    # Collect log files from cassandra hosts
    logger.debug('Collecting Cassandra logs...')
    for host in hosts[0:num_cassandra_nodes]:
        os.system('scp %s:%s/%s/log/system.log %s/cassandra-log-%s.txt' %
                  (host, cassandra_home_base_path, host, result_path, host))

    if should_reconfigure:
        # Extract Morphus result from coordinator machine's log
        morphus_result_f = open('%s/cassandra-log-%s.txt' % (result_path, hosts[0]))

        parser = ps.CassandraLogParser(morphus_result_f.read())
        result_dict = parser.parse()
        logger.debug('Morphus result: %s' % str(result_dict))
        if len(result_dict) < 5:
            logger.error('Morphus script not ended completely')
        else:
            for k, v in result_dict.iteritems():
                meta.set('result', k, v)

    meta.set('config', 'cassandra_coordinator_host', hosts[0])

    meta_file = open('%s/meta.ini' % result_path, 'w')
    meta.write(meta_file)
    meta_file.close()


def experiment_on_workloads(pf, repeat):
    workload_parameters = [  # (Workload type, Workload proportions, should_reconfigure)
        ('uniform', {'read': 4, 'update': 4, 'insert': 2}, True),
        ('zipfian', {'read': 4, 'update': 4, 'insert': 2}, True),
        ('latest', {'read': 4, 'update': 4, 'insert': 2}, True),
        ('uniform', {'read': 10, 'update': 0, 'insert': 0}, True),
        ('uniform', {'read': 4, 'update': 4, 'insert': 2}, False)
    ]
    total_num_records = int(pf.config.get('experiment', 'default_total_num_records'))
    replication_factor = int(pf.config.get('experiment', 'default_replication_factor'))
    num_cassandra_nodes = int(pf.config.get('experiment', 'default_num_cassandra_nodes'))
    target_throughput = int(pf.config.get('experiment', 'default_operations_rate'))

    for run in range(repeat):
        for workload_type, workload_proportions, should_reconfigure in workload_parameters:
            for measurement_type in ['histogram', 'timeseries']:
                total_num_ycsb_threads = pf.get_max_num_connections_per_cassandra_node() * num_cassandra_nodes
                num_ycsb_nodes = total_num_ycsb_threads / pf.get_max_allowed_num_ycsb_threads_per_node() + 1
                logger.debug('workload_type=%s, workload_proportions=%s, should_reconfigure=%s, num_cassandra_nodes=%d,'
                             ' total_num_ycsb_threads=%d, num_ycsb_nodes=%d, total_num_records=%d'
                             % (workload_type, str(workload_proportions), should_reconfigure, num_cassandra_nodes,
                                total_num_ycsb_threads, num_ycsb_nodes, total_num_records))

                result = run_experiment(pf,
                                        hosts=pf.get_hosts(),
                                        overall_target_throughput=target_throughput,
                                        total_num_records=total_num_records,
                                        workload_type=workload_type,
                                        replication_factor=replication_factor,
                                        num_cassandra_nodes=num_cassandra_nodes,
                                        num_ycsb_nodes=num_ycsb_nodes,
                                        total_num_ycsb_threads=total_num_ycsb_threads,
                                        workload_proportions=workload_proportions,
                                        measurement_type=measurement_type,
                                        should_inject_operations=True,
                                        should_reconfigure=should_reconfigure)


def experiment_on_num_cassandra_nodes(pf, repeat):
    # No operations injected
    num_cassandra_nodes_list = [1, 3, 7, 10]
    workload_type = pf.config.get('experiment', 'default_workload_type')
    total_num_records = int(pf.config.get('experiment', 'default_total_num_records'))
    replication_factor = int(pf.config.get('experiment', 'default_replication_factor'))
    measurement_type = pf.config.get('experiment', 'default_measurement_type')

    for run in range(repeat):
        for num_cassandra_nodes in num_cassandra_nodes_list:
            total_num_ycsb_threads = pf.get_max_num_connections_per_cassandra_node() * num_cassandra_nodes
            num_ycsb_nodes = total_num_ycsb_threads / pf.get_max_allowed_num_ycsb_threads_per_node() + 1
            logger.debug('num_cassandra_nodes=%d' % num_cassandra_nodes)

            result = run_experiment(pf,
                                    hosts=pf.get_hosts(),
                                    overall_target_throughput=None,
                                    total_num_records=total_num_records,
                                    workload_type=workload_type,
                                    replication_factor=replication_factor,
                                    num_cassandra_nodes=num_cassandra_nodes,
                                    num_ycsb_nodes=num_ycsb_nodes,
                                    total_num_ycsb_threads=total_num_ycsb_threads,
                                    workload_proportions=None,
                                    measurement_type=measurement_type,
                                    should_inject_operations=False,
                                    should_reconfigure=True)


def experiment_on_num_records(pf, repeat):
    # No operations injected
    capacity_list = [1, 5, 10, 20]
    total_num_records_list = [x * 1000000 for x in capacity_list]
    num_cassandra_nodes = int(pf.config.get('experiment', 'default_num_cassandra_nodes'))
    workload_type = pf.config.get('experiment', 'default_workload_type')
    replication_factor = int(pf.config.get('experiment', 'default_replication_factor'))
    measurement_type = pf.config.get('experiment', 'default_measurement_type')

    for run in range(repeat):
        for total_num_records in total_num_records_list:
            total_num_ycsb_threads = pf.get_max_num_connections_per_cassandra_node() * num_cassandra_nodes
            num_ycsb_nodes = total_num_ycsb_threads / pf.get_max_allowed_num_ycsb_threads_per_node() + 1
            logger.debug('total_num_records=%d' % total_num_records)

            result = run_experiment(pf,
                                    hosts=pf.get_hosts(),
                                    overall_target_throughput=None,
                                    total_num_records=total_num_records,
                                    workload_type=workload_type,
                                    replication_factor=replication_factor,
                                    num_cassandra_nodes=num_cassandra_nodes,
                                    num_ycsb_nodes=num_ycsb_nodes,
                                    total_num_ycsb_threads=total_num_ycsb_threads,
                                    workload_proportions=None,
                                    measurement_type=measurement_type,
                                    should_inject_operations=False,
                                    should_reconfigure=True)


def experiment_on_replication_factors(pf, repeat):
    # No operations injected
    replication_factors = [1, 2, 3, 4]
    total_num_records = int(pf.config.get('experiment', 'default_total_num_records'))
    num_cassandra_nodes = int(pf.config.get('experiment', 'default_num_cassandra_nodes'))
    workload_type = pf.config.get('experiment', 'default_workload_type')
    measurement_type = pf.config.get('experiment', 'default_measurement_type')

    for run in range(repeat):
        for replication_factor in replication_factors:
            total_num_ycsb_threads = pf.get_max_num_connections_per_cassandra_node() * num_cassandra_nodes
            num_ycsb_nodes = total_num_ycsb_threads / pf.get_max_allowed_num_ycsb_threads_per_node() + 1
            logger.debug('replication_factor=%d' % replication_factor)

            result = run_experiment(pf,
                                    hosts=pf.get_hosts(),
                                    overall_target_throughput=None,
                                    total_num_records=total_num_records,
                                    workload_type=workload_type,
                                    replication_factor=replication_factor,
                                    num_cassandra_nodes=num_cassandra_nodes,
                                    num_ycsb_nodes=num_ycsb_nodes,
                                    total_num_ycsb_threads=total_num_ycsb_threads,
                                    workload_proportions=None,
                                    measurement_type=measurement_type,
                                    should_inject_operations=False,
                                    should_reconfigure=True)


def experiment_on_operations_rate(pf, repeat):
    experiment_parameters = [
        (False, 0),
        (True, 100),
        (True, 300),
        (True, 700),
        (True, 1000)
    ]
    workload_type = pf.config.get('experiment', 'default_workload_type')
    workload_proportions = {'read': 4, 'update': 4, 'insert': 2}
    total_num_records = int(pf.config.get('experiment', 'default_total_num_records'))
    replication_factor = int(pf.config.get('experiment', 'default_replication_factor'))
    num_cassandra_nodes = int(pf.config.get('experiment', 'default_num_cassandra_nodes'))
    measurement_type = pf.config.get('experiment', 'default_measurement_type')

    for run in range(repeat):
        for should_inject_operations, overall_target_throughput in experiment_parameters:
            total_num_ycsb_threads = pf.get_max_num_connections_per_cassandra_node() * num_cassandra_nodes
            num_ycsb_nodes = total_num_ycsb_threads / pf.get_max_allowed_num_ycsb_threads_per_node() + 1

            logger.debug('overall_target_throughput=%d' % overall_target_throughput)

            result = run_experiment(pf,
                                    hosts=pf.get_hosts(),
                                    overall_target_throughput=overall_target_throughput,
                                    total_num_records=total_num_records,
                                    workload_type=workload_type,
                                    replication_factor=replication_factor,
                                    num_cassandra_nodes=num_cassandra_nodes,
                                    num_ycsb_nodes=num_ycsb_nodes,
                                    total_num_ycsb_threads=total_num_ycsb_threads,
                                    workload_proportions=workload_proportions,
                                    measurement_type=measurement_type,
                                    should_inject_operations=should_inject_operations,
                                    should_reconfigure=True)


def experiment_with_no_compaction(pf, repeat):
    # No operations injected
    capacity_list = [1, 5, 10, 20]
    total_num_records_list = [x * 1000000 for x in capacity_list]
    num_cassandra_nodes = int(pf.config.get('experiment', 'default_num_cassandra_nodes'))
    workload_type = pf.config.get('experiment', 'default_workload_type')
    replication_factor = int(pf.config.get('experiment', 'default_replication_factor'))
    measurement_type = pf.config.get('experiment', 'default_measurement_type')

    for run in range(repeat):
        for total_num_records in total_num_records_list:
            total_num_ycsb_threads = pf.get_max_num_connections_per_cassandra_node() * num_cassandra_nodes
            num_ycsb_nodes = total_num_ycsb_threads / pf.get_max_allowed_num_ycsb_threads_per_node() + 1
            logger.debug('total_num_records=%d' % total_num_records)

            result = run_experiment(pf,
                                    hosts=pf.get_hosts(),
                                    overall_target_throughput=None,
                                    total_num_records=total_num_records,
                                    workload_type=workload_type,
                                    replication_factor=replication_factor,
                                    num_cassandra_nodes=num_cassandra_nodes,
                                    num_ycsb_nodes=num_ycsb_nodes,
                                    total_num_ycsb_threads=total_num_ycsb_threads,
                                    workload_proportions=None,
                                    measurement_type=measurement_type,
                                    should_inject_operations=False,
                                    should_reconfigure=True,
                                    should_compact=False)


def main():
    profile_name = sys.argv[1]
    pf = profile.get_profile(profile_name)

    # Cleanup existing result directory and create a new one
    result_file_name = strftime('%m-%d-%H%M') + '.tar.gz'
    result_base_path = pf.config.get('path', 'result_base_path')
    os.system('rm -rf %s;mkdir %s' % (result_base_path, result_base_path))

    repeat = int(pf.config.get('experiment', 'repeat'))

    # Do all experiments here
    # workload_proportions = {'read': 4, 'update': 4, 'insert': 2}
    # run_experiment(pf, pf.get_hosts(), 100, 'uniform', 1000000, 1, 3, 1, 48, workload_proportions, 'histogram')
    # run_experiment(pf, pf.get_hosts(), 100, 'uniform', 1000000, 1, 3, 1, 48, {'read': 10, 'update': 0, 'insert': 0}, 'timeseries')

    experiment_on_workloads(pf, repeat)
    experiment_on_num_cassandra_nodes(pf, repeat)
    experiment_on_num_records(pf, repeat)
    experiment_on_replication_factors(pf, repeat)
    experiment_on_operations_rate(pf, repeat)
    experiment_with_no_compaction(pf, repeat)

    # Copy log to result directory
    os.system('cp %s/morphus-cassandra-log.txt %s/' % (pf.get_log_path(), result_base_path))

    # Archive the result and send to remote server
    os.system('tar -czf /tmp/%s -C %s .'
              % (result_file_name, result_base_path))
    private_key_path = pf.config.get('path', 'private_key_path')
    os.system('scp -o StrictHostKeyChecking=no -P8888 -i %s/sshuser_key /tmp/%s sshuser@104.236.110.182:morphus-cassandra/%s/'
              % (private_key_path, result_file_name, pf.get_name()))
    os.system('rm /tmp/%s' % result_file_name)


if __name__ == "__main__":
    main()
