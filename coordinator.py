import os
from time import strftime, sleep
import ConfigParser
from threading import Thread
import thread
import sys
import profile
import logging
import subprocess
import cassandra_log_parser as ps

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

workload_types = ['uniform', 'zipfian', 'latest', 'readonly']


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
                   measurement_type='histogram'):
    cassandra_path = pf.config.get('path', 'cassandra_path')
    cassandra_home_base_path = pf.config.get('path', 'cassandra_home_base_path')
    ycsb_path = pf.config.get('path', 'ycsb_path')
    java_path = pf.config.get('path', 'java_path')
    result_base_path = pf.config.get('path', 'result_base_path')

    result_dir_name = strftime('%m-%d-%H%M')
    result_path = '%s/%s' % (result_base_path, result_dir_name)
    logger.debug('Executing w/ pf=%s, num_hosts=%d, overall_target_throughput=%d, workload_type=%s, '
                 'num_records=%d, replication_factor=%d, num_cassandra_nodes=%d, result_dir_name=%s, '
                 'num_ycsb_nodes=%d, total_num_ycsb_threads=%d, workload_proportions=%s, measurement_type=%s' %
                 (pf.get_name(), len(hosts), int(overall_target_throughput or -1), workload_type,
                  total_num_records, replication_factor, num_cassandra_nodes, result_dir_name,
                  num_ycsb_nodes, total_num_ycsb_threads, str(workload_proportions), measurement_type))

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

    assert workload_proportions.has_key('read') and \
           workload_proportions.has_key('insert') and \
           workload_proportions.has_key('update')

    num_ycsb_threads = total_num_ycsb_threads / num_ycsb_nodes
    ret = os.system('ssh %s \'sh %s/ycsb-load.sh '
                    '--cassandra_path=%s --ycsb_path=%s '
                    '--base_path=%s --num_records=%d --workload=%s '
                    '--replication_factor=%d --seed_host=%s --hosts=%s --num_threads=%d '
                    '--read_proportion=%d --insert_proportion=%d --update_proportion=%d \''
                    % (hosts[num_cassandra_nodes], src_path, cassandra_path, ycsb_path,
                       result_path, total_num_records, workload_type,
                       replication_factor, seed_host, cassandra_nodes_hosts, num_ycsb_threads,
                       workload_proportions['read'], workload_proportions['insert'], workload_proportions['update']))
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

    threads = []
    output = []
    mutex = thread.allocate_lock()

    sleep(10)

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
    logger.debug('Running Morphus script at host %s' % hosts[0])
    # os.system('ssh %s \'sh %s/lsof.sh \'' % (host, src_path))
    os.system('%s/bin/nodetool -h %s -m \'{"column":"%s"}\' morphous %s %s' %
              (cassandra_path, hosts[0], 'field0', 'ycsb', 'usertable'))

    for t in threads:
        t.join()

    logger.debug('Threads finished executing with outputs: %s...' % output)

    logger.debug('Collecting Morphus result from host %s...' % hosts[0])
    cassandra_home = '%s/%s' % (cassandra_home_base_path, hosts[0])
    # os.system('ssh %s \'cat %s/log/system.log\'' % (hosts[0], cassandra_home))
    remote_log_file_path = cassandra_home + '/log/system.log'
    (stdout, stderr) = subprocess.Popen(['ssh', hosts[0], 'cat', remote_log_file_path], stdout=subprocess.PIPE).communicate()
    cassandra_log = open('%s/cassandra.log' % result_path, 'w')
    cassandra_log.write(stdout)
    cassandra_log.close()

    parser = ps.CassandraLogParser(stdout)
    result_dict = parser.parse()
    logger.debug('Morphous result:%s' % str(result_dict))
    if len(result_dict) < 4:
        logger.error('Morphus script not ended completely')
    else:
        for k, v in result_dict.iteritems():
            meta.set('result', k, v)

    meta_file = open('%s/meta.ini' % result_path, 'w')
    meta.write(meta_file)
    meta_file.close()


# differ throughputs
def experiment_on_throughputs(pf, num_cassandra_nodes, repeat):
    default_num_records = int(pf.config.get('experiment', 'default_num_records'))
    default_workload_type = pf.config.get('experiment', 'default_workload_type')
    default_replication_factor = int(pf.config.get('experiment', 'default_replication_factor'))

    target_throughputs = pf.get_heuristic_target_throughputs(num_cassandra_nodes)

    for run in range(repeat):
        for target_throughput in target_throughputs:
            total_num_ycsb_threads = pf.get_max_num_connections_per_cassandra_node() * num_cassandra_nodes
            total_num_records = default_num_records * num_cassandra_nodes
            num_ycsb_nodes = total_num_ycsb_threads / pf.get_max_allowed_num_ycsb_threads_per_node() + 1
            logger.debug('num_cassandra_nodes=%d, total_num_ycsb_threads=%d, num_ycsb_nodes=%d, total_num_records=%d'
                         % (num_cassandra_nodes, total_num_ycsb_threads, num_ycsb_nodes, total_num_records))

            result = run_experiment(pf,
                                    hosts=pf.get_hosts(),
                                    overall_target_throughput=target_throughput,
                                    total_num_records=default_num_records * num_cassandra_nodes,
                                    workload_type=default_workload_type,
                                    replication_factor=default_replication_factor,
                                    num_cassandra_nodes=num_cassandra_nodes,
                                    num_ycsb_nodes=num_ycsb_nodes,
                                    total_num_ycsb_threads=total_num_ycsb_threads)


def experiment_on_num_cassandra_nodes_and_throughput(pf, repeat):
    for run in range(repeat):
        for num_cassandra_nodes in range(1, pf.get_max_num_cassandra_nodes() + 1, 4):
            experiment_on_throughputs(pf, num_cassandra_nodes, 1)


def experiment_on_num_cassandra_nodes_with_no_throughput_limit(pf, repeat):
    default_num_records = int(pf.config.get('experiment', 'default_num_records'))
    default_workload_type = pf.config.get('experiment', 'default_workload_type')
    default_replication_factor = int(pf.config.get('experiment', 'default_replication_factor'))
    hosts = pf.get_hosts()
    for run in range(repeat):
        for num_cassandra_nodes in [1, 5]:
            result = run_experiment(pf,
                                    hosts=hosts,
                                    overall_target_throughput=None,
                                    total_num_records=default_num_records,
                                    workload_type=default_workload_type,
                                    replication_factor=default_replication_factor,
                                    num_cassandra_nodes=num_cassandra_nodes,
                                    num_ycsb_nodes=len(hosts) - pf.get_max_num_cassandra_nodes())


def experiment_on_num_ycsb_threads(pf):
    default_num_records = int(pf.config.get('experiment', 'default_num_records'))
    default_workload_type = pf.config.get('experiment', 'default_workload_type')
    default_replication_factor = int(pf.config.get('experiment', 'default_replication_factor'))
    hosts = pf.get_hosts()
    for num_cassandra_nodes in [1, 5, 10, 15]:
        num_threads_lower_bound = num_cassandra_nodes * 100 + 1
        num_threads_high_bound = num_cassandra_nodes * 300 + 1
        for total_num_ycsb_threads in range(num_threads_lower_bound, num_threads_high_bound,
                                            (num_threads_high_bound - num_threads_lower_bound) / 10):
            num_ycsb_nodes = total_num_ycsb_threads / pf.get_max_allowed_num_ycsb_threads_per_node() + 1
            logger.debug('num_cassandra_nodes=%d, total_num_ycsb_threads=%d, num_ycsb_nodes=%d' % (
                num_cassandra_nodes, total_num_ycsb_threads, num_ycsb_nodes))
            if num_ycsb_nodes > pf.get_max_num_ycsb_nodes():
                logger.debug(
                    '%d exceeds number of allowed ycsb nodes of %d...' % (num_ycsb_nodes, pf.get_max_num_ycsb_nodes()))
                continue
            result = run_experiment(pf,
                                    hosts=hosts,
                                    overall_target_throughput=None,
                                    total_num_records=default_num_records * num_cassandra_nodes,
                                    workload_type=default_workload_type,
                                    replication_factor=default_replication_factor,
                                    num_cassandra_nodes=num_cassandra_nodes,
                                    num_ycsb_nodes=num_ycsb_nodes,
                                    total_num_ycsb_threads=total_num_ycsb_threads)


def experiment_on_latency_scalability(pf):
    default_num_records = int(pf.config.get('experiment', 'default_num_records'))
    default_workload_type = pf.config.get('experiment', 'default_workload_type')
    default_replication_factor = int(pf.config.get('experiment', 'default_replication_factor'))
    hosts = pf.get_hosts()

    for i in range(3):
        for num_cassandra_nodes in [1, 5, 10, 15]:
            total_num_ycsb_threads = pf.get_max_num_connections_per_cassandra_node() * num_cassandra_nodes
            total_num_records = default_num_records * num_cassandra_nodes
            num_ycsb_nodes = total_num_ycsb_threads / pf.get_max_allowed_num_ycsb_threads_per_node() + 1
            logger.debug('num_cassandra_nodes=%d, total_num_ycsb_threads=%d, num_ycsb_nodes=%d, total_num_records=%d'
                         % (num_cassandra_nodes, total_num_ycsb_threads, num_ycsb_nodes, total_num_records))

            result = run_experiment(pf,
                                    hosts=hosts,
                                    overall_target_throughput=None,
                                    total_num_records=total_num_records,
                                    workload_type=default_workload_type,
                                    replication_factor=default_replication_factor,
                                    num_cassandra_nodes=num_cassandra_nodes,
                                    num_ycsb_nodes=num_ycsb_nodes,
                                    total_num_ycsb_threads=total_num_ycsb_threads)



def main():
    profile_name = sys.argv[1]
    pf = profile.get_profile(profile_name)

    # Cleanup existing result directory and create a new one
    result_file_name = strftime('%m-%d-%H%M') + '.tar.gz'
    result_base_path = pf.config.get('path', 'result_base_path')
    os.system('rm -rf %s;mkdir %s' % (result_base_path, result_base_path))

    repeat = int(pf.config.get('experiment', 'repeat'))

    # Do all experiments here
    # experiment_on_throughputs(pf, int(pf.config.get('experiment', 'default_num_cassandra_nodes')), repeat)
    # experiment_on_num_cassandra_nodes_and_throughput(pf, repeat)
    # experiment_on_num_cassandra_nodes_with_no_throughput_limit(pf, repeat)
    # experiment_on_num_ycsb_threads(pf)
    # experiment_on_latency_scalability(pf)

    workload_distribution = {'read': 4, 'update': 4, 'insert': 2}
    run_experiment(pf, pf.get_hosts(), 50000, 'uniform', 1000000, 1, 10, 1, 160, workload_distribution, 'histogram')
    run_experiment(pf, pf.get_hosts(), 50000, 'uniform', 1000000, 1, 10, 1, 160, workload_distribution, 'timeseries')

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
