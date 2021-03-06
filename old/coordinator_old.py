import os
from time import strftime
import subprocess
import ConfigParser
import StringIO
import pandas as pd
from twilio.rest import TwilioRestClient

config = ConfigParser.SafeConfigParser()
config.read('config.ini')

private_config = ConfigParser.SafeConfigParser()
private_config.read('private.ini')
tc = TwilioRestClient(private_config.get('twilio', 'account_sid'), private_config.get('twilio', 'auth_token'))

wokload_types = ['uniform', 'zipfian', 'latest', 'readonly']
local_result_path = config.get('path', 'local_result_path')
local_raw_result_path = local_result_path + '/raw'
local_processed_result_path = local_result_path + '/processed'

default_cluster_size = int(config.get('experiment', 'default_cluster_size'))
default_active_cluster_size = int(config.get('experiment', 'default_active_cluster_size'))
default_num_threads = int(config.get('experiment', 'default_num_threads'))
default_num_records = int(config.get('experiment', 'default_num_records'))
default_workload_type = config.get('experiment', 'default_workload_type')
default_no_reconfiguration = (config.get('experiment', 'default_no_reconfiguration') == 'True')
default_replication_factor = int(config.get('experiment', 'default_replication_factor'))


def run_experiment(cluster_size, active_cluster_size, num_threads, num_records, workload_type, no_reconfiguration, replication_factor):
    # Executing hard reset of Cassandra cluster on Emulab
    print 'Executing hard reset of Cassandra cluster on Emulab'
    ret = os.system('./remote-deploy-cassandra-cluster.sh --mode=hard --cluster_size=%d --active_cluster_size=%d --rebuild=false' % (cluster_size, active_cluster_size))
    if ret != 0:
        raise Exception('Unable to finish remote-deploy-cassandra-cluster.sh')

    base_directory_name = strftime('%m-%d-%H%M')
    base_directory_path = '/tmp/' + base_directory_name
    # SSH into server and run YCSB script
    print 'SSH into server and run YCSB script'
    ret = os.system('ssh yossupp@node-0.cassandra-morphous.ISS.emulab.net \'java -jar /tmp/morphous-script-1.0-SNAPSHOT-jar-with-dependencies.jar --basedir=%s --thread=%d --records=%d --workload=%s --profile=%s --no_reconfiguration=%s --replication_factor=%d\''
                    % (base_directory_path, num_threads, num_records, workload_type, 'emulab', str(no_reconfiguration), replication_factor))
    if ret != 0:
        raise Exception('Unable to finish morphous-script-1.0-SNAPSHOT-jar-with-dependencies.jar')

    result_ini = ConfigParser.ConfigParser()
    try:
        out = subprocess.check_output(('ssh yossupp@node-0.cassandra-morphous.ISS.emulab.net \'cat %s/result.ini\'' % base_directory_path), shell=True)
        buf = StringIO.StringIO(out)
        result_ini.readfp(buf)
        if not no_reconfiguration:
            assert result_ini.has_option('result', 'morphousStartAt')
            assert result_ini.has_option('result', 'CompactMorphousTask')
            assert result_ini.has_option('result', 'InsertMorphousTask')
            assert result_ini.has_option('result', 'AtomicSwitchMorphousTask')
            assert result_ini.has_option('result', 'CatchupMorphousTask')
    finally:
        os.system('ssh yossupp@node-0.cassandra-morphous.ISS.emulab.net \'cp -r /var/log/cassandra %s/logs\'' % base_directory_path)
        result = dict(result_ini.items('result'))
        if no_reconfiguration:
            result['morphousStartAt'] = ''
            result['CompactMorphousTask'] = ''
            result['InsertMorphousTask'] = ''
            result['AtomicSwitchMorphousTask'] = ''
            result['CatchupMorphousTask'] = ''
        os.system('ssh yossupp@node-0.cassandra-morphous.ISS.emulab.net \'tar -czf %s.tgz -C %s %s \'' % (base_directory_path, '/tmp', base_directory_name))
        os.system('scp yossupp@node-0.cassandra-morphous.ISS.emulab.net:%s.tgz %s/' % (base_directory_path, local_raw_result_path))
        os.system('tar -xzf %s/%s.tgz -C %s/' % (local_raw_result_path, base_directory_name, local_raw_result_path))

    result['base_directory_name'] = base_directory_name
    result['workload_type'] = workload_type
    result['num_records'] = num_records
    result['num_threads'] = num_threads
    result['num_nodes'] = active_cluster_size
    result['is_reconfigured'] = str(not no_reconfiguration)
    result['replication_factor'] = replication_factor
    return result


# Throughput on different workloads changing workloads
def experiment_on_workloads(csv_file_name, repeat):
    for run in range(repeat):
        # Different workloads under reconfiguration
        for workload_type in wokload_types:
            result = run_experiment(cluster_size=default_cluster_size,
                                    active_cluster_size=default_active_cluster_size,
                                    num_threads=default_num_threads,
                                    num_records=default_num_records,
                                    workload_type=workload_type,
                                    no_reconfiguration=default_no_reconfiguration,
                                    replication_factor=default_replication_factor)
            append_row_to_csv(csv_file_name, result)
        # workload under no reconfiguration
        result = run_experiment(cluster_size=default_cluster_size,
                                active_cluster_size=default_active_cluster_size,
                                num_threads=default_num_threads,
                                num_records=default_num_records,
                                workload_type=default_workload_type,
                                no_reconfiguration=True,
                                replication_factor=default_replication_factor)
        append_row_to_csv(csv_file_name, result)


# Scalability on different number of machines.
# Don't do any operation(by setting number of YCSB thread to be 0)
def experiment_on_num_nodes(csv_file_name, repeat):
    num_nodes = [2, 5, 10, 15]
    for run in range(repeat):
        for num_node in num_nodes:
            result = run_experiment(cluster_size=default_cluster_size,
                                    active_cluster_size=num_node,
                                    num_threads=0,
                                    num_records=default_num_records,
                                    workload_type=default_workload_type,
                                    no_reconfiguration=default_no_reconfiguration,
                                    replication_factor=default_replication_factor)
            append_row_to_csv(csv_file_name, result)


def experiment_on_replication_factor(csv_file_name, repeat):
    replication_factors = [1, 2, 3, 4]
    for run in range(repeat):
        for replication_factor in replication_factors:
            result = run_experiment(cluster_size=default_cluster_size,
                                    active_cluster_size=default_active_cluster_size,
                                    num_threads=0,
                                    num_records=default_num_records,
                                    workload_type=default_workload_type,
                                    no_reconfiguration=default_no_reconfiguration,
                                    replication_factor=replication_factor)
            append_row_to_csv(csv_file_name, result)


def experiment_on_num_threads(csv_file_name, repeat):
    num_threads = [1, 2, 3, 4]
    for run in range(repeat):
        for num_thread in num_threads:
            result = run_experiment(cluster_size=default_cluster_size,
                                    active_cluster_size=default_active_cluster_size,
                                    num_threads=num_thread,
                                    num_records=default_num_records,
                                    workload_type=default_workload_type,
                                    no_reconfiguration=default_no_reconfiguration,
                                    replication_factor=default_replication_factor)
            append_row_to_csv(csv_file_name, result)


def experiment_on_num_records(csv_file_name, repeat):
    num_records = [100000, 3000000, 5000000, 10000000]
    for run in range(repeat):
        for num_record in num_records:
            result = run_experiment(cluster_size=default_cluster_size,
                                    active_cluster_size=default_active_cluster_size,
                                    num_threads=0,
                                    num_records=num_record,
                                    workload_type=default_workload_type,
                                    no_reconfiguration=default_no_reconfiguration,
                                    replication_factor=default_replication_factor)
            append_row_to_csv(csv_file_name, result)


def append_row_to_csv(csv_file_name, row):
    df = pd.DataFrame([row])
    if os.path.isfile(csv_file_name):
        with open(csv_file_name, 'a') as f:
            df.to_csv(f, header=False)
    else:
        df.to_csv(csv_file_name)


def main():
    try:
        csv_file_name = '%s/%s.csv' % (local_processed_result_path, strftime('%m-%d-%H%M'))
        repeat = int(config.get('experiment', 'repeat'))

        # experiment_on_workloads(csv_file_name, repeat)
        experiment_on_num_nodes(csv_file_name, repeat)
        experiment_on_replication_factor(csv_file_name, repeat)
        experiment_on_num_threads(csv_file_name, repeat)
        experiment_on_num_records(csv_file_name, repeat)

    except Exception, e:
        tc.messages.create(from_=private_config.get('personal', 'twilio_number'),
                           to=private_config.get('personal', 'phone_number'),
                           body='Exp. failed w/:\n%s' % str(e))
        raise
    tc.messages.create(from_=private_config.get('personal', 'twilio_number'),
                       to=private_config.get('personal', 'phone_number'),
                       body='Experiment done!')

if __name__ == "__main__":
    main()
