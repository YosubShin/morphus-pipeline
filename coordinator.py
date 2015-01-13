import os
from time import strftime
import subprocess
import ConfigParser
import StringIO
import traceback
import pandas as pd
from twilio.rest import TwilioRestClient

private_config = ConfigParser.SafeConfigParser()
private_config.read('private.ini')
tc = TwilioRestClient(private_config.get('twilio', 'account_sid'), private_config.get('twilio', 'auth_token'))

wokload_types = ['uniform', 'zipfian', 'latest', 'readonly']
local_result_path = '/Users/Daniel/Dropbox/Illinois/research/experiment'
local_raw_result_path = local_result_path + '/raw'
local_processed_result_path = local_result_path + '/processed'


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

    config = ConfigParser.ConfigParser()
    try:
        out = subprocess.check_output(('ssh yossupp@node-0.cassandra-morphous.ISS.emulab.net \'cat %s/result.ini\'' % base_directory_path), shell=True)
        buf = StringIO.StringIO(out)
        config.readfp(buf)
        assert config.has_option('result', 'morphousStartAt')
        assert config.has_option('result', 'CompactMorphousTask')
        assert config.has_option('result', 'InsertMorphousTask')
        assert config.has_option('result', 'AtomicSwitchMorphousTask')
        assert config.has_option('result', 'CatchupMorphousTask')
    finally:
        os.system('ssh yossupp@node-0.cassandra-morphous.ISS.emulab.net \'cp -r /var/log/cassandra %s/logs\'' % base_directory_path)
        result = dict(config.items('result'))
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

default_cluster_size = 15
default_active_cluster_size = 10
default_num_threads = 1
default_num_records = 1000
default_workload_type = 'uniform'
default_no_reconfiguration = False
default_replication_factor = 1

# Throughput on different workloads
def experiment_on_workloads():
    csv_file_name = '%s/%s-workloads.csv' % (local_processed_result_path, strftime('%m-%d-%H%M'))
    rows = []
    for run in range(3):
        for workload_type in wokload_types:
            result = run_experiment(cluster_size=default_cluster_size,
                                    active_cluster_size=default_active_cluster_size,
                                    num_threads=default_num_threads,
                                    num_records=default_num_records,
                                    workload_type=workload_type,
                                    no_reconfiguration=default_no_reconfiguration,
                                    replication_factor=default_replication_factor)
            rows.append(result)
            df = pd.DataFrame(rows)
            df.to_csv(csv_file_name)

try:
    experiment_on_workloads()
except Exception, e:
    tc.messages.create(from_=private_config.get('personal', 'twilio_number'),
                       to=private_config.get('personal', 'phone_number'),
                       body='Exp. failed w/:\n%s' % str(e))
