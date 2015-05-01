import ConfigParser
import os

__author__ = 'Daniel'


class BaseProfile(object):
    def __init__(self):
        self.config = ConfigParser.SafeConfigParser()
        pass


class EmulabProfile(BaseProfile):
    def __init__(self):
        BaseProfile.__init__(self)
        self.config.read('emulab-config.ini')

    def get_hosts(self):
        hosts = set()
        f = open('/etc/hosts')
        lines = f.read().splitlines()
        for line in lines:
            tokens = line.split()
            if len(tokens) < 4:
                continue
            elif tokens[3].find('node') == -1:
                continue
            else:
                host = tokens[3]
                hosts.add(host)
        return list(hosts)

    def get_name(self):
        return 'emulab'

    def get_log_path(self):
        return '/tmp'

    def get_heuristic_target_throughput(self, num_cassandra_nodes):
        single_node_throughput = 10000
        # Dividing by 2 because two ycsb processes are run for two different schemas
        return single_node_throughput / 2

    def get_max_num_cassandra_nodes(self):
        return 19

    def get_max_num_ycsb_nodes(self):
        return 1

    def get_max_allowed_num_ycsb_threads_per_node(self):
        return 350

    def get_max_num_connections_per_cassandra_node(self):
        return 16  # 8 connections per core (according to Solving Big Data Challenges paper) * 4 cores / 2 (two different schemas)

    def get_default_workload_proportions(self):
        return {'read': 4, 'update': 4, 'insert': 2}



def get_profile(profile_name):
    if profile_name == 'emulab':
        pf = EmulabProfile()
    else:
        raise Exception('Specify which profile to use...')
    return pf