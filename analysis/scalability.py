import os
import time
import pandas as pd
import ConfigParser
import numpy as np
import re
import ycsb_parser
import time
import json

data_base_path = os.path.abspath('../../../experiment/scalability')

output_dir_name = time.strftime('%m-%d-%H%M')
output_dir_path = '%s/processed/%s' % (data_base_path, output_dir_name)
try:
    os.mkdir(output_dir_path)
except:
    pass

raw_data_root = '%s/raw' % data_base_path

rows = []
for dir_name in os.listdir(raw_data_root):
    if re.search('[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9][0-9][0-9]$', dir_name) is not None:
        cur_dir_path = '%s/%s' % (raw_data_root, dir_name)

        print dir_name

        meta = ConfigParser.SafeConfigParser()
        meta.read('%s/meta.ini' % cur_dir_path)
        flat_dict = meta._sections['config']
        flat_dict.update(meta._sections['result'])

        rows.append(flat_dict)

df = pd.DataFrame(rows)
df = df.fillna(value={'target_throughput': '0'})
df = df.dropna(subset=['catchupmorphustask'])
df.to_csv('%s/data.csv' % output_dir_path, index=False)

df = pd.read_csv('%s/data.csv' % output_dir_path)

# Number of Cassandra Nodes
num_cassandra_nodes_df = df[(df['num_records'] == 1000000) & (df['replication_factor'] == 1) & (df['should_inject_operations'] == False)]
num_cassandra_nodes_df['morphus_duration'] = num_cassandra_nodes_df['catchupmorphustask'] - num_cassandra_nodes_df['morphusstartat']
group_by_df = num_cassandra_nodes_df.groupby('num_cassandra_nodes')
num_cassandra_nodes_aggregated = group_by_df['morphus_duration'].agg([np.mean, np.std])
output_path = '%s/scalability-num-cassandra-nodes.png' % output_dir_path
csv_path = '%s/scalability-num-cassandra-nodes.csv' % output_dir_path
num_cassandra_nodes_aggregated.to_csv(csv_path, header=False)
os.system('./plot-num-cassandra-nodes-vs-reconfig-time.sh --output_path=%s --csv_path=%s' % (output_path, csv_path))


# Replication Factors
num_cassandra_nodes_df = df[(df['num_cassandra_nodes'] == 10) & (df['num_records'] == 1000000) & (df['should_inject_operations'] == False)]
num_cassandra_nodes_df['morphus_duration'] = num_cassandra_nodes_df['catchupmorphustask'] - num_cassandra_nodes_df['morphusstartat']
group_by_df = num_cassandra_nodes_df.groupby('replication_factor')
num_cassandra_nodes_aggregated = group_by_df['morphus_duration'].agg([np.mean, np.std])
output_path = '%s/scalability-replication-factor.png' % output_dir_path
csv_path = '%s/scalability-replication-factor.csv' % output_dir_path
num_cassandra_nodes_aggregated.to_csv(csv_path, header=False)
os.system('./plot-replication-factor-vs-reconfig-time.sh --output_path=%s --csv_path=%s' % (output_path, csv_path))


# Number of Records
num_cassandra_nodes_df = df[(df['num_cassandra_nodes'] == 10) & (df['replication_factor'] == 1) & (df['should_inject_operations'] == False)]
num_cassandra_nodes_df['morphus_duration'] = num_cassandra_nodes_df['catchupmorphustask'] - num_cassandra_nodes_df['morphusstartat']
group_by_df = num_cassandra_nodes_df.groupby('num_records')
num_cassandra_nodes_aggregated = group_by_df['morphus_duration'].agg([np.mean, np.std])
output_path = '%s/scalability-num-records.png' % output_dir_path
csv_path = '%s/scalability-num-records.csv' % output_dir_path
num_cassandra_nodes_aggregated.to_csv(csv_path, header=False)
os.system('./plot-num-records-vs-reconfig-time.sh --output_path=%s --csv_path=%s' % (output_path, csv_path))


# Operations Rates
num_cassandra_nodes_df = df[(df['num_cassandra_nodes'] == 10) & (df['num_records'] == 1000000) & (df['replication_factor'] == 1)]
num_cassandra_nodes_df['morphus_duration'] = num_cassandra_nodes_df['catchupmorphustask'] - num_cassandra_nodes_df['morphusstartat']
group_by_df = num_cassandra_nodes_df.groupby('target_throughput')
num_cassandra_nodes_aggregated = group_by_df['morphus_duration'].agg([np.mean, np.std])
output_path = '%s/scalability-operations-rate.png' % output_dir_path
csv_path = '%s/scalability-operations-rate.csv' % output_dir_path
num_cassandra_nodes_aggregated.to_csv(csv_path, header=False)
os.system('./plot-operations-rate-vs-reconfig-time.sh --output_path=%s --csv_path=%s' % (output_path, csv_path))
