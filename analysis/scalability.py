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
df = df.fillna(value={'should_compact': 'True'})
df.to_csv('%s/data.csv' % output_dir_path, index=False)

df = pd.read_csv('%s/data.csv' % output_dir_path)
df = df.dropna(subset=['catchupmorphustask'])

# Number of Cassandra Nodes
raw_df = df[(df['num_records'] == 1000000) & (df['replication_factor'] == 1) & (df['should_inject_operations'] == False) & (df['should_compact'] == True)]
raw_df['morphus_duration'] = raw_df['catchupmorphustask'] - raw_df['morphusstartat']
group_by_df = raw_df.groupby('num_cassandra_nodes')
processed_df = group_by_df['morphus_duration'].agg([np.mean, np.std])
output_path = '%s/scalability-num-cassandra-nodes.png' % output_dir_path
csv_path = '%s/scalability-num-cassandra-nodes.csv' % output_dir_path
processed_df.to_csv(csv_path, header=False)
os.system('./plot-num-cassandra-nodes-vs-reconfig-time.sh --output_path=%s --csv_path=%s' % (output_path, csv_path))


# Replication Factors
raw_df = df[(df['num_cassandra_nodes'] == 10) & (df['num_records'] == 1000000) & (df['should_inject_operations'] == False) & (df['should_compact'] == True)]
raw_df['morphus_duration'] = raw_df['catchupmorphustask'] - raw_df['morphusstartat']
group_by_df = raw_df.groupby('replication_factor')
processed_df = group_by_df['morphus_duration'].agg([np.mean, np.std])
output_path = '%s/scalability-replication-factor.png' % output_dir_path
csv_path = '%s/scalability-replication-factor.csv' % output_dir_path
processed_df.to_csv(csv_path, header=False)
os.system('./plot-replication-factor-vs-reconfig-time.sh --output_path=%s --csv_path=%s' % (output_path, csv_path))


# Number of Records
raw_df = df[(df['num_cassandra_nodes'] == 10) & (df['replication_factor'] == 1) & (df['should_inject_operations'] == False) & (df['should_compact'] == True)]
raw_df['morphus_duration'] = raw_df['catchupmorphustask'] - raw_df['morphusstartat']
group_by_df = raw_df.groupby('num_records')
processed_df = group_by_df['morphus_duration'].agg([np.mean, np.std])
output_path = '%s/scalability-num-records.png' % output_dir_path
csv_path = '%s/scalability-num-records.csv' % output_dir_path
processed_df.to_csv(csv_path, header=False)
os.system('./plot-num-records-vs-reconfig-time.sh --output_path=%s --csv_path=%s' % (output_path, csv_path))


# Operations Rates
raw_df = df[(df['num_cassandra_nodes'] == 10) & (df['num_records'] == 1000000) & (df['replication_factor'] == 1) & (df['should_compact'] == True)]
raw_df['morphus_duration'] = raw_df['catchupmorphustask'] - raw_df['morphusstartat']
group_by_df = raw_df.groupby('target_throughput')
processed_df = group_by_df['morphus_duration'].agg([np.mean, np.std])
output_path = '%s/scalability-operations-rate.png' % output_dir_path
csv_path = '%s/scalability-operations-rate.csv' % output_dir_path
processed_df.to_csv(csv_path, header=False)
os.system('./plot-operations-rate-vs-reconfig-time.sh --output_path=%s --csv_path=%s' % (output_path, csv_path))


# Compaction and no compaction
raw_df = df[(df['num_cassandra_nodes'] == 10) & (df['replication_factor'] == 1) & (df['should_inject_operations'] == False) & (df['should_compact'] == False)]
raw_df['morphus_duration'] = raw_df['catchupmorphustask'] - raw_df['morphusstartat']
group_by_df = raw_df.groupby('num_records')
processed_df = group_by_df['morphus_duration'].agg([np.mean, np.std])
output_path = '%s/scalability-compaction.png' % output_dir_path
csv_path = '%s/scalability-num-records.csv' % output_dir_path
no_compaction_csv_path = '%s/scalability-num-records-no-compaction.csv' % output_dir_path
processed_df.to_csv(no_compaction_csv_path, header=False)
os.system('./plot-num-records-vs-reconfig-time-compaction.sh --output_path=%s --csv_path=%s --no_compaction_csv_path=%s' % (output_path, csv_path, no_compaction_csv_path))
