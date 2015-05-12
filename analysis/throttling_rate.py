import os
import time
import pandas as pd
import ConfigParser
import numpy as np
import re
import ycsb_parser
import time
import json

data_base_path = os.path.abspath('../../../experiment/throttle-rate')

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

rows = []
for dir_name in os.listdir(raw_data_root):
    if re.search('[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9][0-9][0-9]$', dir_name) is not None:
        cur_dir_path = '%s/%s' % (raw_data_root, dir_name)

        print dir_name

        meta = ConfigParser.SafeConfigParser()
        meta.read('%s/meta.ini' % cur_dir_path)
        config_dict = meta._sections['config']
        config_dict['result_dir_name'] = dir_name

        if meta.get('config', 'should_reconfigure') == 'True' and not meta.has_option('result', 'MorphusStartAt'):
            print 'Morphus failed for ', dir_name
            continue

        output_fs = filter(lambda x: x.find('output-') != -1 and x.find('stderr') == -1, os.listdir(cur_dir_path))
        if meta.get('config', 'measurement_type') != 'timeseries':
            continue

        should_reconfigure = meta.get('config', 'should_reconfigure') == 'True'
        is_read_only = json.loads(meta.get('config', 'workload_proportions').replace("'", '"'))['read'] == 10
        paths = {}
        rws = ['read', 'update']
        for rw in rws:
            paths[rw] = {}
        abs_morphus_start_at = 0
        insert_morphus_task = 0
        atomicswitch_morphus_task = 0
        catchup_morphus_task = 0
        morphus_start_at = 0
        workload_type = ''

        if is_read_only:
            continue
        if not should_reconfigure:
            continue
        workload_type = meta.get('config', 'workload_type')
        if workload_type != 'uniform':
            continue

        abs_morphus_start_at = int(meta.get('result', 'MorphusStartAt'))
        if not meta.has_option('result', 'CatchupMorphusTask'):
            print 'Incomplete Morphus script for %s' % dir_name
            continue
        atomicswitch_morphus_task = int(meta.get('result', 'AtomicSwitchMorphusTask')) - abs_morphus_start_at
        catchup_morphus_task = int(meta.get('result', 'CatchupMorphusTask')) - abs_morphus_start_at

        rw_histograms = {}
        for fname in output_fs:
            f = open('%s/%s' % (cur_dir_path, fname))

            f_buf = f.read()
            for rw in rws:
                df = ycsb_parser.parse_timeseries(f_buf, rw)
                # print cur_dir_path, fname, rw, df.describe()
                df['0time'] -= abs_morphus_start_at
                df['1latency'] = df['1latency'].apply(lambda x: -1000000 if x < 0 else x)

                # Read & Write Timeseries
                key = 'original' if fname.find('original') != -1 else 'altered'

                # Availability
                if key == 'original':
                    filtered_df = df[(df['0time'] > morphus_start_at) & (df['0time'] < atomicswitch_morphus_task)]
                else:
                    filtered_df = df[(df['0time'] > atomicswitch_morphus_task) & (df['0time'] < catchup_morphus_task)]

                succeeded_filtered_df = filtered_df[(filtered_df['1latency'] != -1000000)]
                if rw in rw_histograms:
                    rw_histograms[rw] = rw_histograms[rw].append(succeeded_filtered_df)
                else:
                    rw_histograms[rw] = succeeded_filtered_df

            f.close()

        num_threads = meta.getint('config', 'num_morphus_mutation_sender_threads')
        row_dict = {'num_threads': num_threads, 'reconfig_time': catchup_morphus_task / 1000}

        percentiles = [0.5, 0.95]
        for rw in rws:
            percentile_series = rw_histograms[rw]['1latency'].quantile(percentiles)
            for percentile in percentiles:
                row_dict['%s_%.2fp' % (rw, percentile)] = percentile_series[percentile]

        rows.append(row_dict)

df = pd.DataFrame(rows)
print df

csv_path = '%s/throttle_rate.csv' % output_dir_path
aggregated_df = df.groupby('num_threads', sort=True).agg(['mean', 'std'])
print aggregated_df

aggregated_df.to_csv(csv_path)

output_path = '%s/throttle_rate_vs_reconfig_time_and_latencies.png' % output_dir_path

os.system('./plot-throttle-rate-vs-reconfig-time-and-latencies.sh --output_path=%s --csv_path=%s' %
          (output_path, csv_path))
