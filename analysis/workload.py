import os
import time
import pandas as pd
import ConfigParser
import numpy as np
import re
import ycsb_parser
import time
import json

data_base_path = os.path.abspath('../../../experiment/workload-10g-strong-consistency')

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


# Plot Read latency CDF
bucket_dicts = {}
for dir_name in os.listdir(raw_data_root):
    if re.search('[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9][0-9][0-9]$', dir_name) is not None:
        cur_dir_path = '%s/%s' % (raw_data_root, dir_name)

        print 'Read CDF', dir_name

        meta = ConfigParser.SafeConfigParser()
        meta.read('%s/meta.ini' % cur_dir_path)
        config_dict = meta._sections['config']
        config_dict['result_dir_name'] = dir_name

        output_fs = filter(lambda x: x.find('output-') != -1 and x.find('stderr') == -1, os.listdir(cur_dir_path))
        if meta.get('config', 'measurement_type') == 'histogram':
            workload_type = meta.get('config', 'workload_type')
            if json.loads(meta.get('config', 'workload_proportions').replace("'", '"'))['read'] == 10:
                if meta.get('config', 'should_reconfigure') == 'True':
                    workload_type = 'readonly'
                else:
                    workload_type = 'noreconfig'
            elif meta.get('config', 'should_reconfigure') == 'False':
                continue

            aggregate_dict = None
            for fname in output_fs:
                f = open('%s/%s' % (cur_dir_path, fname))
                bucket_dict = ycsb_parser.parse_latency_bucket(f.read(), 'read')
                if aggregate_dict is None:
                    aggregate_dict = bucket_dict
                else:
                    for key, value in bucket_dict.iteritems():
                        if key in aggregate_dict:
                            aggregate_dict[key] += value
                        else:
                            aggregate_dict[key] = value

            if workload_type not in bucket_dicts:
                bucket_dicts[workload_type] = aggregate_dict
            else:
                for key in aggregate_dict.keys():
                    dest_dict = bucket_dicts[workload_type]
                    if key in dest_dict:
                        dest_dict[key] += aggregate_dict[key]
                    else:
                        dest_dict[key] = aggregate_dict[key]

for workload_type in bucket_dicts.keys():
    aggregate_dict = bucket_dicts[workload_type]
    total_num_operations = reduce(lambda x, y: x + y, aggregate_dict.values())
    for key in aggregate_dict.keys():
        aggregate_dict[key] = float(aggregate_dict[key]) / total_num_operations
    rows = []
    for i, key in enumerate(sorted(aggregate_dict.keys())):
        if i == 0:
            rows.append({'0latency': key + 1, '1cumulative': aggregate_dict[key]})
        else:
            cum = rows[i - 1]['1cumulative'] + aggregate_dict[key]
            rows.append({'0latency': key + 1, '1cumulative': cum})
    df = pd.DataFrame(rows)
    df.to_csv('%s/histogram-read-%s.csv' % (output_dir_path, workload_type), header=False,
              index=False)

paths = filter(lambda x: re.search('.*histogram\-read\-.*\.csv', x) is not None,
               ['%s/%s' % (output_dir_path, x) for x in os.listdir(output_dir_path)])
readonly_paths = filter(lambda x: x.find('readonly') != -1, paths)
uniform_paths = filter(lambda x: x.find('uniform') != -1, paths)
latest_paths = filter(lambda x: x.find('latest') != -1, paths)
zipfian_paths = filter(lambda x: x.find('zipfian') != -1, paths)
noreconfig_paths = filter(lambda x: x.find('noreconfig') != -1, paths)

output_path = '%s/read-latency-cdf.png' % output_dir_path

os.system('./plot-read-latency-cdf.sh --output_path=%s '
          '--readonly=%s --uniform=%s --latest=%s --zipfian=%s --noreconfig=%s' %
          (output_path, readonly_paths[0], uniform_paths[0], latest_paths[0], zipfian_paths[0], noreconfig_paths[0]))

bucket_dicts = {}
# Plot update cdf
for dir_name in os.listdir(raw_data_root):
    if re.search('[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9][0-9][0-9]$', dir_name) is not None:
        cur_dir_path = '%s/%s' % (raw_data_root, dir_name)

        print dir_name

        meta = ConfigParser.SafeConfigParser()
        meta.read('%s/meta.ini' % cur_dir_path)
        config_dict = meta._sections['config']
        config_dict['result_dir_name'] = dir_name

        output_fs = filter(lambda x: x.find('output-') != -1 and x.find('stderr') == -1, os.listdir(cur_dir_path))
        if meta.get('config', 'measurement_type') == 'histogram':
            workload_type = meta.get('config', 'workload_type')
            if json.loads(meta.get('config', 'workload_proportions').replace("'", '"'))['read'] == 10:
                # No update operations for readonly workload
                continue
            elif meta.get('config', 'should_reconfigure') == 'False':
                workload_type = 'noreconfig'

            aggregate_dict = None
            for fname in output_fs:
                f = open('%s/%s' % (cur_dir_path, fname))
                bucket_dict = ycsb_parser.parse_latency_bucket(f.read(), 'update')
                if aggregate_dict is None:
                    aggregate_dict = bucket_dict
                else:
                    for key, value in bucket_dict.iteritems():
                        if key in aggregate_dict:
                            aggregate_dict[key] += value
                        else:
                            aggregate_dict[key] = value

            if aggregate_dict is None or len(aggregate_dict) == 0:
                # No update operations for readonly workload
                continue

            if workload_type not in bucket_dicts:
                bucket_dicts[workload_type] = aggregate_dict
            else:
                for key in aggregate_dict.keys():
                    dest_dict = bucket_dicts[workload_type]
                    if key in dest_dict:
                        dest_dict[key] += aggregate_dict[key]
                    else:
                        dest_dict[key] = aggregate_dict[key]

for workload_type in bucket_dicts.keys():
    aggregate_dict = bucket_dicts[workload_type]
    total_num_operations = reduce(lambda x, y: x + y, aggregate_dict.values())
    for key in aggregate_dict.keys():
        aggregate_dict[key] = float(aggregate_dict[key]) / total_num_operations
    rows = []
    for i, key in enumerate(sorted(aggregate_dict.keys())):
        if i == 0:
            rows.append({'0latency': key + 1, '1cumulative': aggregate_dict[key]})
        else:
            cum = rows[i - 1]['1cumulative'] + aggregate_dict[key]
            rows.append({'0latency': key + 1, '1cumulative': cum})
    df = pd.DataFrame(rows)
    df.to_csv('%s/histogram-update-%s-%s.csv' % (output_dir_path, workload_type, dir_name), header=False,
              index=False)

paths = filter(lambda x: re.search('.*histogram\-update\-.*\.csv', x) is not None,
               ['%s/%s' % (output_dir_path, x) for x in os.listdir(output_dir_path)])
uniform_paths = filter(lambda x: x.find('uniform') != -1, paths)
latest_paths = filter(lambda x: x.find('latest') != -1, paths)
zipfian_paths = filter(lambda x: x.find('zipfian') != -1, paths)
noreconfig_paths = filter(lambda x: x.find('noreconfig') != -1, paths)

output_path = '%s/update-latency-cdf.png' % output_dir_path

os.system('./plot-update-latency-cdf.sh --output_path=%s '
          '--uniform=%s --latest=%s --zipfian=%s --noreconfig=%s' %
          (output_path, uniform_paths[0], latest_paths[0], zipfian_paths[0], noreconfig_paths[0]))


# Read timeseries
for dir_name in os.listdir(raw_data_root):
    if re.search('[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9][0-9][0-9]$', dir_name) is not None:
        cur_dir_path = '%s/%s' % (raw_data_root, dir_name)

        print 'Read timeseries', dir_name

        meta = ConfigParser.SafeConfigParser()
        meta.read('%s/meta.ini' % cur_dir_path)
        config_dict = meta._sections['config']
        config_dict['result_dir_name'] = dir_name

        if meta.get('config', 'should_reconfigure') == 'True' and not meta.has_option('result', 'MorphusStartAt'):
            print 'Morphus failed for ', dir_name
            continue

        output_fs = filter(lambda x: x.find('output-') != -1 and x.find('stderr') == -1, os.listdir(cur_dir_path))
        if meta.get('config', 'measurement_type') == 'timeseries':
            paths = {}
            workload_type = ''
            for fname in output_fs:
                f = open('%s/%s' % (cur_dir_path, fname))
                df = ycsb_parser.parse_timeseries(f.read(), 'read')
                if meta.get('config', 'should_reconfigure') == 'False':
                    morphus_start_at = df['0time'].iloc[0]
                else:
                    morphus_start_at = int(meta.get('result', 'MorphusStartAt'))
                df['0time'] -= morphus_start_at
                df['1latency'] = df['1latency'].apply(lambda x: -50000 if x < 0 else x)
                workload_type = meta.get('config', 'workload_type')
                if meta.get('config', 'should_reconfigure') == 'False':
                    if json.loads(meta.get('config', 'workload_proportions').replace("'", '"'))['read'] == 10:
                        workload_type = 'noreconfig-readonly'
                    else:
                        workload_type = 'noreconfig-update'
                elif json.loads(meta.get('config', 'workload_proportions').replace("'", '"'))['read'] == 10:
                    workload_type = 'readonly'
                key = 'original' if fname.find('original') != -1 else 'altered'
                path = '%s/timeseries-read-%s-%s-%s.csv' % (output_dir_path, workload_type, dir_name, key)
                df.to_csv(path, header=False, index=False)
                paths[key] = path

            if meta.get('config', 'should_reconfigure') == 'False':
                insert_morphus_task = 0
                atomicswitch_morphus_task = 0
                catchup_morphus_task = 0
                morphus_start_at = 0
            else:
                morphus_start_at = int(meta.get('result', 'MorphusStartAt'))

                if not (meta.has_option('result', 'InsertMorphusTask') and meta.has_option('result', 'AtomicSwitchMorphusTask') and meta.has_option('result', 'CatchupMorphusTask')):
                    print 'Incomplete Morphus script for %s' % dir_name
                    continue

                insert_morphus_task = int(meta.get('result', 'InsertMorphusTask')) - morphus_start_at
                atomicswitch_morphus_task = int(meta.get('result', 'AtomicSwitchMorphusTask')) - morphus_start_at
                catchup_morphus_task = int(meta.get('result', 'CatchupMorphusTask')) - morphus_start_at
                morphus_start_at = 0

            output_path = '%s/timeseries-read-%s-%s.png' % (output_dir_path, workload_type, dir_name)

            plot_file = 'plot-timeseries.sh' if meta.get('config', 'should_reconfigure') == 'True' else 'plot-timeseries-noreconfig.sh'

            os.system('./%s --type=%s --output_path=%s '
                      '--original=%s --altered=%s --morphus_start_at=%d '
                      '--insert=%d --atomicswitch=%d --catchup=%d' %
                      (plot_file, 'Read', output_path, paths['original'], paths['altered'], morphus_start_at,
                       insert_morphus_task, atomicswitch_morphus_task, catchup_morphus_task))


# Update timeseries
for dir_name in os.listdir(raw_data_root):
    if re.search('[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9][0-9][0-9]$', dir_name) is not None:
        cur_dir_path = '%s/%s' % (raw_data_root, dir_name)

        print 'Update timeseries:', dir_name

        meta = ConfigParser.SafeConfigParser()
        meta.read('%s/meta.ini' % cur_dir_path)
        config_dict = meta._sections['config']
        config_dict['result_dir_name'] = dir_name

        if meta.get('config', 'should_reconfigure') == 'True' and not meta.has_option('result', 'MorphusStartAt'):
            print 'Morphus failed for ', dir_name
            continue

        output_fs = filter(lambda x: x.find('output-') != -1 and x.find('stderr') == -1, os.listdir(cur_dir_path))
        if meta.get('config', 'measurement_type') == 'timeseries':
            workload_type = meta.get('config', 'workload_type')
            if json.loads(meta.get('config', 'workload_proportions').replace("'", '"'))['read'] == 10:
                continue
            elif meta.get('config', 'should_reconfigure') == 'False':
                workload_type = 'noreconfig'
            paths = {}
            for fname in output_fs:
                f = open('%s/%s' % (cur_dir_path, fname))
                df = ycsb_parser.parse_timeseries(f.read(), 'update')
                if meta.get('config', 'should_reconfigure') == 'False':
                    morphus_start_at = df['0time'].iloc[0]
                else:
                    morphus_start_at = int(meta.get('result', 'MorphusStartAt'))
                df['0time'] -= morphus_start_at
                df['1latency'] = df['1latency'].apply(lambda x: -50000 if x < 0 else x)
                key = 'original' if fname.find('original') != -1 else 'altered'
                path = '%s/timeseries-update-%s-%s-%s.csv' % (output_dir_path, workload_type, dir_name, key)
                df.to_csv(path, header=False, index=False)
                paths[key] = path

            if meta.get('config', 'should_reconfigure') == 'False':
                insert_morphus_task = 0
                atomicswitch_morphus_task = 0
                catchup_morphus_task = 0
                morphus_start_at = 0
            else:
                morphus_start_at = int(meta.get('result', 'MorphusStartAt'))

                if not (meta.has_option('result', 'InsertMorphusTask') and meta.has_option('result', 'AtomicSwitchMorphusTask') and meta.has_option('result', 'CatchupMorphusTask')):
                    print 'Incomplete Morphus script for %s' % dir_name
                    continue

                insert_morphus_task = int(meta.get('result', 'InsertMorphusTask')) - morphus_start_at
                atomicswitch_morphus_task = int(meta.get('result', 'AtomicSwitchMorphusTask')) - morphus_start_at
                catchup_morphus_task = int(meta.get('result', 'CatchupMorphusTask')) - morphus_start_at
                morphus_start_at = 0

            output_path = '%s/timeseries-update-%s-%s.png' % (output_dir_path, workload_type, dir_name)

            plot_file = 'plot-timeseries.sh' if meta.get('config', 'should_reconfigure') == 'True' else 'plot-timeseries-noreconfig.sh'

            os.system('./%s --type=%s --output_path=%s '
                      '--original=%s --altered=%s --morphus_start_at=%d '
                      '--insert=%d --atomicswitch=%d --catchup=%d' %
                      (plot_file, 'Update', output_path, paths['original'], paths['altered'], morphus_start_at,
                       insert_morphus_task, atomicswitch_morphus_task, catchup_morphus_task))




# Reconfiguration time
rows = []
for dir_name in os.listdir(raw_data_root):
    if re.search('[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9][0-9][0-9]$', dir_name) is not None:
        cur_dir_path = '%s/%s' % (raw_data_root, dir_name)

        print dir_name

        meta = ConfigParser.SafeConfigParser()
        meta.read('%s/meta.ini' % cur_dir_path)
        config_dict = meta._sections['config']
        config_dict['result_dir_name'] = dir_name

        if meta.get('config', 'should_reconfigure') == 'False':
            continue
        else:
            workload_type = meta.get('config', 'workload_type')
            if json.loads(meta.get('config', 'workload_proportions').replace("'", '"'))['read'] == 10:
                workload_type = 'readonly'
            elif meta.get('config', 'should_reconfigure') == 'False':
                workload_type = 'noreconfig'
                continue
            if not meta.has_option('result', 'CatchupMorphusTask'):
                continue

            duration = int(meta.get('result', 'CatchupMorphusTask')) - int(meta.get('result', 'MorphusStartAt'))
            insert_morphus_task_duration = int(meta.get('result', 'InsertMorphusTask')) - int(
                meta.get('result', 'MorphusStartAt'))
            atomic_switch_morphus_task_duration = int(meta.get('result', 'AtomicSwitchMorphusTask')) - int(
                meta.get('result', 'InsertMorphusTask'))
            catchup_morphus_task_duration = int(meta.get('result', 'CatchupMorphusTask')) - int(
                meta.get('result', 'AtomicSwitchMorphusTask'))
            rows.append({'workload_type': workload_type, 'morphus_duration': duration,
                         'insert_morphus_task_duration': insert_morphus_task_duration,
                         'atomic_switch_morphus_task_duration': atomic_switch_morphus_task_duration,
                         'catchup_morphus_task_duration': catchup_morphus_task_duration})

            df = pd.DataFrame(rows)
            groupby = df.groupby('workload_type')
            df = groupby.agg([np.mean, np.std])
            df.to_csv('%s/reconfiguration-time.csv' % output_dir_path)
