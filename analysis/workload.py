import os
import time
import pandas as pd
import ConfigParser
import numpy as np
import re
import ycsb_parser
import time
import json

data_base_path = os.path.abspath('../../../experiment/workload')

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


# # Plot Read latency CDF
# bucket_dicts = {}
# for dir_name in os.listdir(raw_data_root):
#     if re.search('[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9][0-9][0-9]$', dir_name) is not None:
#         cur_dir_path = '%s/%s' % (raw_data_root, dir_name)
#
#         print 'Read CDF', dir_name
#
#         meta = ConfigParser.SafeConfigParser()
#         meta.read('%s/meta.ini' % cur_dir_path)
#         config_dict = meta._sections['config']
#         config_dict['result_dir_name'] = dir_name
#
#         output_fs = filter(lambda x: x.find('output-') != -1 and x.find('stderr') == -1, os.listdir(cur_dir_path))
#         if meta.get('config', 'measurement_type') == 'histogram':
#             workload_type = meta.get('config', 'workload_type')
#             if json.loads(meta.get('config', 'workload_proportions').replace("'", '"'))['read'] == 10:
#                 if meta.get('config', 'should_reconfigure') == 'True':
#                     workload_type = 'readonly'
#                 else:
#                     workload_type = 'noreconfig'
#             elif meta.get('config', 'should_reconfigure') == 'False':
#                 continue
#
#             aggregate_dict = None
#             for fname in output_fs:
#                 f = open('%s/%s' % (cur_dir_path, fname))
#                 bucket_dict = ycsb_parser.parse_latency_bucket(f.read(), 'read')
#                 if aggregate_dict is None:
#                     aggregate_dict = bucket_dict
#                 else:
#                     for key, value in bucket_dict.iteritems():
#                         if key in aggregate_dict:
#                             aggregate_dict[key] += value
#                         else:
#                             aggregate_dict[key] = value
#
#             if workload_type not in bucket_dicts:
#                 bucket_dicts[workload_type] = aggregate_dict
#             else:
#                 for key in aggregate_dict.keys():
#                     dest_dict = bucket_dicts[workload_type]
#                     if key in dest_dict:
#                         dest_dict[key] += aggregate_dict[key]
#                     else:
#                         dest_dict[key] = aggregate_dict[key]
#
# for workload_type in bucket_dicts.keys():
#     aggregate_dict = bucket_dicts[workload_type]
#     total_num_operations = reduce(lambda x, y: x + y, aggregate_dict.values())
#     for key in aggregate_dict.keys():
#         aggregate_dict[key] = float(aggregate_dict[key]) / total_num_operations
#     rows = []
#     for i, key in enumerate(sorted(aggregate_dict.keys())):
#         if i == 0:
#             rows.append({'0latency': key + 1, '1cumulative': aggregate_dict[key]})
#         else:
#             cum = rows[i - 1]['1cumulative'] + aggregate_dict[key]
#             rows.append({'0latency': key + 1, '1cumulative': cum})
#     df = pd.DataFrame(rows)
#     df.to_csv('%s/histogram-read-%s.csv' % (output_dir_path, workload_type), header=False,
#               index=False)
#
# paths = filter(lambda x: re.search('.*histogram\-read\-.*\.csv', x) is not None,
#                ['%s/%s' % (output_dir_path, x) for x in os.listdir(output_dir_path)])
# readonly_paths = filter(lambda x: x.find('readonly') != -1, paths)
# uniform_paths = filter(lambda x: x.find('uniform') != -1, paths)
# latest_paths = filter(lambda x: x.find('latest') != -1, paths)
# zipfian_paths = filter(lambda x: x.find('zipfian') != -1, paths)
# noreconfig_paths = filter(lambda x: x.find('noreconfig') != -1, paths)
#
# output_path = '%s/read-latency-cdf.png' % output_dir_path
#
# os.system('./plot-read-latency-cdf.sh --output_path=%s '
#           '--readonly=%s --uniform=%s --latest=%s --zipfian=%s --noreconfig=%s' %
#           (output_path, readonly_paths[0], uniform_paths[0], latest_paths[0], zipfian_paths[0], noreconfig_paths[0]))
#
# bucket_dicts = {}
# # Plot update cdf
# for dir_name in os.listdir(raw_data_root):
#     if re.search('[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9][0-9][0-9]$', dir_name) is not None:
#         cur_dir_path = '%s/%s' % (raw_data_root, dir_name)
#
#         print dir_name
#
#         meta = ConfigParser.SafeConfigParser()
#         meta.read('%s/meta.ini' % cur_dir_path)
#         config_dict = meta._sections['config']
#         config_dict['result_dir_name'] = dir_name
#
#         output_fs = filter(lambda x: x.find('output-') != -1 and x.find('stderr') == -1, os.listdir(cur_dir_path))
#         if meta.get('config', 'measurement_type') == 'histogram':
#             workload_type = meta.get('config', 'workload_type')
#             if json.loads(meta.get('config', 'workload_proportions').replace("'", '"'))['read'] == 10:
#                 # No update operations for readonly workload
#                 continue
#             elif meta.get('config', 'should_reconfigure') == 'False':
#                 if workload_type == 'latest':
#                     continue
#                 else:
#                     workload_type = 'noreconfig'
#
#             aggregate_dict = None
#             for fname in output_fs:
#                 f = open('%s/%s' % (cur_dir_path, fname))
#                 bucket_dict = ycsb_parser.parse_latency_bucket(f.read(), 'update')
#                 if aggregate_dict is None:
#                     aggregate_dict = bucket_dict
#                 else:
#                     for key, value in bucket_dict.iteritems():
#                         if key in aggregate_dict:
#                             aggregate_dict[key] += value
#                         else:
#                             aggregate_dict[key] = value
#
#             if aggregate_dict is None or len(aggregate_dict) == 0:
#                 # No update operations for readonly workload
#                 continue
#
#             if workload_type not in bucket_dicts:
#                 bucket_dicts[workload_type] = aggregate_dict
#             else:
#                 for key in aggregate_dict.keys():
#                     dest_dict = bucket_dicts[workload_type]
#                     if key in dest_dict:
#                         dest_dict[key] += aggregate_dict[key]
#                     else:
#                         dest_dict[key] = aggregate_dict[key]
#
# for workload_type in bucket_dicts.keys():
#     aggregate_dict = bucket_dicts[workload_type]
#     total_num_operations = reduce(lambda x, y: x + y, aggregate_dict.values())
#     for key in aggregate_dict.keys():
#         aggregate_dict[key] = float(aggregate_dict[key]) / total_num_operations
#     rows = []
#     for i, key in enumerate(sorted(aggregate_dict.keys())):
#         if i == 0:
#             rows.append({'0latency': key + 1, '1cumulative': aggregate_dict[key]})
#         else:
#             cum = rows[i - 1]['1cumulative'] + aggregate_dict[key]
#             rows.append({'0latency': key + 1, '1cumulative': cum})
#     df = pd.DataFrame(rows)
#     df.to_csv('%s/histogram-update-%s-%s.csv' % (output_dir_path, workload_type, dir_name), header=False,
#               index=False)
#
# paths = filter(lambda x: re.search('.*histogram\-update\-.*\.csv', x) is not None,
#                ['%s/%s' % (output_dir_path, x) for x in os.listdir(output_dir_path)])
# uniform_paths = filter(lambda x: x.find('uniform') != -1, paths)
# latest_paths = filter(lambda x: x.find('latest') != -1, paths)
# zipfian_paths = filter(lambda x: x.find('zipfian') != -1, paths)
# noreconfig_paths = filter(lambda x: x.find('noreconfig') != -1, paths)
#
# output_path = '%s/update-latency-cdf.png' % output_dir_path
#
# os.system('./plot-update-latency-cdf.sh --output_path=%s '
#           '--uniform=%s --latest=%s --zipfian=%s --noreconfig=%s' %
#           (output_path, uniform_paths[0], latest_paths[0], zipfian_paths[0], noreconfig_paths[0]))


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
            insert_morphus_task_duration = (int(meta.get('result', 'InsertMorphusTask')) - int(
                meta.get('result', 'MorphusStartAt'))) / 1000
            atomic_switch_morphus_task_duration = (int(meta.get('result', 'AtomicSwitchMorphusTask')) - int(
                meta.get('result', 'InsertMorphusTask'))) / 1000
            catchup_morphus_task_duration = (int(meta.get('result', 'CatchupMorphusTask')) - int(
                meta.get('result', 'AtomicSwitchMorphusTask'))) / 1000
            rows.append({'workload_type': workload_type,
                         # 'morphus_duration': duration,
                         'Migrate Phase': insert_morphus_task_duration,
                         'Commit Phase': atomic_switch_morphus_task_duration,
                         'Recovery Phase': catchup_morphus_task_duration})

df = pd.DataFrame(rows)
df = df[['workload_type', 'Migrate Phase', 'Commit Phase', 'Recovery Phase']]

groupby = df.groupby('workload_type')
df = groupby.agg([np.mean])

df = df.transpose()
df = df[['readonly', 'uniform', 'zipfian', 'latest']]
csv_file_path = '%s/reconfiguration-time.csv' % output_dir_path
df.to_csv(csv_file_path)

output_path = '%s/reconfiguration-time-stacked-histogram.png' % output_dir_path
os.system('./plot-reconfiguration-time-stacked-histogram.sh --input_path=%s --output_path=%s ' %
          (csv_file_path, output_path))


# Read & Update Timeseries
# Availability
# Readonly, Uniform, Zipfian, Latest, Latest(No roconfig)
bucket_dicts = {'read': {}, 'update': {}}
availability_dict = {}
for dir_name in os.listdir(raw_data_root):
    if re.search('[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9][0-9][0-9]$', dir_name) is not None:
        cur_dir_path = '%s/%s' % (raw_data_root, dir_name)

        print 'Availability', dir_name

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
        rws = ['read']
        if not is_read_only:
            rws.append('update')
        for rw in rws:
            paths[rw] = {}
        abs_morphus_start_at = 0
        insert_morphus_task = 0
        atomicswitch_morphus_task = 0
        catchup_morphus_task = 0
        morphus_start_at = 0
        workload_type = ''
        for fname in output_fs:
            f = open('%s/%s' % (cur_dir_path, fname))

            if should_reconfigure:
                abs_morphus_start_at = int(meta.get('result', 'MorphusStartAt'))
                if not meta.has_option('result', 'CatchupMorphusTask'):
                    print 'Incomplete Morphus script for %s' % dir_name
                    continue
                insert_morphus_task = int(meta.get('result', 'InsertMorphusTask')) - abs_morphus_start_at
                atomicswitch_morphus_task = int(meta.get('result', 'AtomicSwitchMorphusTask')) - abs_morphus_start_at
                catchup_morphus_task = int(meta.get('result', 'CatchupMorphusTask')) - abs_morphus_start_at

            workload_type = meta.get('config', 'workload_type')
            if not should_reconfigure:
                if is_read_only:
                    workload_type = 'noreconfig-readonly'
                elif workload_type == 'latest':
                    workload_type = 'noreconfig-latest'
                else:
                    workload_type = 'noreconfig-update'
            elif is_read_only:
                workload_type = 'readonly'

            f_buf = f.read()
            for rw in rws:
                df = ycsb_parser.parse_timeseries(f_buf, rw)
                # print cur_dir_path, fname, rw, df.describe()
                if not should_reconfigure:
                    abs_morphus_start_at = df['0time'].iloc[0] + 10000
                df['0time'] -= abs_morphus_start_at
                df['1latency'] = df['1latency'].apply(lambda x: -1000000 if x < 0 else x)

                # Read & Write Timeseries
                key = 'original' if fname.find('original') != -1 else 'altered'
                path = '%s/timeseries-read-%s-%s-%s.csv' % (output_dir_path, workload_type, dir_name, key)
                df.to_csv(path, header=False, index=False)
                paths[rw][key] = path

                # Availability
                if not should_reconfigure:
                    if key != 'original':
                        continue
                    else:
                        filtered_df = df[(df['0time'] > morphus_start_at)]
                elif key == 'original':
                    filtered_df = df[(df['0time'] > morphus_start_at) & (df['0time'] < atomicswitch_morphus_task)]
                else:
                    filtered_df = df[(df['0time'] > atomicswitch_morphus_task) & (df['0time'] < catchup_morphus_task)]

                succeeded_filtered_df = filtered_df[(filtered_df['1latency'] != -1000000)]
                key = '%s-%s' % (rw, workload_type)
                if key not in availability_dict:
                    availability_dict[key] = (len(succeeded_filtered_df), len(filtered_df))
                else:
                    availability_tuple = availability_dict[key]
                    availability_dict[key] = (availability_tuple[0] + len(succeeded_filtered_df),
                                              availability_tuple[1] + len(filtered_df))
                # print(key, len(succeeded_filtered_df), len(filtered_df))

                # CDF
                if not(should_reconfigure
                       or workload_type == 'noreconfig-readonly'
                       or workload_type == 'noreconfig-update'):
                    continue

                aggregate_dict = {}

                def add_to_aggregate_dict(latency):
                    bucket_value = latency - latency % 100
                    if bucket_value in aggregate_dict:
                        aggregate_dict[bucket_value] += 1
                    else:
                        aggregate_dict[bucket_value] = 1

                succeeded_filtered_df['1latency'].apply(add_to_aggregate_dict)

                if workload_type not in bucket_dicts[rw]:
                    bucket_dicts[rw][workload_type] = aggregate_dict
                else:
                    for key in aggregate_dict.keys():
                        dest_dict = bucket_dicts[rw][workload_type]
                        if key in dest_dict:
                            dest_dict[key] += aggregate_dict[key]
                        else:
                            dest_dict[key] = aggregate_dict[key]
            f.close()

        for rw in rws:
            output_path = '%s/timeseries-%s-%s-%s.png' % (output_dir_path, rw, workload_type, dir_name)

            plot_file = 'plot-timeseries.sh' if meta.get('config', 'should_reconfigure') == 'True' else 'plot-timeseries-noreconfig.sh'

            os.system('./%s --type=%s --output_path=%s '
                      '--original=%s --altered=%s --morphus_start_at=%d '
                      '--insert=%d --atomicswitch=%d --catchup=%d' %
                      (plot_file, rw, output_path, paths[rw]['original'], paths[rw]['altered'], morphus_start_at,
                       insert_morphus_task, atomicswitch_morphus_task, catchup_morphus_task))

# Save availability fractions csv file
rows = []
for w, t in availability_dict.iteritems():
    rows.append({'workload_type': w, 'success_count': t[0], 'operations_count': t[1], 'availability': 100.0 * t[0] / t[1]})
    # rows.append({'workload_type': w, 'success_count': t[0], 'operations_count': t[1]})
df = pd.DataFrame(rows)
path = '%s/availability.csv' % output_dir_path
df.to_csv(path, index=False)

# Save csv files for CDF for each workload
for rw in bucket_dicts.keys():
    for workload_type in bucket_dicts[rw].keys():
        aggregate_dict = bucket_dicts[rw][workload_type]
        total_num_operations = reduce(lambda x, y: x + y, aggregate_dict.values())
        for key in aggregate_dict.keys():
            aggregate_dict[key] = float(aggregate_dict[key]) / total_num_operations
        rows = []
        for i, key in enumerate(sorted(aggregate_dict.keys())):
            if i == 0:
                rows.append({'0latency': key, '1cumulative': aggregate_dict[key]})
            else:
                cum = rows[i - 1]['1cumulative'] + aggregate_dict[key]
                rows.append({'0latency': key, '1cumulative': cum})
        df = pd.DataFrame(rows)
        df.to_csv('%s/histogram-%s-%s.csv' % (output_dir_path, rw, workload_type), header=False,
                  index=False)

# Plot Read CDF
paths = filter(lambda x: re.search('.*histogram\-read\-.*\.csv', x) is not None,
               ['%s/%s' % (output_dir_path, x) for x in os.listdir(output_dir_path)])
readonly_paths = filter(lambda x: x.find('read-readonly') != -1, paths)
uniform_paths = filter(lambda x: x.find('uniform') != -1, paths)
latest_paths = filter(lambda x: x.find('latest') != -1, paths)
zipfian_paths = filter(lambda x: x.find('zipfian') != -1, paths)
noreconfig_paths = filter(lambda x: x.find('noreconfig-readonly') != -1, paths)

output_path = '%s/read-latency-cdf.png' % output_dir_path

os.system('./plot-read-latency-cdf.sh --output_path=%s '
          '--readonly=%s --uniform=%s --latest=%s --zipfian=%s --noreconfig=%s' %
          (output_path, readonly_paths[0], uniform_paths[0], latest_paths[0], zipfian_paths[0], noreconfig_paths[0]))

# Plot Update CDF
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
