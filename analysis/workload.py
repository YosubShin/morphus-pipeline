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

            total_num_operations = float(reduce(lambda x, y: x + y, aggregate_dict.values()))
            for key in aggregate_dict.keys():
                aggregate_dict[key] = float(aggregate_dict[key]) / total_num_operations
            rows = []
            for i, key in enumerate(sorted(aggregate_dict.keys())):
                if i == 0:
                    rows.append({'0latency': key + 1, '1cumulative': aggregate_dict[key]})
                else:
                    cum = rows[i - 1]['1cumulative'] + aggregate_dict[key]
                    rows.append({'0latency': key + 1, '1cumulative': cum})
            # df = pd.DataFrame(aggregate_dict.items())
            df = pd.DataFrame(rows)
            workload_type = meta.get('config', 'workload_type')
            if json.loads(meta.get('config', 'workload_proportions').replace("'", '"'))['read'] == 10:
                workload_type = 'readonly'
            df.to_csv('%s/histogram-read-%s-%s.csv' % (output_dir_path, workload_type, dir_name), header=False,
                      index=False)

paths = filter(lambda x: re.search('.*histogram\-read\-.*\.csv', x) is not None, ['%s/%s' % (output_dir_path, x) for x in os.listdir(output_dir_path)])
readonly_paths = filter(lambda x: x.find('readonly') != -1, paths)
uniform_paths = filter(lambda x: x.find('uniform') != -1, paths)
latest_paths = filter(lambda x: x.find('latest') != -1, paths)
zipfian_paths = filter(lambda x: x.find('zipfian') != -1, paths)

output_path = '%s/read-latency-cdf.png' % output_dir_path

print './plot-read-latency-cdf.sh --output_path=%s ' \
      '--readonly=%s --uniform=%s --latest=%s --zipfian=%s' % \
      (output_path, readonly_paths[0], uniform_paths[0], latest_paths[0], zipfian_paths[0])

os.system('./plot-read-latency-cdf.sh --output_path=%s '
          '--readonly=%s --uniform=%s --latest=%s --zipfian=%s' %
          (output_path, readonly_paths[0], uniform_paths[0], latest_paths[0], zipfian_paths[0]))


# Plot insert cdf
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
            aggregate_dict = None
            for fname in output_fs:
                f = open('%s/%s' % (cur_dir_path, fname))
                bucket_dict = ycsb_parser.parse_latency_bucket(f.read(), 'insert')
                if aggregate_dict is None:
                    aggregate_dict = bucket_dict
                else:
                    for key, value in bucket_dict.iteritems():
                        if key in aggregate_dict:
                            aggregate_dict[key] += value
                        else:
                            aggregate_dict[key] = value

            if aggregate_dict is None or len(aggregate_dict) == 0:
                # No insert operations for readonly workload
                continue
            total_num_operations = float(reduce(lambda x, y: x + y, aggregate_dict.values()))
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
            workload_type = meta.get('config', 'workload_type')
            if json.loads(meta.get('config', 'workload_proportions').replace("'", '"'))['read'] == 10:
                # No insert operations for readonly workload
                continue
            df.to_csv('%s/histogram-insert-%s-%s.csv' % (output_dir_path, workload_type, dir_name), header=False,
                      index=False)

paths = filter(lambda x: re.search('.*histogram\-insert\-.*\.csv', x) is not None, ['%s/%s' % (output_dir_path, x) for x in os.listdir(output_dir_path)])
uniform_paths = filter(lambda x: x.find('uniform') != -1, paths)
latest_paths = filter(lambda x: x.find('latest') != -1, paths)
zipfian_paths = filter(lambda x: x.find('zipfian') != -1, paths)

output_path = '%s/insert-latency-cdf.png' % output_dir_path

os.system('./plot-insert-latency-cdf.sh --output_path=%s '
          '--uniform=%s --latest=%s --zipfian=%s' %
          (output_path, uniform_paths[0], latest_paths[0], zipfian_paths[0]))

# Read timeseries

for dir_name in os.listdir(raw_data_root):
    if re.search('[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9][0-9][0-9]$', dir_name) is not None:
        cur_dir_path = '%s/%s' % (raw_data_root, dir_name)

        print dir_name

        meta = ConfigParser.SafeConfigParser()
        meta.read('%s/meta.ini' % cur_dir_path)
        config_dict = meta._sections['config']
        config_dict['result_dir_name'] = dir_name

        output_fs = filter(lambda x: x.find('output-') != -1 and x.find('stderr') == -1, os.listdir(cur_dir_path))
        if meta.get('config', 'measurement_type') == 'timeseries':
            paths = {}
            workload_type = ''
            for fname in output_fs:
                f = open('%s/%s' % (cur_dir_path, fname))
                df = ycsb_parser.parse_timeseries(f.read(), 'read')
                workload_type = meta.get('config', 'workload_type')
                if json.loads(meta.get('config', 'workload_proportions').replace("'", '"'))['read'] == 10:
                    workload_type = 'readonly'
                key = 'original' if fname.find('original') != -1 else 'altered'
                path = '%s/timeseries-read-%s-%s-%s.csv' % (output_dir_path, workload_type, dir_name, key)
                df.to_csv(path, header=False, index=False)
                paths[key] = path

            if not (meta.has_option('result', 'compactmorphoustask') and meta.has_option('result', 'insertmorphoustask') and meta.has_option('result', 'atomicswitchmorphoustask') and meta.has_option('result', 'catchupmorphoustask')):
                print 'Incomplete Morphus script for %s' % dir_name
                continue

            morphus_start_at = int(meta.get('result', 'morphus_start_at_in_python')) - int(meta.get('result', 'ycsb_start_at_in_python'))
            compact_morphus_task = int(meta.get('result', 'compactmorphoustask')) + morphus_start_at
            insert_morphus_task = int(meta.get('result', 'insertmorphoustask')) + morphus_start_at
            atomicswitch_morphus_task = int(meta.get('result', 'atomicswitchmorphoustask')) + morphus_start_at
            catchup_morphus_task = int(meta.get('result', 'catchupmorphoustask')) + morphus_start_at

            output_path = '%s/timeseries-read-%s-%s.png' % (output_dir_path, workload_type, dir_name)

            os.system('./plot-timeseries.sh --type=%s --output_path=%s '
                      '--original=%s --altered=%s --morphus_start_at=%d '
                      '--compact=%d --insert=%d --atomicswitch=%d --catchup=%d' %
                      ('Read', output_path, paths['original'], paths['altered'], morphus_start_at, compact_morphus_task,
                       insert_morphus_task, atomicswitch_morphus_task, catchup_morphus_task))
