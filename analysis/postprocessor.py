import os
import time
import pandas as pd
import ConfigParser
from twilio.rest import TwilioRestClient
import numpy as np

config = ConfigParser.SafeConfigParser()
config.read('config.ini')

private_config = ConfigParser.SafeConfigParser()
private_config.read('private.ini')
tc = TwilioRestClient(private_config.get('twilio', 'account_sid'), private_config.get('twilio', 'auth_token'))

workload_types = ['uniform', 'zipfian', 'latest', 'readonly']
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


def get_dataframe_from_csvs(base_dir):
    dir_ = os.path.join(base_dir, 'processed')
    list_ = []
    for fn in os.listdir(dir_):
        full_path = os.path.join(dir_, fn)
        if os.path.isfile(full_path) and full_path.endswith('.csv'):
            current_df = pd.read_csv(full_path, index_col=None, header=0)
            list_.append(current_df)
    df = pd.concat(list_, ignore_index=True)
    return df


def move_csvs(dir_, output_file_name):
    # Create a directory to store merged input files
    os.mkdir(os.path.join(dir_, output_file_name))
    for fn in os.listdir(dir_):
        full_path = os.path.join(dir_, fn)
        if os.path.isfile(full_path) and full_path.endswith('.csv'):
            # Move input file into merged archive directory
            os.rename(full_path, os.path.join(dir_, output_file_name, fn))


def plot_num_nodes_vs_reconfig_time(df, base_dir, timestamp):
    df = df[(df['num_records'] == default_num_records) &
            (df['is_reconfigured'] == (not default_no_reconfiguration)) &
            (df['num_threads'] == 0) &
            (df['replication_factor'] == default_replication_factor) &
            (df['workload_type'] == str(default_workload_type)) &
            (df['num_nodes'] >= 2)]
    gb = df.groupby('num_nodes')
    aggregated_df =  gb['compactmorphoustask', 'insertmorphoustask', 'atomicswitchmorphoustask', 'catchupmorphoustask'].agg([np.mean, np.std])
    output_csv_file_name = 'plot-num-nodes-vs-reconfig-time'
    output_csv_file_path = os.path.join(base_dir, 'processed', timestamp, output_csv_file_name)
    aggregated_df.to_csv(output_csv_file_path + '.csv', header=False)
    os.system('./plot-num-nodes-vs-reconfig-time.sh %s' % output_csv_file_path)

def plot_replication_factor_vs_reconfig_time(df, base_dir, timestamp):
    df = df[(df['num_records'] == default_num_records) &
            (df['is_reconfigured'] == (not default_no_reconfiguration)) &
            (df['num_threads'] == default_num_threads) &  # TODO Maybe change this to 0
            (df['workload_type'] == str(default_workload_type)) &
            (df['num_nodes'] == default_active_cluster_size)]
    gb =  df.groupby('replication_factor')
    df['catchupmorphoustask-compactmorphoustask'] = df['catchupmorphoustask'] - df['compactmorphoustask']
    aggregated_df =  gb['compactmorphoustask', 'insertmorphoustask', 'atomicswitchmorphoustask', 'catchupmorphoustask', 'catchupmorphoustask-compactmorphoustask'].agg([np.mean, np.std])
    output_csv_file_name = 'plot-replication-factor-vs-reconfig-time'
    output_csv_file_path = os.path.join(base_dir, 'processed', timestamp, output_csv_file_name)
    aggregated_df.to_csv(output_csv_file_path + '.csv', header=False)
    os.system('./plot-replication-factor-vs-reconfig-time.sh %s' % output_csv_file_path)

def plot_num_records_vs_reconfig_time(df, base_dir, timestamp):
    df = df[(df['replication_factor'] == default_replication_factor) &
            (df['is_reconfigured'] == (not default_no_reconfiguration)) &
            (df['num_threads'] == default_num_threads) &  # TODO Maybe change this to 0
            (df['workload_type'] == str(default_workload_type)) &
            (df['num_nodes'] == default_active_cluster_size)]
    # df = df[df['num_records'] < 10000000]
    df['catchupmorphoustask-compactmorphoustask'] = df['catchupmorphoustask'] - df['compactmorphoustask']
    gb =  df.groupby('num_records')
    aggregated_df =  gb['compactmorphoustask', 'insertmorphoustask', 'atomicswitchmorphoustask', 'catchupmorphoustask', 'catchupmorphoustask-compactmorphoustask'].agg([np.mean, np.std])
    output_csv_file_name = 'plot-num-records-vs-reconfig-time'
    output_csv_file_path = os.path.join(base_dir, 'processed', timestamp, output_csv_file_name)
    aggregated_df.to_csv(output_csv_file_path + '.csv', header=False)
    os.system('./plot-num-records-vs-reconfig-time.sh %s' % output_csv_file_path)

def plot_num_threads_vs_reconfig_time(df, base_dir, timestamp):
    df = df[(df['num_records'] == default_num_records) &
            (df['replication_factor'] == default_replication_factor) &
            (df['is_reconfigured'] == (not default_no_reconfiguration)) &
            (df['workload_type'] == str(default_workload_type)) &
            (df['num_nodes'] == default_active_cluster_size)]
    df['catchupmorphoustask-compactmorphoustask'] = df['catchupmorphoustask'] - df['compactmorphoustask']
    gb =  df.groupby('num_threads')
    aggregated_df =  gb['compactmorphoustask', 'insertmorphoustask', 'atomicswitchmorphoustask', 'catchupmorphoustask', 'catchupmorphoustask-compactmorphoustask'].agg([np.mean, np.std])
    output_csv_file_name = 'plot-num-threads-vs-reconfig-time'
    output_csv_file_path = os.path.join(base_dir, 'processed', timestamp, output_csv_file_name)
    aggregated_df.to_csv(output_csv_file_path + '.csv', header=False)
    os.system('./plot-num-threads-vs-reconfig-time.sh %s' % output_csv_file_path)


def calculate_workload_phase_times(df, base_dir, output_dir_name):
    df = df[(df['num_records'] == default_num_records) &
            (df['replication_factor'] == default_replication_factor) &
            (df['is_reconfigured'] == (not default_no_reconfiguration)) &
            (df['num_threads'] == default_num_threads) &
            (df['num_nodes'] == default_active_cluster_size)]
    # Because newer results on workloads experiment have floating number latencies
    df = df[df['base_directory_name'] >= '01-22-0505']
    rows = []
    for workload_type in workload_types:
        row = df[df['workload_type'] == workload_type][['compactmorphoustask', 'insertmorphoustask', 'atomicswitchmorphoustask', 'catchupmorphoustask']].mean(axis=0).to_dict()
        row['workload_type'] = workload_type
        rows.append(row)
    result_df = pd.DataFrame(rows)
    result_df.to_csv(os.path.join(base_dir, 'processed', output_dir_name, 'workload_vs_phase_times.csv'))


def calculate_workload_availability(df, base_dir, output_dir_name, num_records):
    df = df[(df['num_records'] == num_records) &
            (df['replication_factor'] == default_replication_factor) &
            (df['is_reconfigured'] == (not default_no_reconfiguration)) &
            (df['num_threads'] == default_num_threads) &
            (df['num_nodes'] == default_active_cluster_size)]
    # Because newer results on workloads experiment have floating number latencies
    # df = df[df['base_directory_name'] >= '01-22-0505']
    rows = []
    for i, row in df.iterrows():
        result_dict = row.to_dict()
        if result_dict['workload_type'] != 'readonly':
            # Insert availability
            file_path = os.path.join(base_dir, 'raw', result_dict['base_directory_name'], 'pool-18-thread-1-y_id-insert-data.txt')
            latency_df = pd.read_csv(file_path, delim_whitespace=True, header=None, names=['timestamp', 'latency'])
            latency_df = latency_df[latency_df['latency'] == -1.0].sort(['timestamp'])
            unavailable_start_at = latency_df.irow(0)['timestamp']
            unavailable_end_at = latency_df.irow(len(latency_df.index) - 1)['timestamp']
            result_dict['insert_availability'] = (1.0 - (unavailable_end_at - unavailable_start_at) / result_dict['catchupmorphoustask']) * 100.0
            if not (7000 > unavailable_end_at - unavailable_start_at > 0):
                print 'unacceptable insert unavailability %d ~ %d \n %s' % (unavailable_start_at, unavailable_end_at, str(result_dict))
                continue

        # Read availability
        file_path = os.path.join(base_dir, 'raw', result_dict['base_directory_name'], 'pool-18-thread-1-y_id-read-data.txt')
        latency_df = pd.read_csv(file_path, delim_whitespace=True, header=None, names=['timestamp', 'latency'])
        latency_df = latency_df[latency_df['latency'] == -1.0].sort(['timestamp'])
        unavailable_start_at = latency_df.irow(0)['timestamp']
        file_path = os.path.join(base_dir, 'raw', result_dict['base_directory_name'], 'pool-18-thread-2-field0-read-data.txt')
        latency_df = pd.read_csv(file_path, delim_whitespace=True, header=None, names=['timestamp', 'latency'])
        latency_df = latency_df[latency_df['latency'] == -1.0].sort(['timestamp'])
        unavailable_end_at = latency_df.irow(len(latency_df.index) - 1)['timestamp']
        if not (5000 > unavailable_end_at - unavailable_start_at > 0):
            print 'unacceptable read unavailability %d ~ %d \n %s' % (unavailable_start_at, unavailable_end_at, str(result_dict))
            continue
        result_dict['read_availability'] = (1.0 - (unavailable_end_at - unavailable_start_at) / result_dict['catchupmorphoustask']) * 100.0
        rows.append(result_dict)
    availability_df = pd.DataFrame(rows)[['workload_type', 'insert_availability', 'read_availability']]
    availability_aggregated = availability_df.groupby('workload_type').mean()
    availability_aggregated.to_csv(os.path.join(base_dir, 'processed', output_dir_name, 'workload-availability-%dgb.csv' % (num_records / 1000000)))


def plot_time_series(df, base_dir, output_dir_name):
    df = df[(df['num_records'] == default_num_records) &
            (df['replication_factor'] == default_replication_factor) &
            (df['is_reconfigured'] == (not default_no_reconfiguration)) &
            (df['num_threads'] == default_num_threads) &
            (df['num_nodes'] == default_active_cluster_size)]
    # Because newer results on workloads experiment have floating number latencies
    df = df[df['base_directory_name'] >= '01-22-0505']
    plot_dir = os.path.join(base_dir, 'processed', output_dir_name, 'timeseries-plots')
    os.mkdir(plot_dir)

    for i, row in df.iterrows():
        result_dict = row.to_dict()
        if result_dict['workload_type'] != 'readonly':
            file_name = 'insert-latency-%s-%s.png' % (result_dict['workload_type'], result_dict['base_directory_name'])
            os.system('./plot-timeseries.sh %s %s %s %s %d %d %d %d %d' %
                      (os.path.join(plot_dir, file_name),
                       'Write',
                       os.path.join(base_dir, 'raw', result_dict['base_directory_name'], 'pool-18-thread-1-y_id-insert-data.txt'),
                       os.path.join(base_dir, 'raw', result_dict['base_directory_name'], 'pool-18-thread-2-field0-insert-data.txt'),
                       result_dict['morphousstartat'],
                       result_dict['compactmorphoustask'],
                       result_dict['insertmorphoustask'],
                       result_dict['atomicswitchmorphoustask'],
                       result_dict['catchupmorphoustask']))

        file_name = 'read-latency-%s-%s.png' % (result_dict['workload_type'], result_dict['base_directory_name'])
        os.system('./plot-timeseries.sh %s %s %s %s %d %d %d %d %d' %
                  (os.path.join(plot_dir, file_name),
                   'Read',
                   os.path.join(base_dir, 'raw', result_dict['base_directory_name'], 'pool-18-thread-1-y_id-read-data.txt'),
                   os.path.join(base_dir, 'raw', result_dict['base_directory_name'], 'pool-18-thread-2-field0-read-data.txt'),
                   result_dict['morphousstartat'],
                   result_dict['compactmorphoustask'],
                   result_dict['insertmorphoustask'],
                   result_dict['atomicswitchmorphoustask'],
                   result_dict['catchupmorphoustask']))


# Plot read or write cdf graph across different workloads
# No reconfig, readonly, uniform, latest, zipfian
def plot_cdf(df, base_dir, output_dir_name, read_or_insert):
    df = df[(df['num_records'] == default_num_records) &
            (df['replication_factor'] == default_replication_factor) &
            (df['num_threads'] == default_num_threads) &
            (df['num_nodes'] == default_active_cluster_size)]
    # Because newer results on workloads experiment have floating number latencies
    df = df[df['base_directory_name'] >= '01-22-0505']
    if read_or_insert == 'insert':
        df = df[df['workload_type'] != 'readonly']
    latencies_dict = {}
    for i, row in df.iterrows():
        result_dict = row.to_dict()
        assert result_dict['num_threads'] == default_num_threads
        key = (result_dict['workload_type'], result_dict['is_reconfigured'])
        new_list = get_latency_as_list(base_dir, result_dict, read_or_insert)
        existing_list = latencies_dict.get(key, [])
        existing_list.extend(new_list)
        latencies_dict[key] = existing_list

    for key, latency_list in latencies_dict.iteritems():
        cdf_df = get_cdf_as_dataframe(latency_list)
        csv_file_path = os.path.join(base_dir, 'processed', output_dir_name, '%s-%s-%i.txt' % (read_or_insert, key[0], key[1]))
        cdf_df.to_csv(csv_file_path, header=False, sep='\t')

    os.system('./plot-%s-latency-cdf.sh %s %s %s %s %s %s %s %s %s %s %s %s' %
              (read_or_insert,
               'Read' if (read_or_insert == 'read') else 'Write',
               os.path.join(base_dir, 'processed', output_dir_name, '%s-uniform-0.txt' % read_or_insert),
               os.path.join(base_dir, 'processed', output_dir_name, '%s-readonly-1.txt' % read_or_insert),
               os.path.join(base_dir, 'processed', output_dir_name, '%s-uniform-1.txt' % read_or_insert),
               os.path.join(base_dir, 'processed', output_dir_name, '%s-latest-1.txt' % read_or_insert),
               os.path.join(base_dir, 'processed', output_dir_name, '%s-zipfian-1.txt' % read_or_insert),
               os.path.join(base_dir, 'processed', output_dir_name, '%s-uniform-0.txt' % read_or_insert),
               os.path.join(base_dir, 'processed', output_dir_name, '%s-readonly-1.txt' % read_or_insert),
               os.path.join(base_dir, 'processed', output_dir_name, '%s-uniform-1.txt' % read_or_insert),
               os.path.join(base_dir, 'processed', output_dir_name, '%s-latest-1.txt' % read_or_insert),
               os.path.join(base_dir, 'processed', output_dir_name, '%s-zipfian-1.txt' % read_or_insert),
               os.path.join(base_dir, 'processed', output_dir_name, 'plot-%s-latency-cdf.png' % read_or_insert)))


def get_latency_as_list(base_dir, result_dict, read_or_write):
    result_dir = os.path.join(base_dir, 'raw', result_dict['base_directory_name'])
    file_name = 'pool-18-thread-1-y_id-%s-data.txt' % read_or_write
    df = pd.read_csv(os.path.join(result_dir, file_name), delim_whitespace=True, header=None, names=['timestamp', 'latency'])
    if result_dict['is_reconfigured']:
        df = df[(df['timestamp'] > result_dict['morphousstartat']) &
                (df['timestamp'] < (result_dict['morphousstartat'] + result_dict['catchupmorphoustask']))]
    else:
        first_timestamp = df['timestamp'].irow(0)
        df = df[(df['timestamp'] > (first_timestamp + 25000))]
    df = df[(df['latency'] > -1)]
    return df['latency'].tolist()


def get_cdf_as_dataframe(latency_list):
    latency_list.sort()
    cum_dict = {}
    assert len(latency_list) > 0
    prev_elem = latency_list[0]
    for i, elem in enumerate(latency_list):
        if elem > prev_elem:
            cum_dict[prev_elem] = float(i + 1) / len(latency_list)
            prev_elem = elem
        elif i == len(latency_list) - 1:
            cum_dict[elem] = 1.0
    df = pd.DataFrame.from_dict(cum_dict, 'index')
    df = df.sort([0])
    return df


def main():
    base_dir = '/Users/Daniel/Dropbox/Illinois/research/experiment'
    output_dir_name = time.strftime('%m-%d-%H%M%S')
    os.mkdir(os.path.join(base_dir, 'processed', output_dir_name))
    df = get_dataframe_from_csvs(base_dir)
    df.to_csv(os.path.join(base_dir, 'processed', output_dir_name, 'merged.csv'))
    # plot_num_nodes_vs_reconfig_time(df, base_dir, output_dir_name)
    # plot_replication_factor_vs_reconfig_time(df, base_dir, output_dir_name)
    # plot_num_records_vs_reconfig_time(df, base_dir, output_dir_name)
    # plot_num_threads_vs_reconfig_time(df, base_dir, output_dir_name)
    plot_cdf(df, base_dir, output_dir_name, 'read')
    plot_cdf(df, base_dir, output_dir_name, 'insert')
    # plot_time_series(df, base_dir, output_dir_name)
    # calculate_workload_phase_times(df, base_dir, output_dir_name)
    # calculate_workload_availability(df, base_dir, output_dir_name, 1000000)
    # calculate_workload_availability(df, base_dir, output_dir_name, 10000000)

if __name__ == "__main__":
    main()
