import os
import time
import pandas as pd
import ConfigParser
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


def merge_csvs(dir_, output_file_name):
    list_ = []
    merged_file_path = os.path.join(dir_, output_file_name + '.csv')
    # Create a directory to store merged input files
    os.mkdir(os.path.join(dir_, output_file_name))
    print os.listdir(dir_)
    for fn in os.listdir(dir_):
        full_path = os.path.join(dir_, fn)
        if os.path.isfile(full_path) and full_path.endswith('.csv'):
            print full_path
            current_df = pd.read_csv(full_path, index_col=None, header=0)
            list_.append(current_df)
            # Move input file into merged archive directory
            os.rename(full_path, os.path.join(dir_, output_file_name, fn))
    df = pd.concat(list_, ignore_index=True)
    df = df.drop('Unnamed: 0', 1)
    df.to_csv(merged_file_path)


def plot_num_nodes_vs_reconfig_time(df, output_dir):
    print df.desc()
    df = df[(df['num_records'] == default_num_records) &
            (df['is_reconfigured'] == str(not default_no_reconfiguration)) &
            (df['num_threads'] == default_num_threads) &
            # (df['replication_factor'] == 1) &
            (df['workload_type'] == str(default_workload_type))]
    print df


def main():
    input_dir = '/Users/Daniel/Dropbox/Illinois/research/experiment/processed/test'
    merged_file_name = time.strftime('%m-%d-%H%M-merged')
    merge_csvs(input_dir, merged_file_name)
    df = pd.read_csv(os.path.join(input_dir, (merged_file_name + '.csv')))
    print df.shape
    plot_num_nodes_vs_reconfig_time(df, merged_file_name)
    print df.shape

if __name__ == "__main__":
    main()
