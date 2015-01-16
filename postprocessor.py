import os
import time
import pandas as pd
import ConfigParser

config = ConfigParser.SafeConfigParser()
config.read('config.ini')


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
    df = df[(df['num_records'] == 1)]
    print df.shape


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
