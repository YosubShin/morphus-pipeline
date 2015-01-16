import os
import time
import pandas as pd


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
    df.drop('Unnamed: 0', 1)
    print df
    df.to_csv(merged_file_path)

merge_csvs('/Users/Daniel/Dropbox/Illinois/research/experiment/processed/test', time.strftime('%m-%d-%H%M-merged'))