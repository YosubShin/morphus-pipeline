import os
import time
import pandas as pd
import ConfigParser
import numpy as np
import re


data_base_path = '../../../experiment/workload'

def plot_workload():
    df = parse_results()

    output_dir_name = strftime('%m-%d-%H%M')
    try:
        os.mkdir('%s/processed/%s' % (data_base_path, output_dir_name))
        df.to_csv('%s/processed/%s/data.csv' % (data_base_path, output_dir_name))
    except:
        pass


def parse_results():
    rows = []
    data_root = '%s/raw' % data_base_path

    for dir_name in os.listdir(data_root):
        if re.search('[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9][0-9][0-9]$', dir_name) is not None:
            cur_dir_path = '%s/%s' % (data_root, dir_name)

            result = None
            print dir_name
            for fname in os.listdir(cur_dir_path):
                if fname.find('output-') != -1 and fname.find('stderr') == -1:
                    f = open('%s/%s' % (cur_dir_path, fname))
                    try:
                        cur_result = ycsb_parser.parse_execution_output(f.read())
                    except Exception, e:
                        print str(e)
                        continue
                    if result is None:
                        result = cur_result
                    else:
                        print fname
                        new_result = dict()
                        new_result['update_num_operations'] = result['update_num_operations'] + cur_result['update_num_operations']
                        new_result['read_num_operations'] = result['read_num_operations'] + cur_result['read_num_operations']
                        new_result['overall_num_operations'] = result['overall_num_operations'] + cur_result['overall_num_operations']

                        new_result['update_average_latency'] = result['update_average_latency'] * result['update_num_operations'] / new_result['update_num_operations'] \
                                                               + cur_result['update_average_latency'] * cur_result['update_num_operations'] / new_result['update_num_operations']
                        new_result['read_average_latency'] = result['update_average_latency'] * result['update_num_operations'] / new_result['update_num_operations'] \
                                                               + cur_result['update_average_latency'] * cur_result['update_num_operations'] / new_result['update_num_operations']

                        new_result['overall_throughput'] = result['overall_throughput'] + cur_result['overall_throughput']
                        result = new_result

            meta = ConfigParser.SafeConfigParser()
            meta.read('%s/meta.ini' % cur_dir_path)
            config_dict = meta._sections['config']
            config_dict['result_dir_name'] = dir_name
            result.update(config_dict)

            rows.append(result)
    return pd.DataFrame(rows)