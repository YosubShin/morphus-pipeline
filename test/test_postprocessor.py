from unittest import TestCase
import os
import shutil

import pandas as pd

from analysis import postprocessor as pp


__author__ = 'Daniel'


class Test_postprocessor(TestCase):
    def test_get_cdf_as_dataframe(self):
        result_dict = {
            'is_reconfigured': True,
            'base_directory_name': '01-22-1005',
            'catchupmorphoustask': 35575,
            'morphousstartat': 1658781
        }
        latency_list = pp.get_latency_as_list('./test', result_dict, 'read')
        print pp.get_cdf_as_dataframe(latency_list)

    def test_get_latency_as_list(self):
        result_dict = {
            'is_reconfigured': True,
            'base_directory_name': '01-22-1005',
            'catchupmorphoustask': 35575,
            'morphousstartat': 1658781
        }
        print pp.get_latency_as_list('./test', result_dict, 'read')


    def test_plot_read_cdf(self):
        df = pd.read_csv('./test/processed/01-15-1532.csv', index_col=None)
        test_result_path = './test/processed/test_plot_read_cdf'
        if os.path.exists(test_result_path):
            shutil.rmtree(test_result_path)
        os.mkdir(test_result_path)

        pp.plot_cdf(df, os.path.abspath('test'), 'test_plot_read_cdf', 'read')

    def test_plot_write_cdf(self):
        df = pd.read_csv('./test/processed/01-15-1532.csv', index_col=None)
        test_result_path = './test/processed/test_plot_insert_cdf'
        if os.path.exists(test_result_path):
            shutil.rmtree(test_result_path)
        os.mkdir(test_result_path)

        pp.plot_cdf(df, os.path.abspath('test'), 'test_plot_insert_cdf', 'insert')