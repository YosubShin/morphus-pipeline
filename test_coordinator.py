from unittest import TestCase
import pandas as pd
import ConfigParser
from twilio.rest import TwilioRestClient
import coordinator
import os

__author__ = 'Daniel'


class Test_coordinator(TestCase):
    def test_pandas_dict_append(self):
        rows = []
        for i in range(3):
            row_dict = {'base_directory_name': 'name%d' % i,
                   'CompactMorphousTask': str(i),
                   'InsertMorphousTask': str(i),
                   'AtomicSwitchMorphousTask': str(i),
                   'CatchupMorphousTask': str(i)}
            rows.append(row_dict)

        df = pd.DataFrame(rows)
        df.to_csv('/tmp/test_pandas_dict_append.csv')
        print df

        read_df = df.from_csv('/tmp/test_pandas_dict_append.csv')
        print read_df

    def test_private_config_parser(self):
        private_config = ConfigParser.SafeConfigParser()
        private_config.read('config.ini')
        print private_config.get('twilio', 'account_sid')
        print private_config.get('twilio', 'auth_token')

    def test_twilio_sms(self):
        private_config = ConfigParser.SafeConfigParser()
        private_config.read('config.ini')
        print private_config.get('twilio', 'account_sid')
        print private_config.get('twilio', 'auth_token')
        tc = TwilioRestClient(private_config.get('twilio', 'account_sid'), private_config.get('twilio', 'auth_token'))
        tc.messages.create(from_=private_config.get('personal', 'twilio_number'),
                           to=private_config.get('personal', 'phone_number'),
                           body='Sending test SMS.\n Test with more than 160 characters '\
                                'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\n'\
                                'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\n'\
                                'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\n'\
                                'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\n'\
                                'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\n'\
                                'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\n')

    def test_append_to_csv(self):
        csv_file_name = '/tmp/test_csv.csv'
        if os.path.isfile(csv_file_name):
            os.remove(csv_file_name)
        for i in range(10):
            row_dict = {'base_directory_name': 'name%d' % i,
                   'CompactMorphousTask': str(i),
                   'InsertMorphousTask': str(i),
                   'AtomicSwitchMorphousTask': str(i),
                   'CatchupMorphousTask': str(i)}
            coordinator.append_row_to_csv(csv_file_name, row_dict)
            read_df = pd.read_csv(csv_file_name)
            assert read_df.shape[0] == i + 1

        print read_df