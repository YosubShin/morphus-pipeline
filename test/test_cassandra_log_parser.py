from unittest import TestCase
import cassandra_log_parser as parse


class TestCassandraLogParser(TestCase):
    def test_parse(self):
        f = open('system.log')
        log = f.read()
        parser = parse.CassandraLogParser(log)
        parser.parse()