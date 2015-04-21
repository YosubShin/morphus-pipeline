import re
import pandas as pd


def parse_histogram_execution_output(buf):
    all_lines = buf.split('\n')

    def is_avg_latency(x):
        return x.find('AverageLatency') is not -1

    latency_lines = filter(is_avg_latency, all_lines)

    result_dict = dict()
    for line in latency_lines:
        latency = float(line.split(' ')[2])
        if line.find('UPDATE') is not -1:
            result_dict['update_average_latency'] = latency
        elif line.find('READ') is not -1:
            result_dict['read_average_latency'] = latency

    for line in all_lines:
        if line.find('OVERALL') is not -1 and line.find('Throughput') is not -1:
            result_dict['overall_throughput'] = float(line.split(' ')[2])

    def is_num_operations(l):
        return l.find('Operations') != -1

    num_operations_lines = filter(is_num_operations, all_lines)
    for line in num_operations_lines:
        num_operations = int(float(line.split(' ')[2]))
        if line.find('UPDATE') is not -1:
            result_dict['update_num_operations'] = num_operations
        elif line.find('READ') is not -1:
            result_dict['read_num_operations'] = num_operations
        elif line.find('OVERALL') is not -1:
            result_dict['overall_num_operations'] = num_operations

    for line in all_lines:
        if line.find('RunTime') != -1:
            result_dict['runtime'] = float(line.split(' ')[2])

    if not result_dict.has_key('update_average_latency') or \
            not result_dict.has_key('read_average_latency') or \
            not result_dict.has_key('overall_throughput') or \
            not result_dict.has_key('update_num_operations') or \
            not result_dict.has_key('read_num_operations') or \
            not result_dict.has_key('overall_num_operations') or \
            not result_dict.has_key('runtime'):
        raise Exception('Could not parse YCSB result...')

    return result_dict


# 'read' or 'update'?
def parse_latency_bucket(buf, type_):
    assert type_ == 'read' or type_ == 'update' or type_ == 'insert'
    all_lines = buf.split('\n')

    to_match = '[%s]' % type_.upper()

    def latency_lines_filter(x):
        splitted = [y.strip() for y in x.split(',')]
        return x.find(to_match) != -1 and re.match('^[0-9]+$', splitted[1])

    latency_lines = filter(latency_lines_filter, all_lines)

    result_dict = dict()
    for l in latency_lines:
        split = [y.strip() for y in l.split(',')]
        assert len(split) == 3
        millisec = int(split[1])
        count = int(split[2])
        result_dict[millisec] = count

    return result_dict


def parse_timeseries(buf, type_):
    assert type_ == 'read' or type_ == 'update' or type_ == 'insert'
    all_lines = buf.split('\n')

    to_match = '[%s]' % type_.upper()

    def latency_lines_filter(x):
        splitted = [y.strip() for y in x.split(',')]
        return x.find(to_match) != -1 and re.match('^[0-9]+$', splitted[1])

    latency_lines = filter(latency_lines_filter, all_lines)

    rows = []
    for l in latency_lines:
        split = [y.strip() for y in l.split(',')]
        assert len(split) == 3
        time = int(split[1])
        latency = int(float(split[2]))
        rows.append({'0time': time, '1latency': latency})

    return pd.DataFrame(rows)