class CassandraLogParser:
    def __init__(self, log):
        self.log = log

    def parse(self):
        raw_lines = filter(lambda x: x.find('MorphusTimestamp') != -1, self.log.splitlines())
        result = {}
        for line in raw_lines:
            task_name = None
            timestamp = None
            tokens = line.split()
            for i, token in enumerate(tokens):
                if token.find('MorphusTimestamp') != -1:
                    task_name = tokens[i + 1]
                    timestamp = int(tokens[i + 2])
            assert task_name is not None and timestamp is not None
            result[task_name] = timestamp

        return result