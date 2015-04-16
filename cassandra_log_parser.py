class CassandraLogParser:
    def __init__(self, log):
        self.log = log

    def parse(self):
        raw_lines = filter(lambda x: x.find('MorphousTask is over in') != -1, self.log.splitlines())
        result = {}
        for line in raw_lines:
            task_name = None
            completion_time = None
            for token in line.split():
                if token.find('Task') != -1:
                    task_name = token
                elif token.find('ms') != -1:
                    completion_time = int(token[0:len(token) - 2])
            assert task_name is not None and completion_time is not None
            result[task_name] = completion_time

        print result