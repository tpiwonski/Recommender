import logging
import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
log_handler = logging.StreamHandler()
# file_handler = logging.FileHandler("log_{}.log".format(datetime.datetime.now().strftime('%y-%m-%d_%H-%M-%S')))
log_formatter = logging.Formatter('[%(asctime)s][%(name)s][%(levelname)s] %(message)s')
log_handler.setFormatter(log_formatter)
# file_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)
# logger.addHandler(file_handler)

def batch(it, size):
    assert size > 0
    result = []
    for e in it:
        result.append(e)
        if len(result) == size:
            yield result
            result = []

    if result:
        yield result


class Progress(object):

    def __init__(self, total):
        self.total = total
        self.count = 0
        self.start = datetime.datetime.now()

    def advance(self):
        self.count += 1
    
    def get_progress(self):
        return (self.count / self.total) * 100.0;
    
    def get_estimated_time(self):
        now = datetime.datetime.now()
        dt = now - self.start
        left = self.total - self.count
        return now + (left * dt / self.count) 
