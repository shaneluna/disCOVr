import logging
import time

LOG_FORMAT = '%(asctime)s | %(levelname)s | %(name)s | %(funcName)s() | %(message)s'
LOG_FILENAME = "logs/main.log"
LOG_LEVEL = logging.DEBUG

logging.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL, filename=LOG_FILENAME, filemode='a')
logging.Formatter.converter = time.gmtime

class LogHandler():
    def __init__(self, *args, **kwargs):
        self.log = logging.getLogger(self.__class__.__name__)