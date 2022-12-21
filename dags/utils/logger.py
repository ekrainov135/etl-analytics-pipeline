import logging
import sys


LOG_TEXT_FORMAT = '%(levelname)s [%(asctime)s] %(name)s: %(message)s'


def get_logger(path, level=logging.INFO):
    #logging.basicConfig(level=level, filename=path, format=LOG_TEXT_FORMAT)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.INFO)

    logger = logging.getLogger('DAG')

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(level)
    stream_handler.setFormatter(logging.Formatter('%(name)s: %(message)s', '%m.%d %X'))

    file_handler = logging.FileHandler(path)
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter(LOG_TEXT_FORMAT, '%m.%d %X'))

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logger
