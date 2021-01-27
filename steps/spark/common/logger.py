import logging
import os
import sys

def setup_logging(log_level, log_path):
    the_logger = logging.getLogger()
    
    if log_path is None:
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = logging.FileHandler(log_path)
    
    json_format = "{ 'timestamp': '%(asctime)s', 'log_level': '%(levelname)s', 'message': '%(message)s' }"
    handler.setFormatter(logging.Formatter(json_format))
    the_logger.addHandler(handler)
    new_level = logging.getLevelName(log_level.upper())
    the_logger.setLevel(new_level)

    return the_logger
    