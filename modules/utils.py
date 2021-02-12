import logging
import sys
import json

def _logger(name):
    """ Return a new logger """
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)20s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S')
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger

def get_parameter(file):
    """Get script parameters""" 

    with open(file, 'r') as f:
        rec_dict = json.load(f)

    return rec_dict

def write_parameter(param_dict, file):

    with open(file, 'w') as fp:
        json.dump(param_dict, fp, indent = 4, separators=(',', ': '))
