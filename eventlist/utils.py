import yaml
import os
import logging

log = logging.getLogger(__name__)


def load_config(filename=None, environ_name='EVENTLIST_CONFIG'):
    '''
    load a yaml config file

    If filename is not given, the function looks first if there
    is an EVENTLIST_CONFIG environment variable then if there is an `eventlist.yaml` in
    the current directory
    '''
    if filename is None:
        if environ_name in os.environ:
            filename = os.environ[environ_name]
        elif os.path.isfile('eventlist.yaml'):
            filename = 'eventlist.yaml'
        else:
            raise ValueError('No config file found')

    log.debug('Loading config file {}'.format(filename))

    with open(filename, 'r') as f:
        config = yaml.safe_load(f)

    return config, os.path.abspath(filename)
