#!python
from dateutil.parser import parse as time_parser
import requests

import schedule
import time
import datetime
import os
import re
import logging
from logging import config

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'detailed',
        },
    },
    'formatters': {
        'detailed': {
            'format': '%(asctime)s\t%(name)s:%(lineno)d\t%(levelname)-8s\t%(message)s',
        },
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': os.getenv('LOGGING_LEVEL', 'DEBUG'),
        },
        'requests.packages.urllib3.connectionpool': {
            'handlers': ['console'],
            'level': os.getenv('LOGGING_LEVEL', 'WARNING'),
        },
    },
}

logger = logging.getLogger(__name__)

config.dictConfig(LOGGING)

host = os.getenv('RABBITMQ_MANAGEMENT_HOST', "http://guest:guest@localhost:15672")
timeout_seconds = float(os.getenv('QUEUE_TIMEOUT_MINUTES', 10)) * 60
clean_minutes = float(os.getenv('CLEAN_MINUTES', 10))
regex_pattern = os.getenv('PATTERN', '.*')
force_delete = os.getenv('FORCE_DELETE', False)


def clean_empty_queues():
    logger.info("running")
    try:
        response = requests.request("GET", host + '/api/queues')
        count = 0
        pattern = re.compile(regex_pattern)
        for queue in response.json():
            count += 1
            consumers = queue['consumers']
            messages = queue['messages']
            state = queue['state']
            messages_unacknowledged = queue['messages_unacknowledged']
            idle_since = time_parser(queue['idle_since']) if 'idle_since' in queue else datetime.datetime.now()
            name = queue['name']
            vhost = '%2f' if (queue['vhost'] == '/') else queue['vhost']
    
            if (pattern.match(name) and
                    consumers == 0 and
                    messages == 0 and
                    messages_unacknowledged == 0 and
                    (idle_since.now() - idle_since).total_seconds() > timeout_seconds):
                idle_minutes = int((idle_since.now() - idle_since).total_seconds() / 60)
                logger.debug('queue "%s" has been inactive for %d minutes' % (queue, idle_minutes))
    
                delete_url = host + '/api/queues/' + vhost + '/' + name
                delete_url += '' if force_delete else '?if-empty=true:if-unused=true'
                logger.info('deleting queue "%s"' % delete_url)
                response = requests.request("DELETE", delete_url)
                if not 200 <= response.status_code < 300:
                    logger.error('problem deleting queue "%s": %s' % (name, response.text))

        logger.debug('%d queues processed' % count)
    
    except Exception as e:
        logger.error("cleanup failed: %s" % e)


schedule.every(clean_minutes).minutes.do(clean_empty_queues)

clean_empty_queues()
while True:
    schedule.run_pending()
    time.sleep(1)
