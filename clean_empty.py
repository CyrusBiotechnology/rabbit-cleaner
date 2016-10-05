#!python
from dateutil.parser import parse as time_parser
import requests

import argparse
import datetime
import logging
from logging import config
import os
import re
import schedule
import time

logger = logging.getLogger(__name__)


def clean_empty_queues(base_url, regex_pattern, queue_idle_minutes, force_delete=False, vhost=None):
    logger.info("running")
    try:
        response = requests.request("GET", base_url + '/api/queues')
        pattern = re.compile(regex_pattern)
        total = 0
        matched = 0
        deleted = 0
        for i, queue in enumerate(response.json()):
            total += 1
            consumers = queue['consumers']
            messages = queue['messages']
            messages_unacknowledged = queue['messages_unacknowledged']
            idle_since = time_parser(queue['idle_since']) if 'idle_since' in queue else datetime.datetime.now()
            name = queue['name']
            vhost = '%2f' if (queue['vhost'] == '/') else queue['vhost']
    
            if (pattern.match(name) and
                    consumers == 0 and
                    messages == 0 and
                    messages_unacknowledged == 0 and
                    (idle_since.now() - idle_since).total_seconds() > queue_idle_minutes * 60):
                matched += 1
                idle_minutes = int((idle_since.now() - idle_since).total_seconds() / 60)
                logger.debug('queue "%s" has been inactive for %d minutes' % (queue, idle_minutes))
    
                delete_url = base_url + '/api/queues/' + vhost + '/' + name
                delete_url += '' if force_delete else '?if-empty=true:if-unused=true'
                logger.info('deleting queue "%s"' % delete_url)
                response = requests.request("DELETE", delete_url)
                if 200 <= response.status_code < 300:
                    deleted += 1
                else:
                    logger.error('problem deleting queue "%s": %s' % (name, response.text))

        logger.debug('%d queues processed, %d matched parameters, %d deleted' % (total, matched, deleted))
    
    except Exception as e:
        logger.error("cleanup failed: %s" % e)


if __name__ == "__main__":
    config.dictConfig({
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
    })

    argparser = argparse.ArgumentParser(description='Clean empty RabbitMQ queues.')
    argparser.add_argument('--url', type=str, default="http://guest:guest@localhost:15672",
                           help="RabbitMQ management interface base URL (default: %(default)s)")
    argparser.add_argument('--queue-idle-minutes', type=int, default=10,
                           help=("Number of minutes a queue has to be inactive before is marked for deletion "
                                 "(default: %(default)s)"))
    argparser.add_argument('--clean-minutes', type=int, default=10,
                           help="Number of minutes between runs (default: %(default)s)")
    argparser.add_argument('--pattern', type=str, default=".*",
                           help="Regex pattern use to select the queues under management (default: %(default)s)")
    argparser.add_argument('--force', type=bool, default=False, help="Delete queue even if it is not empty or unused")
    args = argparser.parse_args()

    schedule.every(args.clean_minutes).minutes.do(clean_empty_queues)

    clean_empty_queues(args.url, args.pattern, args.queue_idle_minutes, force_delete=args.force)
    while True:
        schedule.run_pending()
        time.sleep(1)
