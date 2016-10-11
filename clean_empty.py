#!/usr/bin/env python
import traceback

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


# def get_queue_list_in_pages(base_url, page_size=100, offset=0):
#     page = offset
#     payload = {'page': page, 'page_size': page_size}
#     response = requests.request("GET", base_url + '/api/queues', params=payload)
#     json = response.json()
#     json['']
#     while response.status_code == 200:
#         response = requests.request("GET", base_url + '/api/queues', params=payload)
#         yield response.json()


def clean_empty_queues(base_url, regex_pattern, queue_idle_minutes, force_delete=False):
    start = time.time()
    pattern = re.compile(regex_pattern)
    payload = {
        'name': regex_pattern,
        'use_regex': 'true'
    }
    try:
        response = requests.request("GET", base_url + '/api/queues', params=payload)
        total = 0
        matched = 0
        deleted = 0
        for i, queue in enumerate(response.json()):
            total += 1
            name = queue['name']

            consumers = queue.get('consumers')
            if consumers and consumers != 0:
                logger.debug('%s has %s consumers, skipping' % (name, consumers))
                continue

            messages = queue.get('messages')
            if messages and messages != 0:
                logger.debug('%s has %s messages, skipping' % (name, messages))
                continue

            messages_unacknowledged = queue.get('messages_unacknowledged')
            if messages_unacknowledged and messages_unacknowledged != 0:
                logger.debug('%s has %s unacked messages, skipping' % (name, messages_unacknowledged))
                continue

            idle_since = time_parser(queue['idle_since']) if 'idle_since' in queue else datetime.datetime.now()
            if idle_since and (idle_since.now() - idle_since).total_seconds() > queue_idle_minutes * 60:
                logger.debug('%s is not idle, skipping' % name)
                continue

            vhost = '%2f' if (queue['vhost'] == '/') else queue['vhost']

            if pattern.match(name):
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

    except Exception as error:
        logger.exception(error)
    else:
        logger.info("execution took %s" % (time.time() - start))


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
                'level': os.getenv('LOGGING_LEVEL', 'INFO'),
            },
            'requests.packages.urllib3.connectionpool': {
                'handlers': ['console'],
                'level': os.getenv('LOGGING_LEVEL', 'WARNING'),
            },
        },
    })

    argparser = argparse.ArgumentParser(description='Clean empty RabbitMQ queues.')
    argparser.add_argument('pattern', type=str, default=".*",
                           help="Regex pattern use to select the queues under management (default: %(default)s)")
    argparser.add_argument('--url', type=str, default="http://guest:guest@localhost:15672",
                           help="RabbitMQ management interface base URL (default: %(default)s)")
    argparser.add_argument('--queue-idle-minutes', type=int, default=10,
                           help=("Number of minutes a queue has to be inactive before is marked for deletion "
                                 "(default: %(default)s)"))
    argparser.add_argument('--clean-minutes', type=int, default=10,
                           help="Number of minutes between runs (default: %(default)s)")
    argparser.add_argument('--force', default=False, action='store_true',
                           help="Delete queue even if it is not empty or unused")
    args = argparser.parse_args()

    logger.info('starting up...')

    def job():
        clean_empty_queues(args.url, args.pattern, args.queue_idle_minutes, force_delete=args.force)

    if args.clean_minutes is not None and args.clean_minutes > 0:
        logger.info('running every %s minutes' % args.clean_minutes)
        schedule.every(args.clean_minutes).minutes.do(job)

    job()
    while True:
        schedule.run_pending()
        time.sleep(1)
