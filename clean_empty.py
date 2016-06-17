#!python
import schedule
import time
import datetime
import requests
from dateutil.parser import parse as time_parser
import argparse
import os

host = os.getenv('RABBITMQ_MANAGEMENT_HOST', "http://guest:guest@localhost:15672")
timeout_seconds = float(os.getenv('QUEUE_TIMEOUT_MINUTES', 10)) * 60
clean_minutes =  float(os.getenv('CLEAN_MINUTES', 10))


def clean_empty_queues():
    response = requests.request("GET", host + '/api/queues')
    count = 0
    for queue in response.json():
        count += 1
        consumers = queue['consumers']
        messages = queue['messages']
        state = queue['state']
        messages_unacknowledged = queue['messages_unacknowledged']
        idle_since =  time_parser(queue['idle_since']) if 'idle_since' in queue else datetime.datetime.now()
        name = queue['name']
        vhost = '%2f'  if (queue['vhost'] == '/') else queue['vhost']

        if (consumers == 0 and
            messages == 0 and
            messages_unacknowledged == 0 and
            (idle_since.now() - idle_since).total_seconds() > timeout_seconds):
            print 'queue', name, 'has been inactive for', int((idle_since.now() - idle_since).total_seconds()/60), 'minutes'

            delete_url = host +  '/api/queues/' +vhost + '/' + name + '?if-empty=true:if-unused=true'
            response = requests.request("DELETE", delete_url)
            print 'deleting queue '+ name + ' ' + delete_url, response

    print time.strftime("%a, %d %b %Y %H:%M:%S"), count, 'queues processed'


schedule.every(clean_minutes).minutes.do(clean_empty_queues)


while True:
    clean_empty_queues()
    schedule.run_pending()
    time.sleep(1)
