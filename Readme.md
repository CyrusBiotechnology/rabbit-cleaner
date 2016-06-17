Utility container that cleans up unused queues from rabbitmq.

RABBITMQ_MANAGEMENT_HOST: url of rabbitmq management rest api (deafult="http://guest:guest@localhost:15672").
QUEUE_TIMEOUT_MINUTES = Number of minutes a queue has to be inactive before is marked for deletion (default=10).
CLEAN_MINUTES = Number of minutes between cleanups (default=10).
