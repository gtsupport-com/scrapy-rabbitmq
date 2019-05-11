# -*- coding: utf-8 -*-
import logging

try:
    import pika
except ImportError:
    raise ImportError("Please install pika before running scrapy-rabbitmq.")


RABBITMQ_CONNECTION_TYPE = 'blocking'
RABBITMQ_QUEUE_NAME = 'scrapy_queue'
RABBITMQ_CONNECTION_PARAMETERS = {'host': 'localhost'}
RABBITMQ_DSN = 'amqp://guest:guest@localhost/'


def from_settings(settings):
    """ Factory method that returns an instance of channel

        :param str connection_type: This field specifies connection adapter, it can be
            `blocking`, `select`, `tornado` or `twisted`

        See pika documentation for more details:
            https://pika.readthedocs.io/en/stable/modules/adapters/index.html

        Parameters is a dictionary that can
        include the following values:

            :param str host: Hostname or IP Address to connect to
            :param int port: TCP port to connect to
            :param str virtual_host: RabbitMQ virtual host to use
            :param pika.credentials.Credentials credentials: auth credentials
            :param int channel_max: Maximum number of channels to allow
            :param int frame_max: The maximum byte size for an AMQP frame
            :param int heartbeat_interval: How often to send heartbeats
            :param bool ssl: Enable SSL
            :param dict ssl_options: Arguments passed to ssl.wrap_socket as
            :param int connection_attempts: Maximum number of retry attempts
            :param int|float retry_delay: Time to wait in seconds, before the next
            :param int|float socket_timeout: Use for high latency networks
            :param str locale: Set the locale value
            :param bool backpressure_detection: Toggle backpressure detection

        :return: Channel object
    """
    
    connection_type = settings.get('RABBITMQ_CONNECTION_TYPE', RABBITMQ_CONNECTION_TYPE)
    connection_parameters = settings.get('RABBITMQ_CONNECTION_PARAMETERS', RABBITMQ_CONNECTION_PARAMETERS)
    connection_dsn = settings.get('RABBITMQ_DSN', RABBITMQ_DSN)
    pika_log_level = settings.get('RABBITMQ_PIKA_LOG_LEVEL', logging.WARNING)

    logging.getLogger('pika').setLevel(pika_log_level)

    if connection_type == 'blocking':
        from pika import BlockingConnection as ConnectionAdapter
    elif connection_type == 'select':
        from pika import SelectConnection as ConnectionAdapter
    elif connection_type == 'tornado':
        from pika.adapters.tornado_connection import TornadoConnection as ConnectionAdapter
    elif connection_type == 'twisted':
        from pika.adapters.twisted_connection import TwistedProtocolConnection as ConnectionAdapter
    else:
        raise RuntimeError("Adapter '{}' was not found!".format(connection_type))

    if connection_dsn:
        connection = ConnectionAdapter(pika.connection.URLParameters(connection_dsn))
    else:
        connection = ConnectionAdapter(pika.connection.ConnectionParameters(**connection_parameters))

    return connection
