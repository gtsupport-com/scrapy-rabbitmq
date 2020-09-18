__author__ = 'roycehaynes'

from scrapy.spiders import Spider
from scrapy import signals
from scrapy.exceptions import DontCloseSpider

import scrapy_rabbitmq.connection as con


class RabbitMQMixin(object):
    """ A RabbitMQ Mixin used to read URLs from a RabbitMQ queue.
    """


    def __init__(self, *args, **kwargs):
        print('>>>>>>>>>>>>>RabbitMQMixin#__init__')
        self.crawler = args[0]
        self.queue_name = self.crawler.settings.get('RABBITMQ_QUEUE_NAME')
        self.connection = con.from_settings(self.crawler.settings) 
        self.channel = self.connection.channel() 
        self.channel.queue_declare(self.queue_name)      
    
    
    def _before_schedule(self, url):
        """ Preprocess url before schedule to create custom Request object
        """
        req = None
        
        if hasattr(self, 'before_schedule'):
            req = self.before_schedule(url)
        else:
            req = url
        
        return req
    
                
    def next_request(self):
        """ Provides a request to be scheduled.
        :return: Request object or None
        """
        print('>>>>>>>>>>>next_request:queue:{}'.format(self.queue_name))
        method, header, body = self.channel.basic_get(self.queue_name)
        
        if body:
            self.channel.basic_ack(delivery_tag=method.delivery_tag)
            
            url = self.before_schedule(body.decode('UTF-8'))
            print('>>>>>>>>>>>next_request:got url:{}'.format(url))
            return url


    def schedule_next_request(self):
        """ Schedules a request, if exists.

        :return:
        """
        req = self.next_request()
        if req:
            self.crawler.engine.crawl(req, spider=self)


    def spider_idle(self):
        """ Waits for request to be scheduled.
        :return: None
        """
        print('>>>>>>>>>>spider_idle')
        self.schedule_next_request()
        raise DontCloseSpider


    def item_scraped(self, *args, **kwargs):
        """ Avoid waiting for spider.
        :param args:
        :param kwargs:
        :return: None
        """
        self.schedule_next_request()
        
    
    def spider_closed(self, spider):
        print('>>>>>>>>>>>>>Spider closed: %s', spider.name)
        self.connection.close()
    


class RabbitMQSpider(RabbitMQMixin, Spider):
    """ Spider that reads urls from RabbitMQ queue when idle.
    """

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        print('>>>>>>>>>>>>>from_crawler')
        spider = super(RabbitMQSpider, cls).from_crawler(crawler, crawler, *args, **kwargs)
        spider.settings = crawler.settings
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        crawler.signals.connect(spider.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider


