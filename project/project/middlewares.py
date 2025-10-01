# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message
import time
# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter


import random


class TooManyRequestsRetryMiddleware(RetryMiddleware):
    
    def __init__(self, crawler):
        super(TooManyRequestsRetryMiddleware, self).__init__(crawler.settings)
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_response(self, request, response, spider):
        if request.meta.get('dont_retry', False):
            return response
        elif response.status == 429:
            self.crawler.engine.pause()
            time.sleep(60) # If the rate limit is renewed in a minute, put 60 seconds, and so on.
            self.crawler.engine.unpause()
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        elif response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        return response 


class CustomProxyMiddleware(object):
    def __init__(self, proxy_start=0, proxy_end=50):
        # Convert proxy range to integers
        self.proxy_start = int(proxy_start)
        self.proxy_end = int(proxy_end)
        # Load proxies from the external file
        self.proxy_list = self.load_proxies()[self.proxy_start:self.proxy_end]
        print(f"Initialized proxy middleware with range {self.proxy_start}-{self.proxy_end}, loaded {len(self.proxy_list)} proxies")

    @classmethod
    def from_crawler(cls, crawler):
        # Get proxy range from spider settings and convert to integers
        proxy_start = int(crawler.settings.get('PROXY_START', 0))
        proxy_end = int(crawler.settings.get('PROXY_END', 50))
        return cls(proxy_start, proxy_end)

    def process_request(self, request, spider):
        # Randomly select a proxy from the list
        if "proxy" not in request.meta:
            proxy = self.get_proxy()
            print(f'Using proxy for {spider.name}: {proxy}')  # Optional: for debugging
            request.meta["proxy"] = proxy

    def load_proxies(self):
        # Load the proxies from the .txt file
        with open("proxyscrape_premium_http_proxies.txt", "r") as file:
            return [line.strip() for line in file if line.strip()]

    def get_proxy(self):
        # Randomly pick a proxy from the list and ensure it has the correct scheme
        proxy = random.choice(self.proxy_list)
        if not proxy.startswith("http://") and not proxy.startswith("https://"):
            proxy = "http://" + proxy  # Append the "http://" scheme if missing
        return proxy


class ShowRequestsHeadersMiddleWare:
    def process_request(self, request, spider):

        print('------------------------------------------------------')
        #print('Request Headers:', request.headers )
        print('------------------------------------------------------')
        
        return None
    
    def process_response(self, request, response, spider):
        print('------------------------------------------------------')
        #print('Response:', response )
        print('------------------------------------------------------')
        return response


class ProjectSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn't have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class ProjectDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)
