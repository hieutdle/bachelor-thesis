from scrapy import Spider
from scrapy.selector import Selector
from crawler.items import CrawlerItem

class CrawlerSpider(Spider):
    name = "crawler"
    allowed_domains = ["moderndatastack.xyz"]
    start_urls = [
        "https://www.moderndatastack.xyz/company/Metabase",
    ]

    def parse(self, response):
            item = CrawlerItem()

            item['name'] = question.xpath(
                'div[@class="rowuser"]/a/strong/text()').extract_first()
            item['category'] = question.xpath(
                'div[@class="question"]/text()').extract_first()
            item['businessmodel'] = question.xpath(
                'div[@class="actionuser"]/a[@class="time"]/text()').extract_first()

            yield item