from urllib import response
import scrapy

class PostsSpider(scrapy.Spider):
    name = "posts"
    start_url =[
        'https://www.moderndatastack.xyz/company/andfacts',
        'https://www.moderndatastack.xyz/company/Metabase'
    ]

    def parse(self,reponse):
        page = response.url.split('/')[-1]
        filename='posts-'