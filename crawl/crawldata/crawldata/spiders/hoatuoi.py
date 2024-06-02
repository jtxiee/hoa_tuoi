import scrapy
import os
from scrapy.crawler import CrawlerProcess
import json
import csv

class HoatuoiSpider(scrapy.Spider):
    name = "hoatuoi"
    allowed_domains = ["hoayeuthuong.com"]
    # start_urls = ["https://hoayeuthuong.com/"]
    jobs = []

    def start_requests(self):
            url = "https://hoayeuthuong.com/giaonhanh.aspx"
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        jobs_crawl = response.xpath('//*[@id="content"]/div/div[1]/div/div[2]')
        length = len(jobs_crawl)
        for i in range(1, length + 1):
            relative_url = response.xpath(f'//*[@id="content"]/div/div[1]/div[{i}]/div[2]/a/@href').get()
            url = 'https://hoayeuthuong.com/' + relative_url
            yield scrapy.Request(url, callback=self.parse_job)
            # yield{
            #     'url' :url
            # }
            
    def parse_job(self, response):
        data = {
            'tenhoa':  response.xpath('normalize-space(//*[@id="content"]/div/div[1]/div[2]/h2/text())').get(),
            'giahoa': response.xpath('normalize-space(//*[@id="content"]/div/div[1]/div[2]/div[1]/span[2]/text())').get(),
            'mota': response.xpath('normalize-space(//*[@id="content"]/div/div[1]/div[2]/div[2]/text())').get(),
            'thanhphan': response.xpath('//*[@id="content"]/div/div[1]/div[2]/ul/li/text()').getall()
        }
        self.jobs.append(data)

    def close(self, reason):
        # directory = 'C:\\Users\\ACER\\Desktop\\hoatuoi\\data\\crawl_data'
        directory = '/opt/airflow/data/crawl_data'
        
        with open(f'{directory}/hoatuoi_job.json', 'w',encoding='utf-8') as f:
            json.dump(self.jobs, f, ensure_ascii=False)

        with open(f'{directory}/hoatuoi_job.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(
                ['tenhoa', 'giahoa', 'mota', 'thanhphan'])
            for job in self.jobs:
                writer.writerow([
                    job['tenhoa'],
                    job['giahoa'],
                    job['mota'],
                    job['thanhphan'],
                ])
    
process = CrawlerProcess(settings={
    "CONCURRENT_REQUESTS": 3,
    "DOWNLOAD_TIMEOUT": 60,
    "RETRY_TIMES": 5,
    "ROBOTSTXT_OBEY": False,
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
})

process.crawl(HoatuoiSpider)
process.start()