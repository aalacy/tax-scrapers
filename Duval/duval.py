from webbrowser import Chrome

import scrapy
import re
from copy import deepcopy
from scrapy import Request
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from time import sleep
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from scrapy.crawler import CrawlerProcess
import pdb

from util import Util

myutil = Util('Duval')

class DuvalSpider(scrapy.Spider):
    name = 'duval'
    allowed_domains = ['paopropertysearch.coj.net']
    start_urls = ['https://paopropertysearch.coj.net/Basic/Detail.aspx?RE=0000010005']
    base_url = 'https://paopropertysearch.coj.net'

    def start_requests(self):
        input_file = open('duval.txt', 'r', encoding='utf-8')
        lines = input_file.readlines()
        for id in lines:
            url = 'https://paopropertysearch.coj.net/Basic/Detail.aspx?RE=' + id.replace('\n', '').strip()
            headers = {
                'USER_AGENT': myutil._get_ua(),
                'proxy': '108.59.14.200:13152'
            }
            yield Request(url=url, headers=headers, callback=self.parse)


    def parse(self, response):
        dic = {
            'folio': response.request.url.split('=')[-1],
            'url': response.request.url,
        }
        dic['owner_info'] = self.parse_owner_info(response)
        dic['prop_info'] = self.parse_prop_info(response)
        dic['value_summary'] = self.parse_value_summary(response)
        dic['taxable_values_and_exemptions'] = self.parse_taxable_values_and_exemptions(response)
        dic['sales_history'] = self.parse_sales_history(response)
        dic['extra_features'] = self.prase_extra_features(response)
        dic['land_and_legal'] = self.parse_land_and_legal(response)
        dic['buildings_info'] = self.parse_buildings(response)
        dic['trim_notice'] = self.parse_trim_notice(response)
        dic['extended_trim'] = self.parse_extended_trim(response)
        dic['property_record_card_pdf'] = self.parse_property_record_card_pdf(response)
        
        myutil._normalizeKeys(dic)
        myutil._save_to_mongo(permit_type='property', data=dic)
        myutil._download_pdf(dic)

    def parse_owner_info(self, response):
        temp = {}
        try:
            temp['name'] = response.xpath("//*[contains(@id,'lblOwnerName')]/text()").extract()[0]
        except:
            temp['name'] = ''

        try:
            t = response.xpath("//div[@id='ownerName']//ol//span/text()").extract()
            temp['address'] = ' '.join(t)
        except:
            temp['address'] = ''
        try:
            t = response.xpath("//div[@id='primaryAddr']//ol//span/text()").extract()
            temp['primary_address'] = ' '.join(t)
        except:
            temp['primary_address'] = ''
        try:
            temp['book_page'] = response.xpath("//*[contains(@id,'lblBookPage')]/text()").extract()[0]
        except:
            temp['book_page'] = ''
        try:
            temp['title'] = response.xpath("//*[contains(@id,'lblTileNumber')]/text()").extract()[0]
        except:
            temp['title'] = ''

        return temp

    def parse_prop_info(self, response):
        rows = response.xpath("//*[contains(@id,'propDetail_data')]//tr")
        temp = {}
        # print('rows:', len(rows))
        for row in rows:
            key = row.css('th').css('span::text').extract()[0].replace("#", 'no').replace('.', '').strip().replace(" ",
                                                                                                                   '_').lower()
            value = row.css('td').css('span::text').extract()[0]
            temp[key] = value
        return temp

    def parse_value_summary(self, response):
        rows = response.xpath("//*[contains(@id,'propValue')]//tr")
        temps = []
        try:
            heads = rows[0].css('span::text').extract()
        except:
            return {}
        for h in heads:
            temps.append(h.strip().lower().replace(' ', '_'))
        rows = rows[1:]
        # print('len', len(rows))
        dic = {temps[0]: {}, temps[1]: {}}
        for row in rows:
            texts = row.css('span::text').extract()
            key = texts[0].strip().lower().replace('(', '').replace(')', '').replace('/', '').replace(' ', '_')
            v1 = texts[1]
            v2 = texts[2]
            dic[temps[0]][key] = v1
            dic[temps[1]][key] = v2
        return dic

    def parse_taxable_values_and_exemptions(self, response):
        tables = response.xpath("//*[contains(@id,'details_exemptions')]//div[contains(@id,'propExemptions')]")
        dic = {}
        for table in tables:
            name = table.css('h3').css('span::text').extract()[0].strip().lower().replace('/', '').replace(' ', '_')
            dic[name] = {}
            lis = table.css('ul.exemptionList li')
            for li in lis:
                key = li.css('span.prompt::text').extract()[0]
                key = re.sub(' +', ' ', key).replace("\r", '').replace("\n", '').strip().replace(' ', '_')
                value = \
                    li.xpath(".//span[@class='exemption'] | .//span[contains(@id,'Value')]").xpath(
                        './text()').extract()[0].replace("\n", '').strip()
                dic[name][key] = value
        return dic

    def parse_sales_history(self, response):
        rows = response.xpath("//*[contains(@id,'divSalesHistory')]//table//tr")
        return self.parse_table(response, rows)

    def parse_land_and_legal(self, response):
        dic = {}
        dic['land'] = self.parse_land(response)
        dic['legal'] = self.parse_legal(response)
        return dic

    def parse_land(self, response):
        rows = response.xpath("//*[contains(@id,'propLand')]//table//tr")
        return self.parse_table(response, rows)
    def parse_legal(self, response):
        rows = response.xpath("//*[contains(@id,'propLegal')]//table//tr")
        return self.parse_table(response, rows)

    def parse_table(self, response, selector):
        # print('rows', len(selector))
        rows = selector
        headslist = []
        headsdic = {}
        try:
            ths = rows[0].css('th')
        except:
            return {}
        for h in ths:
            try:
                t = h.xpath('./text()').extract()[0]
            except:
                t = h.css('span.shortTip,span.longTip').xpath('./text()').extract()[0]
            t = t.replace('/', ' ').strip().replace(' ', '_').lower()
            headslist.append(t)
            headsdic[t] = ''
        data = []
        rows = rows[1:]
        for row in rows:
            tds = row.css('td')
            tt = deepcopy(headsdic)
            count = 0
            for td in tds:
                try:
                    value = td.css('a::text').extract()[0]
                    if 'book' in headslist[count]:
                        value = td.css('a::attr(href)').extract()[0]
                except:
                    try:
                        value = td.xpath('./text()').extract()[0]
                    except:
                        value = ''
                tt[headslist[count]] = value
                count += 1
            data.append(tt)
        return data

    def parse_buildings(self, response):
        buildings = response.css('div.actualBuildingData')
        data = []
        for building in buildings:
            buil = {'info': {}, 'elements': {}, 'sketch': ''}
            try:
                t = building.xpath(".//span[contains(@id,'SiteAddressLine')]/text()").extract()
                buil['info']['address'] = ' '.join(t)
            except:
                buil['info']['address'] = ' '
            typetable = building.css('div.buildingType table tr')
            for row in typetable:
                key = row.css('th').css('span::text').extract()[0].replace(' ', '_').lower()
                value = row.css('td').css('span::text').extract()[0]
                buil['info'][key] = value
            typetable = building.css('div.typeList table tr')
            buil['info']['type'] = self.parse_table(response, typetable)
            elements = building.css('div.propBuildingElements table tr')
            buil['elements'] = self.parse_table(response, elements)
            buil['sketch'] = self.base_url + building.css("div.propBuildingImage img").xpath('./@src').extract()[0][
                                             2:].replace(' ', '%20')
            data.append(buil)
        return data

    def parse_trim_notice(self, response):
        rows = response.xpath("//table[contains(@id,'gridTaxDetails')]//tr")
        return self.parse_table(response, rows)

    def parse_extended_trim(self, response):
        rows = response.xpath("//div[contains(@id,'trimExtended')]//tr")
        headslist = []
        headsdic = {}
        try:
            ths = rows[0].css('th')
        except:
            return {}
        for h in ths:
            try:
                t = h.css('span').xpath('./text()').extract()[0]
            except:
                t = h.xpath('./text()').extract()[0]
            t = t.replace('/', ' ').strip().replace(' ', '_').lower()
            headslist.append(t)
            headsdic[t] = ''
        # print(headsdic)
        data = []
        rows = rows[1:]
        for row in rows:
            tds = row.css('th,td')
            tt = deepcopy(headsdic)
            count = 0
            for td in tds:
                try:
                    value = td.css('span::text').extract()[0]
                except:
                    try:
                        value = td.xpath('./text()').extract()[0]
                    except:
                        value = ''
                tt[headslist[count]] = value
                count += 1
            data.append(tt)
        return data

    def prase_extra_features(self, response):
        rows = response.xpath("//div[contains(@id,'divExtraFeatures')]//tr")
        return self.parse_table(response, rows)

    def parse_property_record_card_pdf(self, response):
        # https://paopropertysearch.coj.net/Basic/Detail.aspx?RE=0000010005&pdfYear=2019&pdfType=final
        rows = response.xpath("//table[contains(@id,'gvPRCFinal')]//tr/@onclick").extract()
        links = []
        for row in rows:
            values = row.split('PDF(')[-1].replace("'", '').replace(")", '').split(',')
            link = response.request.url + '&pdfYear={year}&pdfType={type}'.format(year=values[0].strip(),
                                                                                  type=values[1].strip())
            links.append({values[0].strip(): link})
        return links


if __name__ == '__main__':
    process = CrawlerProcess(settings={
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1',
        "CONCURRENT_REQUESTS": "100",
        "DOWNLOAD_DELAY": ".5",
        "RETRY_ENABLED": "False",
        "DOWNLOADER_CLIENT_TLS_CIPHERS": 'DEFAULT:!DH'
    })

    process.crawl(DuvalSpider)
    process.start() # the script will block here until the crawling is finished