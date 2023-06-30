import scrapy
from scrapy import Request
from scrapy.utils.response import open_in_browser
from copy import deepcopy
from pymongo import MongoClient
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from time import sleep
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class GadsdenspiderSpider(scrapy.Spider):
    name = 'gad'

    # allowed_domains = ['gadsden.com']
    # start_urls = [
    #     'https://qpublic.schneidercorp.com/Application.aspx?AppID=814&LayerID=14537&PageTypeID=4&PageID=6817&KeyValue=1-35-4N-4W-0000-00313-0300']

    # https://qpublic.schneidercorp.com/Application.aspx?AppID=814&LayerID=14537&PageTypeID=4&PageID=6817&KeyValue=1-35-4N-4W-0000-00313-0300
    def start_requests(self):
        input_file = open('gadsden.txt', 'r', encoding='utf-8')
        lines = input_file.readlines()
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-logging')
        for line in lines:
            line = str(line).replace('\n', '')
            # line = '1-31-4N-3W-0000-00332-0300'
            search_link = 'https://qpublic.schneidercorp.com/Application.aspx?AppID=814&LayerID=14537&PageTypeID=4&PageID=6817&KeyValue='
            url = search_link + str(line)
            print('ID:', line)
            dic = {}
            browser = Chrome(options=options)
            dic = self.selenium_parse(browser, url)
            try:
                browser.quit()
            except:
                pass
            yield Request(url, meta=dic, callback=self.parse)
            # break

    def selenium_parse(self, browser, url):
        browser.get(url)
        dic = {'sales_by_distance': '', 'tax_collector': ''}
        try:
            btn = browser.find_element(By.CSS_SELECTOR, 'div.modal-dialog a.btn-primary')
            browser.execute_script('arguments[0].click()', btn)
            print('Pop up closed')
        except:
            pass
        try:
            btns = browser.find_element_by_xpath("//input[contains(@name,'RecentSalesInArea')]")
            browser.execute_script('arguments[0].click()', btns)
            sleep(3)
            d = self.parse_tax_by_table_data(browser)
            dic['sales_by_distance'] = d
        except:
            pass

        finally:
            browser.get(url)
        dic['tax_collector'] = self.parse_tax_collector(browser)
        return dic

    def parse_tax_collector(self, browser):
        link = browser.find_element(By.XPATH, "//a[.='Click here to view the Tax Collector website.']").get_attribute(
            "href")
        browser.get(link)
        frame = WebDriverWait(browser, 20).until(
            EC.presence_of_element_located((By.XPATH, "//frame[@name='body']")))
        browser.switch_to.frame(frame)
        temp = WebDriverWait(browser, 20).until(
            EC.presence_of_element_located((By.XPATH, "//b[contains(.,'Tax Type')]")))
        rows = browser.find_elements(By.XPATH,
                                     "//b[contains(.,'Tax Type')]/parent::font/parent::td/parent::tr/parent::tbody/tr")
        d = {}
        temp = rows[1].find_elements(By.CSS_SELECTOR, 'b')
        d['account_number'] = temp[0].get_attribute("innerHTML").replace("\n", '')
        d['tax_type'] = temp[1].get_attribute("innerHTML").replace("\n", '')
        d['tax_year'] = temp[2].get_attribute("innerHTML").replace("\n", '')
        volerem = rows[7].find_element(By.CSS_SELECTOR, 'table').find_elements(By.CSS_SELECTOR, 'tr')
        headlist = []
        headdic = {}
        d['ad_valorem_taxes'] = []
        heads = volerem[0].find_elements(By.CSS_SELECTOR, "td font b")
        for h in heads:
            h = h.get_attribute("innerHTML").replace('&nbsp;\n', '').strip().replace(" ", '_').lower()
            headlist.append(h)
            headdic[h] = ''
        for row in volerem[1:]:
            tds = row.find_elements(By.TAG_NAME, 'font')
            if len(tds) != len(headlist):
                continue
            count = 0
            k = deepcopy(headdic)
            for t in tds:
                value = t.get_attribute('innerHTML').replace('\n', '').replace("&nbsp;", '').strip()
                k[headlist[count]] = value
                count += 1
            d['ad_valorem_taxes'].append(k)
        nonvolerem = rows[9].find_element(By.CSS_SELECTOR, 'table').find_elements(By.CSS_SELECTOR, 'tr')
        headlist = []
        headdic = {}
        d['non_ad_valorem_taxes'] = []
        heads = nonvolerem[0].find_elements(By.CSS_SELECTOR, "td font b")
        for h in heads:
            h = h.get_attribute("innerHTML").replace('&nbsp;\n', '').strip()
            headlist.append(h)
            headdic[h] = ''
        for row in nonvolerem[1:]:
            tds = row.find_elements(By.TAG_NAME, 'font')
            if len(tds) != len(headlist):
                continue
            count = 0
            k = deepcopy(headdic)
            for t in tds:
                value = t.get_attribute('innerHTML').replace('\n', '').strip()
                k[headlist[count]] = value
                count += 1
            d['non_ad_valorem_taxes'].append(k)
        d['amount_dues'] = []
        amount_table = browser.find_elements(By.XPATH,
                                             "//b[contains(.,'Amount Due')]/parent::font/parent::td/parent::tr/parent::tbody/tr")
        for row in amount_table[1:]:
            try:
                values = row.find_elements(By.CSS_SELECTOR, 'b')
                p = values[0]
            except:
                values = row.find_elements(By.CSS_SELECTOR, 'font')
            date = values[0].get_attribute('innerHTML').replace('\n', '').replace('&nbsp;', '').strip()
            am = values[1].get_attribute('innerHTML').replace('\n', '').replace('&nbsp;', '').strip()
            if date == '':
                continue
            d['amount_dues'].append({date: am})
        return d

    def parse_tax_by_table_data(self, browser):
        headers = browser.find_elements(By.CSS_SELECTOR, 'table thead th')
        md = {}
        lst = []
        d = []
        # print('heads', len(headers))
        for head in headers:
            head = str(head.get_attribute('innerHTML')).replace('<br>', '').split('<')[0].replace('\n',
                                                                                                  '').strip().replace(
                '  ', '_').replace(' ', '_').replace('/', '_').lower()
            md[head] = ''
            lst.append(head)
        rows = browser.find_elements(By.CSS_SELECTOR, 'table tbody tr')
        # print('rows', len(rows))
        temp = 0
        for row in rows:
            values = row.find_elements(By.CSS_SELECTOR, 'td,th')
            # print('values', len(values))
            count = 0
            temp += 1
            m = deepcopy(md)
            for value in values[1:]:
                try:
                    v = value.find_element(By.TAG_NAME, 'a').get_attribute('innerHTML')
                except:
                    v = value.get_attribute('innerHTML')
                m[lst[count]] = str(v).replace('<br>', ' ').replace('&nbsp;', '')
                count += 1
            # print(temp, m)
            if temp == 30:
                d.append(m)
        return d

    def parse(self, response):
        maindict = {
            'folio': str(response.request.url).split('KeyValue=')[-1],
            'url': response.request.url,
            'parcel_summary': '',
            'owner_info': '',
            'valuation': '',
            'land_info': '',
            'building_info': '',
            'sales': '',
            'area_sale_report': {
                'sales_by_distance': response.meta['sales_by_distance'],
            },
            'extra_features': '',
            'tax_collector': response.meta['tax_collector'],
            'maps': '',
            'sketches': '',
        }
        maindict['parcel_summary'] = self.parse_parcel_summary(response)
        maindict['owner_info'] = self.parse_owner_info(response)
        maindict['land_info'] = self.parse_land_info(response)
        maindict['building_info'] = self.parse_building_info(response)
        maindict['extra_features'] = self.parse_extra_features(response)
        maindict['sales'] = self.parse_sales(response)
        maindict['valuation'] = self.parse_valuation(response)
        maindict['maps'] = self.parse_maps(response)
        maindict['sketches'] = self.parse_sketches(response)
        print(maindict)
        yield {'data': maindict}

    def parse_parcel_summary(self, response):
        trs = response.css("#ctlBodyPane_ctl00_mSection table tr")
        # print('len', len(trs))
        parcel_summary = {}
        for tr in trs:
            try:
                key = str(tr.xpath('.//td/text()').extract()[0]).replace('*', '').replace('/', '_').replace('\u00a0',
                                                                                                            ' ').strip().replace(
                    ' ',
                    '_').lower()
                if key == '':
                    continue
                value = str(tr.css('span::text').extract()[0]).strip('\n').strip()
                parcel_summary[key] = value
            except Exception as e:
                continue
        return parcel_summary

    def parse_owner_info(self, response):
        try:
            name = response.css('#ctlBodyPane_ctl01_mSection .module-content').css('a::text').extract()[0]
        except:
            name = response.xpath("//span[contains(@id,'PrimaryOwnerName')]/text()").extract()[0]
        address = response.xpath('//span[contains(@id,"OwnerAddress")]/text()').extract()
        address = ' '.join(address)
        owner = {'name': name, 'address': address}
        return owner

    def parse_land_info(self, response):
        table = response.xpath("//table[contains(@id,'gvwLand')]")
        return self.parse_table(response, table)

    def parse_extra_features(self, response):
        table = response.css("#ctlBodyPane_ctl05_mSection table")
        return self.parse_table(response, table)

    def parse_sales(self, response):
        table = response.css("#ctlBodyPane_ctl06_mSection table")
        return self.parse_table(response, table)

    def parse_valuation(self, response):
        table = response.css("#ctlBodyPane_ctl08_mSection table")
        rows = table.css('tbody tr')
        headslist = []
        headsdic = {}
        try:
            ths = table.css('thead tr th')
        except:
            return {}
        for h in ths:
            t = h.xpath('./text()').extract()[0].replace('/', ' ').strip().replace(' ', '_').lower()
            headslist.append(t)
            headsdic[t] = {}
        # print(headslist)
        data = []
        for row in rows:
            tds = row.css('th,td')
            # print('lenth',len(tds))
            count = 0
            name = tds[0].xpath('./text()').extract()[0].replace('/', ' ').replace('(', '').replace(')',
                                                                                                    '').strip().replace(
                ' ', '_').lower()
            for td in tds[1:]:
                try:
                    value = td.css('a::attr(href)').extract()[0]
                except:
                    value = td.xpath('./text()').extract()[0]
                headsdic[headslist[count]][name] = value
                count += 1
        return headsdic

    def parse_building_info(self, response):
        # open_in_browser(response)
        tables = response.css("#ctlBodyPane_ctl03_mSection table")
        d = []
        for table in tables:
            rows = table.css("tr")
            data = {}
            for row in rows:
                # print(row)
                key = row.css('td::text').extract()[0].replace("\r", '').replace("\n", '').strip().replace(" ",
                                                                                                           '_').lower()
                value = row.css('td').css('span::text').extract()[0].replace("\r", '').replace("\n", '').strip()
                if key == '':
                    continue
                data[key] = value
            d.append(data)
        return d

    def parse_table(self, response, table):
        rows = table.css('tbody tr')
        # print('')
        headslist = []
        headsdic = {}
        try:
            ths = table.css('thead tr th')
        except:
            return {}
        for h in ths:
            t = h.xpath('./text()').extract()[0].replace('/', ' ').strip().replace(' ', '_').lower()
            headslist.append(t)
            headsdic[t] = ''
        # print(headslist)
        data = []
        for row in rows:
            tds = row.css('th,td')
            # print('lenth',len(tds))
            tt = deepcopy(headsdic)
            count = 0
            for td in tds:
                try:
                    value = td.css('a::text').extract()[0]
                except:
                    value = td.xpath('./text()').extract()[0]
                tt[headslist[count]] = value.strip()
                count += 1
            data.append(tt)
        return data

    def parse_sketches(self, response):
        images = response.css("#ctlBodyPane_ctl11_mSection").css("img::attr(src)").extract()
        lst = []
        for im in images:
            lst.append(im)
        return lst

    def parse_maps(self, response):
        maps = response.xpath("//tr[contains(@id,'trMapLink')]//a/@href").extract()
        lst = []
        for im in maps:
            lst.append('https://qpublic.schneidercorp.com/' + im)
        return lst
