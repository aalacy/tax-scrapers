import scrapy
import scrapy
from scrapy import Request
from scrapy.utils.response import open_in_browser
from copy import deepcopy
import requests
from scrapy.http import HtmlResponse


class SarasotaSpiderSpider(scrapy.Spider):
    name = 'sara'

    def start_requests(self):
        input_file = open('sarasota.txt', 'r', encoding='utf-8')
        search_link = 'https://www.sc-pa.com/propertysearch/parcel/details/'
        lines = input_file.readlines()
        c = 0
        for line in lines:
            try:
                line = str(line).replace('\n', '')
                c += 1
                print(c," Going for ---",line,'------------------------')
                link = search_link + line
                yield Request(url=link, callback=self.parse)
            except:
                continue


    def parse(self, response):
        d = {'folio': response.request.url.split('/details/')[-1], 'url': response.request.url}
        d['owner_info'] = self.parse_owner_info(response)
        d['parcel_summary'] = self.parse_parcel_summary(response)
        try:
            d['buildings'] = self.parse_buildings(response)
        except:
            d['buildings'] = []
        try:
            d['extra_features'] = self.parse_extra_features(response)
        except:
            d['extra_features'] = []
        try:
            d['values'] = self.parse_values(response)
        except:
            d['values'] = []
        try:
            d['sales_and_transfer'] = self.parse_sales_and_transfer(response)
        except:
            d['sales_and_transfer'] = []
        try:
            d['associated_tangible_accounts'] = self.parse_associated_tangible_accounts(response)
        except:
            d['associated_tangible_accounts'] = []
        try:
            d['property_record_information'] = self.parse_property_record_information(response)
        except:
            d['property_record_information'] = []
        try:
            d['tax_link'] = response.xpath("//a[@id='tax-link']/@href").extract()[0]
            yield Request(d['tax_link'], meta=d, callback=self.parse_tax_collector_navigator)
        except:
            d['tax_link'] = ''
        yield {'data': d}

    def parse_owner_info(self, response):
        d = {}
        abs = response.xpath("//li[text()='Ownership:']/following-sibling::li/text()").extract()
        try:
            d['name'] = abs[0].strip()
        except:
            d['name'] = ''
        try:
            d['address'] = abs[-2].strip()
        except:
            d['address'] = ''
        try:
            d['situs_address'] = abs[-1].strip()
        except:
            d['situs_address'] = ''
        return d

    def parse_parcel_summary(self, response):
        d = {}
        rows = response.css('ul.resultr.spaced li')
        for row in rows:
            key = row.xpath('./strong/text()').extract()[0].replace('\u00a0', '').strip()
            value = row.xpath('./text()').extract()[0].replace('\u00a0', '').strip()
            d[key] = value
        return d

    def parse_extra_features(self, response):
        rows = response.xpath("//th[text()='Building Number']/parent::tr/parent::thead/parent::table")[0].css('tr')
        return self.parse_table_1(rows)

    def parse_values(self, response):
        rows = response.xpath("//th[text()='Land']/parent::tr/parent::thead/parent::table")[0].css('tr')
        return self.parse_table_1(rows)

    def parse_sales_and_transfer(self, response):
        rows = response.xpath("//th[text()='Transfer Date']/parent::tr/parent::thead/parent::table")[0].css('tr')
        return self.parse_table_1(rows)

    def parse_associated_tangible_accounts(self, response):
        rows = response.xpath("//th[text()='Account Number']/parent::tr/parent::thead/parent::table")[0].css('tr')
        return self.parse_table_1(rows)

    def parse_property_record_information(self, response):
        rows = response.xpath("//th[text()='FIRM Panel']/parent::tr/parent::thead/parent::table")[0].css('tr')
        return self.parse_table_1(rows)

    def parse_buildings(self, response):
        rows = response.css("table#Buildings tr")
        tt = self.parse_building_table_2(rows)
        for t in tt:
            t['Situs'] = t.pop('Situs - click address for building details')
            res = requests.get(t['building_link'], verify=False)
            res = HtmlResponse(url=t['building_link'], body=res.text, encoding='utf-8')
            t['data'] = self.parse_building_info_3(res)
        return tt

    def parse_building_table_2(self, rows):
        try:
            heads = rows[0].css('th')
        except:
            return []
        data = []
        headlist = []
        headdict = {}
        for h in heads:
            h = h.xpath('./text()').extract()[0]
            headdict[h] = ''
            headlist.append(h)
        rows = rows[1:]
        for row in rows:
            tds = row.css('td')
            if len(tds) != len(headlist):
                continue
            count = 0
            temp = deepcopy(headdict)
            for td in tds:
                try:
                    value = 'http://www.sc-pa.com' + td.xpath('./a/@href').extract()[0]
                    temp['building_link'] = value
                except:
                    pass
                value = td.xpath('./text() | ./a/text()').extract()[0].replace("\u00a0", '').strip()
                # value = ''.join(value).replace('\r', '').replace('\n', '').strip()
                temp[headlist[count]] = value
                count += 1
            data.append(temp)
        return data

    def parse_building_info_3(self, response):
        sub_area_rows = response.xpath("//th[text()='Gross Area']/parent::tr/parent::thead/parent::table//tr")
        extra_features = response.xpath("//th[text()='Building Number']/parent::tr/parent::thead/parent::table//tr")
        d = {}
        d['info'] = self.parse_building_info_table(response)
        d['sub_area'] = self.parse_table_1(sub_area_rows)
        d['extra_features'] = self.parse_table_1(extra_features)
        try:
            d['sketch'] = 'https://www.sc-pa.com' + response.xpath("//a[contains(@href,'sketches')]/@href").extract()[0]
        except:
            d['sketch'] = ''
        return d

    def parse_building_info_table(self, response):
        rows = response.css('ul.bullet li')
        d = {}
        for row in rows:
            t = row.xpath('./text()').extract()[0].split(':')
            key = t[0]
            value = t[1]
            d[key] = value
        return d

    def parse_table_1(self, rows):
        try:
            heads = rows[0].css('th')
        except:
            return []
        data = []
        headlist = []
        headdict = {}

        for h in heads:
            h = h.xpath('./text()').extract()[0]
            headdict[h] = ''
            headlist.append(h)
        rows = rows[1:]
        for row in rows:
            tds = row.css('td')
            if len(tds) != len(headlist):
                continue
            count = 0
            temp = deepcopy(headdict)
            for td in tds:
                try:
                    value = td.xpath('./text() | ./a/text()').extract()[0].replace("\u00a0", '').strip()
                except:
                    value = ''
                # value = ''.join(value).replace('\r', '').replace('\n', '').strip()
                temp[headlist[count]] = value
                count += 1
            data.append(temp)
        return data

    def parse_tax_collector_navigator(self, response):
        sid = response.request.url.split('sid=')[1]
        folio = response.meta['folio']
        link = 'http://sarasotataxcollector.governmax.com/collectmax/tab_collect_mvptaxV7.120617.asp?reset=True&body=tab_collect_mvptaxV7.120617.asp&ParcelID=[f]&ParcelYear=[y]&eBillingInvitation=eBillingInvitation&Parcelacct={folio}&t_nm=collect_mvptax&sid={sid}&agencyid=sarasotataxcollector'
        link = link.format(folio=folio, sid=sid)
        yield Request(link, meta=response.meta, callback=self.parse_tax_collector_navigator2)

    def parse_tax_collector_navigator2(self, response):
        link = response.request.url + '&wait=done'
        yield Request(link, meta=response.meta, callback=self.parse_tax_collector)

    def parse_tax_collector(self, response):
        d = {}
        # open_in_browser(response)
        rows = response.xpath("//b[contains(.,'Type Tax')]/parent::font/parent::td/parent::tr/parent::table/tr")
        temp = rows[1].css('b')
        d['account_number'] = temp[0].xpath('./text()').extract()[0].replace("\n", '')
        d['tax_type'] = temp[1].xpath('./text()').extract()[0].replace("\n", '')
        d['tax_year'] = temp[2].xpath('./text()').extract()[0].replace("\n", '')
        volerem = rows[7].css('table')[0].css('tr')
        headlist = []
        headdic = {}
        d['ad_valorem_taxes'] = []
        heads = volerem[0].css("td font b")
        for h in heads:
            h = h.xpath('./text()').extract()[0].replace('&nbsp;\n', '').strip().replace(" ", '_').lower()
            headlist.append(h)
            headdic[h] = ''
        for row in volerem[1:]:
            tds = row.css('font')
            if len(tds) != len(headlist):
                continue
            count = 0
            k = deepcopy(headdic)
            for t in tds:
                value = t.xpath('./text()').extract()[0].replace('\n', '').replace("&nbsp;", '').strip()
                k[headlist[count]] = value
                count += 1
            d['ad_valorem_taxes'].append(k)
        nonvolerem = rows[9].css('table')[0].css('tr')
        headlist = []
        headdic = {}
        d['non_ad_valorem_taxes'] = []
        heads = nonvolerem[0].css("td font b")
        for h in heads:
            h = h.xpath('./text()').extract()[0].replace('&nbsp;\n', '').strip()
            headlist.append(h)
            headdic[h] = ''
        for row in nonvolerem[1:]:
            tds = row.css('font')
            if len(tds) != len(headlist):
                continue
            count = 0
            k = deepcopy(headdic)
            for t in tds:
                value = t.xpath('./text()').extract()[0].replace('\n', '').strip()
                k[headlist[count]] = value
                count += 1
            d['non_ad_valorem_taxes'].append(k)
        d['amount_dues'] = []
        amount_table = response.xpath(
            "//b[contains(.,'Amount Due')]/parent::font/parent::td/parent::tr/parent::table/tr")
        for row in amount_table[1:]:
            try:
                values = row.css('b')
                p = values[0]
            except:
                values = row.css('font')
            date = values[0].xpath('./text()').extract()[0].replace('\n', '').replace('&nbsp;', '').strip()
            am = values[1].xpath('./text()').extract()[0].replace('\n', '').replace('&nbsp;', '').strip()
            if date == '':
                continue
            d['amount_dues'].append({date: am})
        response.meta['tax_collector'] = d
        try:
            del response.meta['depth']
            del response.meta['download_timeout']
            del response.meta['download_slot']
            del response.meta['download_latency']
            del response.meta['redirect_ttl']
            del response.meta['redirect_times']
            del response.meta['redirect_urls']
            del response.meta['redirect_reasons']
        except:
            pass
        yield {'data': response.meta}
