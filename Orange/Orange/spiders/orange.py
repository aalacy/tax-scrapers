import urllib.request
from urllib import request

import requests
import scrapy
import os
from scrapy import Request
from scrapy.utils.response import open_in_browser
from w3lib.http import basic_auth_header
import base64
from copy import deepcopy



class OrangeSpider(scrapy.Spider):
    name = 'orange'
    # allowed_domains = ['google.com']
    # start_urls = ['http://google.com/']


    def start_requests(self):
        input_file = open('orange.txt', 'r', encoding='utf-8')
        self.base_url='https://www.ocpafl.org/'
        self.base_url2='http://pt.octaxcol.com/'
        lines = input_file.readlines()
        c=0
        for line in lines:
            c+=1
            dic={}
            line = str(line).replace('\n', '').replace('\t','').strip()
            # line='272001000000006'
            link = "https://www.ocpafl.org/Searches/ParcelSearch.aspx/PID/{}".format(line)
            dic['folio'] = line
            print(c," Going For Folio",line,'==================================================')
            yield Request(link, meta={'data':dic, 'proxy': 'http://95.211.175.167:13150'}, callback=self.parse)
            
    def parse(self, response):
        dic = response.meta['data']
        dic['owner_info'] = self.parse_owner_and_property_info(response)
        dic['property_features'] = self.property_features(response)
        dic['taxes'] = self.parse_taxes(response)
        dic['sales_history'] = self.parse_sales_history(response)
        dic['pictures'] = self.parse_pictures(response)
        dic['trim'] = self.parse_trim(response)
        dic['maps'] = self.parse_map(response)
        link = response.xpath("//a[contains(@onclick,'Tax Collector - View Taxes')]/@href").extract()[0]
        yield Request(link, meta={'data': dic, 'proxy': 'http://95.211.175.167:13150'}, callback=self.parse_tax_collector)

    def parse_owner_and_property_info(self,response ):
        dic = {}
        try:
            names = response.xpath("//legend[contains(text(),'Name(s)')]/following-sibling::div/text()").extract()
            dic['name']=''
            for name1 in names:
                dic['name']+=name1.replace('\n', '').replace("\t", '').strip()
        except:
            pass
        dic['mailing_address']=''
        mailing_addresses =response.xpath("//legend[contains(text(),'Mailing Address On')]/parent::fieldset/text()").extract()
        for name1 in mailing_addresses:
            dic['mailing_address'] += name1.replace('\n', '').replace("\t", '').strip().replace('\r\n','')
        try:
            physical_street_addresses =response.xpath("//legend[contains(text(),'Physical Street Address')]/parent::fieldset/text()").extract()
            dic['physical_street_address']=''
            for name1 in physical_street_addresses:
                dic['physical_street_address'] += name1.replace('\n', '').replace("\t", '').strip().replace('\r\n', '')
        except:
            pass
        try:
            postal_city_and_zip_codes =response.xpath("//legend[contains(text(),'Postal City and Zipcode')]/parent::fieldset/text()").extract()
            dic['postal_city_and_zip_code'] = ''
            for name1 in postal_city_and_zip_codes:
                dic['postal_city_and_zip_code'] += name1.replace('\n', '').replace("\t", '').strip().replace('\r\n', '')
        except:
            pass
        try:
            property_uses = response.xpath("//legend[contains(text(),'Property Use')]/parent::fieldset/text()").extract()
            dic['property_use'] = ''
            for name1 in property_uses:
                dic['property_use'] += name1.replace('\n', '').replace("\t", '').strip().replace('\r\n', '')
        except:
            pass
        try:
            municipalitys = response.xpath("//legend[contains(text(),'Municipality')]/parent::fieldset/text()").extract()
            dic['municipality'] = ''
            for name1 in municipalitys:
                dic['municipality'] += name1.replace('\n', '').replace("\t", '').strip().replace('\r\n', '')
        except:
            pass
        return dic

    def property_features(self,response):
        dic = {}
        description = response.xpath("//legend[contains(.,'Property Description')]/parent::fieldset/text()").extract()
        dic['description'] = ''
        for name1 in description:
            dic['description'] += name1.replace('\n', '').replace("\t", '').strip().replace('\r\n', '').replace("&nbsp;", '').strip()

        total_area_fts = response.xpath("//td[contains(@id,'_PropertySzInSqFt')]/text()").extract()
        dic['total_area_ft'] = ''
        for name1 in total_area_fts:
            dic['total_area_ft'] += name1.replace('\n', '').replace("\t", '').strip().replace('\r\n', '').replace(
                "&nbsp;", '').strip()

        total_area_acrs = response.xpath("//td[contains(@id,'_PropertySzInAcres')]/text()").extract()
        dic['total_area_acr'] = ''
        for name1 in total_area_acrs:
            dic['total_area_acr'] += name1.replace('\n', '').replace("\t", '').strip().replace('\r\n', '').replace(
                "&nbsp;", '').strip()

        dic['land_info'] = self.parse_land_info(response)
        dic['buildings'] = self.parse_building_info(response)
        dic['extra_features'] = self.parse_extra_features(response)
        return dic

    def parse_extra_features(self,response):
        rows = response.xpath("//table[contains(@id,'_PropertyFeatures1_xFOBGrid')]//tr")
        return self.parse_table_type1(rows)

    def parse_land_info(self,response ):
        rows = response.xpath("//table[contains(@id,'_PropertyFeatures1_LandGrid')]//tr")
        return self.parse_table_type1(rows)

    def parse_building_info(self,response):
        rows = response.xpath("//table[contains(@id,'_PropertyFeatures1_BuildingGrid')]//table//tr")
        d = {}
        for row in rows:
            key = row.css('th::text').extract()[0].replace('\n', '').strip()
            value = row.css('td::text').extract()[0].replace('\n', '').strip()
            d[key] = value
        return d

    def parse_table_type1(self, rows):
        try:
            heads = rows[0].css('th')
        except:
            return []
        rows = rows[1:]
        dic = {}
        lst = []
        data = []
        for head in heads:
            try:
                head = head.css('span::text').extract()[0]
            except:
                head = head.xpath("./text()").extract()[0]
            head = head.split("<a")[0].replace('\n', '').strip().replace('/', '_').replace(' ', '_').lower()
            lst.append(head)
            dic[head] = {}
        for row in rows:
            values = row.css('td')
            if len(values) != len(lst):
                continue
            tt = deepcopy(dic)
            count = 0
            for value in values:
                try:
                    value = value.css('a::text').extract()[0]
                except:
                    try:
                        value = value.css('span.LabelPart::text').extract()[0]
                    except:
                        try:
                            value = value.css('span::text').extract()[0]
                        except:
                            try:
                                value = value.xpath("./text()").extract()[0]
                            except:
                                value=''
                tt[lst[count]] = value.replace('<br>', ',').replace('&nbsp;', '').replace('\n', '').strip()
                count += 1
            data.append(tt)
        return data

    def parse_taxes(self,response):
        d = {}
        d['ad_valorem_assessments'] = self.ad_valorem_assessment(response)
        d['non_ad_valorem_assessments'] = self.non_nad_valorem_assessment(response)
        try:
            d['total_tax'] = response.xpath("//span[contains(@id,'_ValueTax_ValuesTaxes1_NATotalTax')]/text()").extract()[0]
        except:
            d['total_tax'] = ''
        try:
            d['tax_break_down'] = response.xpath('div.ChartImageContainer img').extract()[0]
        except:
            d['tax_break_down'] = ''
        return d

    def ad_valorem_assessment(self, response):
        rows = response.xpath("//table[contains(@id,'_ValueTax_ValuesTaxes1_Grid1')]//tr")
        return self.parse_table_type1(rows[:-1])

    def non_nad_valorem_assessment(self, response):
        rows =response.xpath("//table[contains(@id,'_ValuesTaxes1_NonAdValoremTaxes1_Grid1')]//tr")
        return self.parse_table_type1(rows[:-1])

    def parse_sales_history(self,response):
        rows = response.xpath("//table[contains(@id,'_SaleAnalysis_SalesAnalysis1_Grid1')]//tr")
        return self.parse_table_type1(rows)

    def parse_pictures(self,response):
        imgs = response.xpath("//td[@class='ImageD']/a/img[@alt='Parcel Photo']/@src").extract()
        l = []
        for i in imgs:
            l.append(self.base_url+str(i))
        return l

    def parse_trim(self,response):
        try:
            link = response.xpath("//a[@target='_trim']/@href").extract()[0]
            res = requests.post('https://trimnet.ocpafl.org/Default.aspx/WSGetPDF', json={'p_page': link})
            return res.json()['d']
        except:
            return ''

    def parse_map(self,response):
        try:
            link = response.xpath("//a[contains(@onclick,'GIS Parcel')]/@href").extract()[0]
            return link
        except:
            return ''

    def parse_tax_collector(self,response):
        dic = response.meta['data']
        d={}
        d['summary'] = self.summary(response)
        d['current_taxes_and_unpaid_delinquent_warrants'] = self.parse_table_1(response)
        d['unpaid_real_estate_certificates'] = self.parse_table_2(response,'table#mainContent_tblUnpaidCerts tr')
        d['other_real_estate_certificates'] = self.parse_table_3(response,'table#mainContent_tblOtherCerts tr')
        dic['tax_collector']=d
        yield {'data':dic}

    def summary(self,response):
        d = {}
        try:
            d['parcel_id'] = response.css('td#mainContent_cellDisplayIdentifier::text').extract()[0]
        except:
            d['parcel_id'] = ''
        try:
            d['owner_and_address'] = response.css('td#mainContent_cellOwnerInformation::text').extract()[0].replace('<br>', ', ').replace('\n', '').strip()
        except:
            d['owner_and_address'] = ''
        try:
            d['date'] = response.css("//strong[.='Date:']/parent::td::text").extract()
            # .split('strong>')[-1].replace('<br>', ', ').replace('\n', '').strip()
        except:
            d['date'] = ''
        try:
            d['tax_year'] = response.css("td#mainContent_cellTaxYearInfo u::text").extract()[0].replace('\n', '').strip()
        except:
            d['tax_year'] = ''
        try:
            d['legal_description'] = response.xpath( "//b[.='Legal Description:']/parent::td/following-sibling::td/b/text()").extract()[0].replace('\n', '').strip()
        except:
            d['legal_description'] = ''
        try:
            d['total_assessed_value'] = response.css("td#mainContent_cellAssessedValue::text").extract()[0].replace('\n', '').strip()
        except:
            d['total_assessed_value'] = ''
        try:
            d['taxable_value'] = response.css("td#mainContent_cellTaxableValue::text").extract()[0].replace('\n', '').strip()
        except:
            d['taxable_value'] = ''
        try:
            d['location_address'] = response.xpath("//b[.='Location Address:']/parent::td/following-sibling::td/text()").extract()[0].replace('\n', '').strip()
        except:
            d['location_address'] = ''
        try:
            d['gross_tax_amount'] = response.css("td#mainContent_cellGrossTaxAmount::text").extract()[0].replace('\n', '').strip()
        except:
            d['gross_tax_amount'] = ''
        try:
            d['millage_code'] = response.css("td#mainContent_cellMillageCode::text").extract()[0].replace('\n', '').strip()
        except:
            d['millage_code'] = ''
        return d

    def parse_table_1(self,response):
        rows = response.css('table#mainContent_tableUnpaidTaxes tr')
        # try:
        heads = rows[0].css('strong')[:-2]
        # except:
        #     return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            h = h.xpath('./text()').extract()[0].replace(' ','_').lower().strip()
            headlist.append(h)
            headdict[h] = ''
        for row in rows[1:]:
            tds = row.css('td')[:-2]
            count = 0
            temp = deepcopy(headdict)
            for td in tds:
                try:
                    value = td.xpath('./a/@href').extract()[0]
                except:
                    try:
                        value='http://pt.octaxcol.com/PropertyTax/'+td.xpath('./center/a/@href').extract()[0]
                    except:
                        try:
                            value = td.css('span::text').extract()[0]
                        except:
                            try:
                                value = td.css('center::text').extract()[0]
                            except:
                                try:
                                    value = td.xpath('./text()').extract()[0]
                                except:
                                    value=''
                temp[headlist[count]] = value
                count += 1
            data.append(temp)
        return data

    def parse_table_2(self,response,selector,):
        rows = response.css(selector)
        try:
            heads = rows[0].css('strong')[:-1]
        except:
            return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            h = h.xpath('./text()').extract()[0].replace(' ','_').lower().strip()
            headlist.append(h)
            headdict[h] = ''
        for row in rows[1:]:
            tds = row.css('td')[:-1]
            count = 0
            temp = deepcopy(headdict)
            for td in tds:
                try:
                    value = td.css('center::text').extract()[0]
                except:
                    try:
                        value = td.css('span::text').extract()[0]
                    except:
                        value = td.xpath('./text()').extract()[0]
                temp[headlist[count]] = value
                count += 1
            data.append(temp)
        return data

    def parse_table_3(self,response, selector):
        rows =response.css(selector)
        try:
            heads = rows[0].css('strong')
        except:
            return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            h = h.xpath('./text()').extract()[0]
            headlist.append(h)
            headdict[h] = ''
        for row in rows[1:]:
            tds = row.css('td')
            count = 0
            temp = deepcopy(headdict)
            for td in tds:
                try:
                    value = td.css('center::text').get_attribute('innerHTML').extract()[0]
                except:
                    try:
                        value = td.css('span::text').get_attribute('innerHTML').extract()[0]
                    except:
                        value = td.xpath("./text()").extract()[0]
                temp[headlist[count]] = value
                count += 1
            data.append(temp)
        return data











