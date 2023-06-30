import scrapy
from scrapy import Request
from copy import deepcopy


class SeminoleSpiderSpider(scrapy.Spider):
    name = 'semi'

    def start_requests(self):
        input_file = open('seminole.txt', 'r', encoding='utf-8')
        search_link = 'https://parceldetails.scpafl.org/ParcelDetailInfo.aspx?PID='
        lines = input_file.readlines()
        c = 0
        for line in lines:
            try:
                line = str(line).replace('\n', '')
                c += 1
                print(c,' Going for ---',line,'------')
                link = search_link + line
                yield Request(url=link, callback=self.parse)
            except:
                continue

    def parse(self, response):
        d = {'folio': response.request.url.split('PID=')[-1], 'url': response.request.url}
        d['parcel_info'] = self.parse_parcel_info(response)
        d['value_summary'] = self.parse_value_summary(response)
        d['legal_description'] = self.parse_value_legal_description(response)
        d['land'] = self.parse_land(response)
        d['building_info'] = self.parse_building_info(response)
        d['permits'] = self.parse_permits(response)
        d['extra_features'] = self.parse_extra_features(response)
        d['taxes'] = self.parse_taxes(response)
        d['sales'] = self.parse_sales(response)
        d['zoning'] = self.parse_zoning(response)
        d['map_link'] = 'https://maps2.scpafl.org/SCPAExternal/?query=PARCELS;PARCEL;' + d['folio']
        try:
            d['comparable_sales_pdf'] = response.xpath("//a[text()=' View Comparable Sales']/@href").extract()[0]
        except:
            d['comparable_sales_pdf'] = ''
        d['tax_collector_link'] = 'https://payments.seminolecounty.tax/_asp/payresult.asp?txtAccountID=' + d['folio']
        yield Request(d['tax_collector_link'], meta=d, callback=self.parse_tax_collector)
        # yield {'data': d}

    def parse_parcel_info(self, response):
        rows = response.css("table#ParcelInfo tr")
        d = {}
        for row in rows:
            key = row.css('th').xpath("./text() | .//span/text()").extract()
            key = ''.join(key).replace('\n', '').replace('\r', '').strip()
            value = row.css('td').css("span::text , a::text").extract()
            value = ''.join(value).replace('\n', '').replace('\r', '').strip()
            d[key] = value
        return d

    def parse_value_summary(self, response):
        d = {}
        try:
            d['Tax Amount without SOH'] = response.xpath(
                "//div[contains(text(),'Tax Amount without SOH:')]/following-sibling::div/span/text()").extract()[0]
        except:
            d['Tax Amount without SOH'] = ''
        try:
            d['2020 Tax Bill Amount'] = response.xpath(
                "//*[contains(text(),'2020 Tax Bill Amount')]/parent::div/following-sibling::div/span/text()").extract()[
                0]
        except:
            d['2020 Tax Bill Amount'] = ''
        try:
            d['Save Our Homes Savings'] = response.xpath(
                "//div[contains(text(),'Save Our Homes Savings:')]/following-sibling::div/span/text()").extract()[0]
        except:
            d['Save Our Homes Savings'] = ''
        rows = response.xpath(
            "//table[@id='ctl00_Content_PageControl1_gridValue_DXMainTable']/tr[not(contains(@style,'none;'))]")
        d['table'] = self.parse_table_1(rows)
        return d

    def parse_value_legal_description(self, response):
        tt = response.xpath("//span[@id='ctl00_Content_PageControl1_txtLegalInfo']/text()").extract()
        t = ''.join(tt)
        return t

    def parse_land(self, response):
        rows = response.xpath(
            "//table[@id='ctl00_Content_PageControl1_gridLand_DXMainTable']/tr[not(contains(@style,'none;'))]")
        return self.parse_table_1(rows)

    def parse_building_info(self, response):
        rows = response.xpath(
            "//table[@id='ctl00_Content_PageControl1_grdResBldg_DXMainTable']/tr[not(contains(@style,'none;'))]")
        return self.parse_table_1(rows)

    def parse_permits(self, response):
        rows = response.xpath(
            "//table[@id='ctl00_Content_PageControl1_grdPermits_DXMainTable']/tr[not(contains(@style,'none;'))]")
        return self.parse_table_1(rows)

    def parse_extra_features(self, response):
        rows = response.xpath(
            "//table[@id='ctl00_Content_PageControl1_grdExft_DXMainTable']/tr[not(contains(@style,'none;'))]")
        return self.parse_table_1(rows)

    def parse_taxes(self, response):
        rows = response.xpath(
            "//table[@id='ctl00_Content_PageControl1_grdTaxes_DXMainTable']/tr[not(contains(@style,'none;'))]")
        return self.parse_table_1(rows)

    def parse_sales(self, response):
        rows = response.xpath(
            "//table[@id='ctl00_Content_PageControl1_grdSales_DXMainTable']/tr[not(contains(@style,'none;'))]")
        return self.parse_table_1(rows)

    def parse_zoning(self, response):
        rows = response.xpath(
            "//table[@id='ctl00_Content_PageControl1_ASPxGridView1_DXMainTable']/tr[not(contains(@style,'none;'))]")
        return self.parse_table_1(rows)

    def parse_table_1(self, rows):
        try:
            heads = rows[0].xpath('./td')[:-1]
        except:
            return []
        data = []
        headlist = []
        headdict = {}
        for h in heads:
            h = h.css('tr td::text').extract()[0].strip()
            if h == '':
                h = 'name'
            headdict[h] = ''
            headlist.append(h)
        rows = rows[1:]
        for row in rows:
            tds = row.xpath('./td')[:-1]
            if len(tds) != len(headlist):
                continue
            count = 0
            temp = deepcopy(headdict)
            for td in tds:
                try:
                    rows2 = td.css('table table')[0].xpath('./tr')
                    value = self.parse_table_2(rows2)
                except:
                    try:
                        value = td.xpath('./span/text() | ./a/text()').extract()[0].replace("\u00a0", '').strip()
                    except:
                        value = td.xpath('./text()').extract()[0].replace("\u00a0", '').strip()
                temp[headlist[count]] = value
                count += 1
            data.append(temp)
        return data

    def parse_table_2(self, rows):
        try:
            heads = rows[0].xpath('./td')
        except:
            return []
        data = []
        headlist = []
        headdict = {}
        for h in heads:
            h = h.css('tr td::text').extract()[0].strip()
            if h == '':
                h = 'name'
            headdict[h] = ''
            headlist.append(h)
        rows = rows[1:]
        for row in rows:
            tds = row.xpath('./td')
            if len(tds) != len(headlist):
                continue
            count = 0
            temp = deepcopy(headdict)
            for td in tds:
                try:
                    value = td.xpath('./span/text() | ./a/text()').extract()[0].replace("\u00a0", '').strip()
                except:
                    value = td.xpath('./text()').extract()[0].replace("\u00a0", '').strip()
                temp[headlist[count]] = value
                count += 1
            data.append(temp)
        return data

    def parse_tax_collector(self, response):
        rows = response.xpath("//tr[@id='Parcel']/parent::table/tr")
        d = {'summary': {}}
        for row in rows:
            tds = row.css("td")
            if len(tds) > 1:
                key = tds[0].xpath("./p/text() | ./text()").extract()[0].replace(':', '').strip()
                kk = tds[1].xpath("./p/text() | .//p/strong/text() | ./p/strong/a/text()").extract()
                value = ''
                for k in kk:
                    value += ', ' + k.replace('\n', '').replace('\r', '').strip()
                    value = value.strip(',').strip()
                d['summary'][key] = value.strip(',').strip()
        try:
            tt = response.xpath("//h2[text()='Tax Status:']/following-sibling::h4")[0].xpath('./text()').extract()[
                0].strip()
            d['Status'] = {'Tax Status': tt}
            kk = response.xpath("//p[text()='Date']/parent::td/parent::tr/following-sibling::tr[1]/td")
            d['Status']['date'] = kk[0].xpath('./h3/text()').extract()[0].strip()
            d['Status']['receipt'] = kk[1].xpath('./p/text()').extract()[0].strip()
            d['Status']['amount_paid'] = kk[2].xpath('./h3/text()').extract()[0].strip()
        except:
            pass
        try:
            d['Prior Years Unpaid Delinquent Taxes'] = \
                response.xpath("//h4[contains(text(),'Prior Years Unpaid Delinquent Taxes')]/text()").extract()[
                    0].split(
                    ':')[-1].strip()
        except:
            d['Prior Years Unpaid Delinquent Taxes'] = ''
        response.meta['tax_collector'] = d
        try:
            del response.meta['depth']
            del response.meta['download_timeout']
            del response.meta['download_slot']
            del response.meta['download_latency']
            del response.meta['redirect_times']
            del response.meta['redirect_ttl']
            del response.meta['redirect_urls']
            del response.meta['redirect_reasons']
        except:
            pass
        yield {'data': response.meta}
