import scrapy
from scrapy import Request
from scrapy.utils.response import open_in_browser
from copy import deepcopy


class PolkSpiderSpider(scrapy.Spider):
    name = 'polk'

    def start_requests(self):
        input_file = open('polk.txt', 'r', encoding='utf-8')
        search_link = 'https://www.polkpa.org/CamaDisplay.aspx?OutputMode=Display&SearchType=RealEstate&ParcelID='
        lines = input_file.readlines()
        c = 0
        for line in lines:
            try:
                line = str(line).replace('\n', '')
                c += 1
                print(c," Going for ------",line,'-------------------------')
                link = search_link + line
                yield Request(url=link, callback=self.parse, meta={'proxy': 'http://95.211.175.167:13150'})
            except:
                continue
            # break

    def parse(self, response):
        d = {
            'folio': response.request.url.split("&ParcelID=")[-1],
            'url': response.request.url
        }
        d['owner_info'] = self.parse_owner_info(response)
        d['legal_description'] = self.prase_legal_description(response)
        d['parcel_summary'] = self.parse_parcel_summary(response)
        d['sales_history'] = self.parse_sales_summary(response)
        d['exemptions'] = self.parse_exemptions(response)
        d['buildings'] = self.parse_buildings(response)
        d['extra_features'] = self.parse_extra_feautres(response)
        d['land_lines'] = self.parse_land_lines(response)
        d['value_summary_2020'] = self.parse_value_summary(response)
        d['value_by_district'] = self.parse_value_by_district(response)
        d['ad_non_valorem'] = self.parse_non_valorem(response)
        d['taxes'] = self.parse_taxes(response)
        d['parse_prior_year_final_values'] = self.parse_prior_year_final_values(response)
        d['map_link'] = self.parse_map_link(response)
        d['trim_pdf'] = self.parse_trim_pdf(response)
        d['proxy'] = 'http://95.211.175.167:13150'
        # trim pdf is not downloadable, requrie recpatcha
        d['tax_link'] = 'http://fl-polk-taxcollector.governmax.com/collectmax/site_authlink.asp?r=www.polkpa.org&g=' + \
                        d['folio']
        yield Request(d['tax_link'], meta=d, callback=self.parse_tax_collector_navigator)

    def prase_legal_description(self, response):
        temp = response.xpath("//b[text()='DISCLAIMER: ']/parent::div/parent::div/text()").extract()
        tt = ''
        for t in temp:
            tt += t.replace("\r", '').replace("\n", '').strip()
        return tt

    def parse_owner_info(self, response):
        d = {}
        names = response.xpath("//h4[text()='Owners']/parent::td/table[1]//tr")
        d['names'] = []
        for ad in names:
            d['names'].append(ad.css('td::text').extract()[0])
        address = response.xpath("//h4[text()='Mailing Address']/following-sibling::table[1]//tr")
        d['addresses'] = {}
        for ad in address:
            tds = ad.css('td')
            key = tds[0].xpath('./text()').extract()[0]
            try:
                value = tds[1].xpath('./span/text()').extract()[0]
                d['addresses'][key] = value
            except:
                pass
        d['site_addresses'] = {}
        sts = response.xpath("//h4[text()='Site Address']/following-sibling::table[1]//tr")
        for ad in sts:
            tds = ad.css('td')
            key = tds[0].xpath('./text()').extract()[0]
            try:
                value = tds[1].xpath('./span/text()').extract()[0]
                d['site_addresses'][key] = value
            except:
                pass
        return d

    def parse_parcel_summary(self, response):
        d = {}
        address = response.xpath("//h4[text()='Parcel Information']/following-sibling::table[1]//tr")
        for ad in address:
            tds = ad.css('td')
            key = tds[0].xpath('./text() | ./a/text()').extract()[0]
            try:
                value = tds[1].xpath('./span/text()').extract()[0]
                d[key] = value.replace('&nbsp;', '').replace("\xa0", '')
            except:
                pass
        return d

    def parse_sales_summary(self, response):
        rows = response.xpath("//td[text()='OR Book/Page']/parent::tr/parent::table/tr")
        return self.parse_table_1(rows)

    def parse_exemptions(self, response):
        rows = response.xpath("//td[text()='Renew Cd']/parent::tr/parent::table/tr")
        return self.parse_table_1(rows[:-1])

    def parse_extra_feautres(self, response):
        rows = response.xpath("//td[text()='Year Built']/parent::tr/parent::table/tr")
        return self.parse_table_1(rows)

    def parse_value_by_district(self, response):
        rows = response.xpath("//td[text()='District Description']/parent::tr/parent::table/tr")
        return self.parse_table_1(rows[:-1])

    def parse_land_lines(self, response):
        rows = response.xpath("//td[text()='Land Dscr']/parent::tr/parent::table/tr")
        return self.parse_table_1(rows)

    def parse_non_valorem(self, response):
        rows = response.xpath("//td[text()='Assessment']/parent::tr/parent::table/tr")
        return self.parse_table_1(rows[:-1])

    def parse_taxes(self, response):
        rows = response.xpath("//td[text()='Last Year']/parent::tr/parent::table/tr")
        return self.parse_table_1(rows[:-2])

    def parse_value_summary(self, response):
        rows = response.xpath(
            "//h3[contains(text(),'Value Summary (2020)')]/following-sibling::table//td[text()='Value']/parent::tr/parent::table/tr")
        return self.parse_table_1(rows)

    def parse_prior_year_final_values(self, response):
        tables = response.xpath(
            "//h3[text()='Prior Year Final Values']/parent::div/table")
        data = {}
        for table in tables:
            head = table.css('tr.header').css('td::text').extract()[0]
            rows = table.xpath('./tr')[1:]
            data[head] = self.parse_table_2(rows)
        return data

    def parse_map_link(self, response):
        try:
            return response.xpath("//a[text()='Open Interactive Map']/@href").extract()[0]
        except:
            return ''

    def parse_trim_pdf(self, response):
        try:
            tt = response.xpath(
                "//a[contains(@onclick,'TrimNotice') and contains(@onclick,'pdf=true')]/@onclick").extract()[0]
            tt = tt.split("window.open('")[-1].split("', '")[0]
            return 'https://www.polkpa.org/' + tt
        except:
            return ''

    def parse_table_2(self, rows):
        d = {}
        for row in rows:
            tds = row.css("td::text").extract()
            key = tds[0]
            value = tds[1]
            d[key] = value
        return d

    def parse_table_1(self, rows):
        try:
            heads = rows[0].css('td')
        except:
            return []
        data = []
        headlist = []
        headdict = {}
        for h in heads:
            h = h.xpath('./text()').extract()
            h = ''.join(h).replace('\r', '').replace('\n', '').strip()
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
                value = td.xpath('./text() | .//a/text() | .//span/text()').extract()
                value = ''.join(value).replace('\r', '').replace('\n', '').strip()
                temp[headlist[count]] = value
                count += 1
            data.append(temp)
        return data

    def parse_buildings(self, response):
        buildings = response.xpath("//h3[contains(text(),'Buildings')]/parent::div/table")
        data = []
        for build in buildings:
            d = {}
            try:
                d['BAS Note'] = build.xpath(".//strong[text()='Building BAS Note:']/parent::div/text()").extract()[
                    1].replace('\u00a0', '').strip()
            except:
                d['BAS Note'] = ''
            rows = build.xpath(".//td[text()='Element']/parent::tr/parent::table/tr")
            d['elements'] = self.parse_table_1(rows)
            rows = build.xpath(".//td[text()='Code']/parent::tr/parent::table/tr")
            try:
                d['Sub Area Note'] = \
                    build.xpath(".//strong[text()='Building Sub Area Note:']/parent::div/text()").extract()[
                        1].replace('\u00a0', '').strip()
            except:
                d['Sub Area Note'] = ''
            d['sub_area'] = self.parse_table_1(rows)
            try:
                d['sketch'] = response.xpath('.//img[@alt="Building Traverse"]/@src').extract()[0]
            except:
                d['sketch'] = ''
            data.append(d)
        return data

    def parse_tax_collector_navigator(self, response):
        sid = response.request.url.split('sid=')[1]
        folio = response.meta['folio']
        link = 'http://fl-polk-taxcollector.governmax.com/collectmax/search_collect.asp?reset=True&body=search_collect.asp&geo_number={folio}&go.x=1&l_nm=geo_number&sid={sid}&agencyid=FLPolkTC'
        link = link.format(folio=folio, sid=sid)
        yield Request(link, meta=response.meta, callback=self.parse_tax_collector_navigator2)

    def parse_tax_collector_navigator2(self, response):
        link = response.request.url + '&wait=done'
        yield Request(link, meta=response.meta, callback=self.parse_tax_collector)

    def parse_tax_collector(self, response):
        d = {}
        rows = response.xpath("//b[contains(.,'Tax Type')]/parent::font/parent::td/parent::tr/parent::table/tr")
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
            del response.meta['proxy']
        except:
            pass
        yield {'data': response.meta}
