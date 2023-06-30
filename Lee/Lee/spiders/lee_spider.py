import scrapy
from scrapy import Request
from scrapy.http import FormRequest


class LeeSpiderSpider(scrapy.Spider):
    name = 'lee'
    base_url1 = 'https://www.leepa.org'

    def start_requests(self):
        input_file = open('ids.txt', 'r', encoding='utf-8')
        lines = input_file.readlines()
        c = 0
        for s in lines:
            s = s.replace('\n', '').strip()
            id = s[0:2] + '-' + s[2:4] + '-' + s[4:6] + '-' + s[6:8] + '-' + s[8:13] + '.' + s[13:]
            print(c,'Going For ',id,'------------------')
            d = {'parcel_id': id, 'parcel_id_': s}
            d['folio']=id
            link = 'https://www.leepa.org/Search/PropertySearch.aspx'
            c += 1
            yield Request(url=link, meta={'data': d, 'proxy': 'http://95.211.175.167:13150'}, callback=self.search, dont_filter=True)


    def search(self, response):
        d = response.meta['data']
        formData = {
            'ctl00$BodyContentPlaceHolder$PropertySearchUScriptManager': 'ctl00$BodyContentPlaceHolder$PropertySearchUpdatePanel | ctl00$BodyContentPlaceHolder$SubmitPropertySearch',
            '__VIEWSTATE': response.css("input#__VIEWSTATE::attr(value)").extract()[0],
            '__VIEWSTATEGENERATOR': response.css("input#__VIEWSTATEGENERATOR::attr(value)").extract()[0],
            'ctl00$BodyContentPlaceHolder$STRAPTextBox': d['parcel_id'],
            'ctl00$BodyContentPlaceHolder$SearchSouceGroup': 'SiteRadioButton',
            'ctl00$BodyContentPlaceHolder$CountryDropDownList': 'UNITED STATES OF AMERICA',
            'ctl00$BodyContentPlaceHolder$hdnViewPort': 'false',
            '__ASYNCPOST': 'true',
            'ctl00$BodyContentPlaceHolder$SubmitPropertySearch': 'Search'
        }
        link = 'https://www.leepa.org/Search/PropertySearch.aspx'
        yield FormRequest(url=link, formdata=formData, meta={'data': d, 'proxy': 'http://95.211.175.167:13150'}, method='POST', callback=self.getResult)

    def getResult(self, response):
        d = response.meta['data']
        try:
            link = self.base_url1 + response.xpath("//a[contains(text(),'Parcel Details')]/@href").extract()[0]
        except:
            return
        d['folio'] = link.split('FolioID=')[-1].split('&')[0]
        d['url'] = link
        link = 'https://www.leepa.org/Display/DisplayParcel.aspx?FolioID={folio}&ViewMobile=false&ExemptDetails=True&PermitDetails=True&TaxRollDetails=True&SalesDetails=True&AuthDetails=True&RenumberDetails=True&GarbageDetails=True&ElevationDetails=True&AddressHistoryDetails=True&AppraisalDetails=True&AppraisalDetailsCurrent=True'
        link = link.format(folio=d['folio'])
        yield Request(url=link, meta={'data': d, 'proxy': 'http://95.211.175.167:13150'}, callback=self.parse_main_page)

    def parse_main_page(self, response):
        d = response.meta['data']
        d['property_info'] = self.parse_property_info(response)
        rows = response.xpath("//div[@id='Exemptions']//table//tr")
        try:
            d['exemptions'] = self.parse_table_headers_wala(rows)
        except:
            d['exemptions'] = []
        tables = response.xpath("//table[@id='taxRollTable']//table")
        l = []
        for table in tables:
            rows = table.css('tr')
            l.append(self.parse_table_2_columns(rows))
        d['values'] = l
        rows = response.xpath("//div[@id='TaxAuthority']//table//tr")
        d['tax_authorities'] = self.parse_table_headers_wala(rows)
        rows = response.xpath("//div[@id='SalesDetails']//table//tr")
        d['sales'] = self.parse_table_headers_wala(rows)
        rows = response.xpath("//div[@id='PermitDetails']//table//tr")
        d['PermitDetails'] = self.parse_table_headers_wala(rows)
        rows = response.xpath("//div[@id='NumberingDetails']//table//tr")
        d['parcel_numbring_history'] = self.parse_table_headers_wala(rows)
        rows = response.xpath("//div[@id='GarbageDetails']//table//tr")
        d['solid_waste'] = self.parse_table_headers_wala(rows)
        rows = response.xpath("//div[@id='ElevationDetails']//table//tr")[1:]
        ex = ['Evacuation Zone']
        d['flood_zone'] = self.parse_table_headers_wala_special(rows, ex)
        rows = response.xpath("//div[@id='AddressHistoryDetails']//table//tr")
        d['address_history'] = self.parse_table_headers_wala(rows)

        boxes = response.xpath("//div[@id='AppraisalDetails']//div[@class='box']")
        k = {}
        for box in boxes:
            h = box.xpath("./div[@class='sectionSubTitle']/text()").extract()[0]
            if h == 'Land':
                k[h] = self.parse_table_headers_wala_for_land(box)
            if h == 'Buildings':
                k[h] = self.parse_table_buildings(box)
        d['appraisal_details'] = k

        boxes = response.xpath("//div[@id='AppraisalDetailsCurrent']//div[@class='box']")
        k = {}
        for box in boxes:
            h = box.xpath("./div[@class='sectionSubTitle']/text()").extract()[0]
            if h == 'Land':
                k[h] = self.parse_table_headers_wala_for_land(box)
            if h == 'Buildings':
                k[h] = self.parse_table_buildings(box)
        d['appraisal_details_current'] = k
        d['trim_pdfs'] = self.parse_trim_pdfs(response, d['folio'])
        link = 'https://www.leepa.org/Display/DisplayParcel.aspx?FolioID={folio}&ViewMobile=false&LocationDetails=True#LocationDetails'
        link = link.format(folio=d['folio'])
        yield Request(link, meta={'data': d, 'proxy': 'http://95.211.175.167:13150'}, callback=self.parse_location_information)
        # yield {'data': d}

    def parse_trim_pdfs(self, response, folio):
        years = response.xpath("//a[@class='availableTaxYears']/text()").extract()
        l = []
        for year in years:
            l.append({year: 'https://www.leepa.org/Display/DisplayTrim.aspx?FolioID={folio}&TaxYear={year}'.format(
                folio=folio, year=year)})
        return l

    def parse_property_info(self, response):
        d = {}
        t = response.xpath(
            "//img[@title='Show Complete Ownership Information']/parent::div/following-sibling::div/div/text()").extract()
        d['owner'] = ''
        for i in t:
            d['owner'] += ", " + i.replace('\r', '').replace("\n", '').strip()
        d['owner'] = d['owner'].strip(', ')
        t = response.xpath(
            "//div[contains(text(),'Site Address')]/following-sibling::div/text()").extract()
        d['site_address'] = ''
        for i in t:
            d['site_address'] += ", " + i.replace('\r', '').replace("\n", '').strip()
        d['site_address'] = d['site_address'].strip(', ')

        t = response.xpath(
            "//div[contains(text(),'Property Description')]/following-sibling::div/text()").extract()
        d['legal_description'] = ''
        for i in t:
            d['legal_description'] += ", " + i.replace('\r', '').replace("\n", '').strip()
        d['legal_description'] = d['legal_description'].strip(', ')

        t = response.xpath(
            "//div[contains(text(),'Classification')]/following-sibling::div/text()").extract()
        d['classification'] = ''
        for i in t:
            d['classification'] += ", " + i.replace('\r', '').replace("\n", '').strip()
        d['classification'] = d['classification'].strip(', ')
        try:
            d['map_image'] = \
                response.xpath("//div[@id='divDisplayParcelTaxMap']//img[contains(@src,'TaxMap')]/@src").extract()[0]
        except:
            d['map_image'] = ''
        rows = response.xpath("//div[@id='divDisplayParcelAttributes']//tr")
        d['attributes'] = self.parse_table_2_columns(rows)
        try:
            d['tax_map_viewer'] = response.xpath("//a[contains(text(),'Tax Map Viewer')]/@href").extract()[0]
        except:
            d['tax_map_viewer'] = ''
        try:
            d['aerial_viewer'] = response.xpath("//a[contains(text(),'Aerial Viewer')]/@href").extract()[0]
        except:
            d['aerial_viewer'] = ''
        try:
            tt = response.xpath("//div[@id='divDisplayParcelPhoto']//img[contains(@src,'dotnet')]/@src").extract()
            d['images'] = [self.base_url1 + t for t in tt]
        except:
            d['images'] = ''
        return d

    def parse_location_information(self, response):
        d = response.meta['data']
        tables = response.xpath("//div[@id='LocationDetails']//table")
        d['location_information'] = self.parse_table_2_columns_special(tables[0])
        try:
            d['location_information']['google_map'] = \
                tables[1].xpath(".//a[contains(text(),'View Parcel on Google Maps')]/@href").extract()[0]
        except:
            d['location_information']['google_map'] = ''
        try:
            d['location_information']['lee_clerk'] = \
                tables[1].xpath(".//a[contains(text(),'View Recorded Plat at LeeClerk.org')]/@href").extract()[0]
        except:
            d['location_information']['lee_clerk'] = ''
        try:
            d['location_information']['geo_view'] = \
                tables[1].xpath(".//a[contains(text(),'View Parcel on GeoView')]/@href").extract()[0]
        except:
            d['location_information']['geo_view'] = ''
        # https://www.leetc.com/ncp/search_detail.asp?SearchType=RP&TaxYear=2020&Account=11432002000000270&Option=DETAIL
        yield Request(
            url='https://www.leetc.com/ncp/search_detail.asp?SearchType=RP&TaxYear=2020&Account={parcel}&Option=DETAIL'.format(
                parcel=d['parcel_id_']),
            callback=self.parse_tax_collector, meta={'data': d, 'proxy': 'http://95.211.175.167:13150'})

    def parse_table_2_columns(self, rows):
        d = {}
        for row in rows:
            try:
                key = row.css('th::text').extract()[0].replace('\r\n', '').strip()
                value = row.css('td::text').extract()[0].replace('\n', '').strip()
                d[key] = value
            except:
                continue
        return d

    def parse_table_2_columns_special(self, table):
        keys = table.css('th')
        values = table.css('td')
        d = {}
        for k in range(len(keys)):
            try:
                kk = keys[k].xpath('./text()').extract()[0]
            except:
                continue
            try:
                vv = values[k].xpath('./text()').extract()[0]
            except:
                vv = ''
            d[kk] = vv
        return d

    def parse_table_headers_wala(self, rows):
        try:
            heads = rows[0].css('th')
        except:
            return []
        headlist = []
        data = []
        for h in heads:
            h = h.xpath('./text()').extract()[0].replace("\r\n", '').strip()
            headlist.append(h)
        rows = rows[1:]
        for row in rows:
            tds = row.css('td')
            count = 0
            d = {}
            for td in tds:
                try:
                    v = td.xpath("./pre/text()").extract()
                    l = v[0]
                except:
                    v = td.xpath("./text() | .//strong/text() | .//a/text() | .//div/text()").extract()
                value = ''
                for i in v:
                    value += ' ' + i.replace('\r\n', '').strip()
                value = value.strip()
                d[headlist[count]] = value
                count += 1
            data.append(d)
        return data

    def parse_table_headers_wala_for_land(self, box):
        tables = box.css('table.appraisalAttributes')
        dd = {}
        for table in tables:
            rows = table.css('tr')
            name = rows[0].css('th.subheader::text').extract()[0]
            try:
                heads = rows[1].css('th')
            except:
                return []
            headlist = []
            data = []
            for h in heads:
                h = h.xpath('./text()').extract()[0].replace("\r\n", '').strip()
                headlist.append(h)
            rows = rows[2:]
            for row in rows:
                tds = row.css('td')
                count = 0
                d = {}
                for td in tds:
                    try:
                        v = td.xpath("./pre/text()").extract()
                        l = v[0]
                    except:
                        v = td.xpath("./text() | .//strong/text() | .//a/text() | .//div/text()").extract()
                    value = ''
                    for i in v:
                        value += ' ' + i.replace('\r\n', '').strip()
                    value = value.strip()
                    d[headlist[count]] = value
                    count += 1
                data.append(d)
            dd[name] = data
        return dd

    def parse_table_buildings(self, box):
        rows = box.css('table.appraisalAttributes tr')
        subh = ''
        headlist = []
        data = {}
        ind = -1
        complete = []
        for row in rows:
            try:
                subh = row.css('th.subheader::text').extract()[0]
                if 'Building' in subh and 'of' in subh:
                    if len(data.keys()) > 0:
                        complete.append(data)
                    data = {}
                    pass
                else:
                    data[subh] = []
                    headlist = []
                continue
            except:
                pass
            try:
                heads = row.css('th')
                h = heads[0]
                headlist = []
                for h in heads:
                    h = h.xpath('./text()').extract()[0].replace("\r\n", '').strip()
                    headlist.append(h)
                continue
            except:
                pass
            tds = row.css('td')
            count = 0
            d = {}
            for td in tds:
                try:
                    value = td.xpath(".//a/@href").extract()
                    l = value[0]
                    for idx, v in enumerate(value):
                        value[idx] = self.base_url1 + v
                except:
                    v = td.xpath("./text() | .//strong/text() | .//a/text() | .//div/text()").extract()
                    value = ''
                    for i in v:
                        value += ' ' + i.replace('\r\n', '').strip()
                    value = value.strip()
                d[headlist[count]] = value
                count += 1
            data[subh].append(d)
        complete.append(data)
        return complete

    def parse_table_headers_wala_special(self, rows, exheads):
        try:
            heads = rows[0].css('th')
        except:
            return []
        headlist = []
        data = []
        for h in heads:
            h = h.xpath('./text()').extract()[0].replace("\r\n", '').strip()
            headlist.append(h)
        headlist = headlist + exheads
        rows = rows[1:]
        for row in rows:
            tds = row.css('td')
            count = 0
            d = {}
            for td in tds:
                try:
                    v = td.xpath("./pre/text()").extract()
                    l = v[0]
                except:
                    v = td.xpath("./text() | .//strong/text() | .//a/text() | .//div/text()").extract()
                value = ''
                for i in v:
                    value += ' ' + i.replace('\r\n', '').strip()
                value = value.strip()
                d[headlist[count]] = value
                count += 1
            data.append(d)
        return data

    def parse_tax_collector(self, response):
        d = response.meta['data']
        dd = {}
        tds = response.xpath("//table[@class='sTable sBorder'][1]//tr[3]//td")
        try:
            dd['account'] = tds[0].xpath(".//a/text()").extract()[0]
        except:
            dd['account'] = ''
        try:
            dd['tax_year'] = tds[1].xpath("./span/text()").extract()[0]
        except:
            dd['tax_year'] = ''
        try:
            dd['status'] = tds[2].xpath(".//a/text()").extract()[0]
        except:
            dd['status'] = ''
        tds = response.xpath("//table[@class='sTable sBorder'][1]//tr[5]//td")
        try:
            dd['orignal_account'] = tds[0].xpath("./span/text()").extract()[0]
        except:
            dd['orignal_account'] = ''
        try:
            dd['instrument_number'] = tds[1].xpath("./span/text()").extract()[0]
        except:
            dd['instrument_number'] = ''

        try:
            dd['owner'] = response.xpath("//table[@class='sTable sBorder'][1]//tr[7]//td//span/text()").extract()[0]
        except:
            dd['owner'] = ''
        tds = response.xpath("//table[@class='sTable sBorder'][1]//tr[9]//td")
        try:
            tt = tds[0].xpath("./span/text()").extract()
            tt = ' '.join(tt)
            dd['physical_address'] = tt
        except:
            dd['physical_address'] = ''
        try:
            tt = tds[1].xpath("./span/text()").extract()
            tt = ' '.join(tt)
            dd['mailing_address'] = tt
        except:
            dd['mailing_address'] = ''
        tds = response.xpath("//table[@class='sTable sBorder'][1]//tr[11]//td")
        try:
            tt = tds[0].xpath("./span/text()").extract()
            tt = ' '.join(tt)
            dd['legal_description'] = tt
        except:
            dd['legal_description'] = ''
        rows = response.xpath("//span[.='Values & Exemptions']/parent::td/parent::tr/parent::table//tr")[1:]
        dd['values_and_exemptions'] = self.tax_collector_2columns_table(rows)
        try:
            table = response.xpath("//table[@id='stacktable']")[0]
            rows = table.css('tr')
            dd['Ad Valorem Taxes'] = self.parse_tax_collector_table_headers_wala(rows)
        except:
            dd['Ad Valorem Taxes'] = []
        try:
            table = response.xpath("//table[@id='stacktable']")[1]
            rows = table.css('tr')
            dd['Non-Ad Valorem Taxes'] = self.parse_tax_collector_table_headers_wala(rows)
        except:
            dd['Non-Ad Valorem Taxes'] = []
        tds = response.css("td.sAmountFooter")
        dd['amount_due_if_paid_in'] = self.parse_amount_due(tds)
        # td.sAmountFooter
        d['tax_collector'] = dd
        yield {'data': d}

    def tax_collector_2columns_table(self, rows):
        d = {}
        for row in rows:
            tds = row.css('td')
            key = tds[0].css('span::text,span a::text').extract()[0].replace('\u00a0', '').strip()
            value = tds[1].css('span::text,span a::text').extract()[0].replace('\u00a0', '').strip()
            d[key] = value
        return d

    def parse_tax_collector_table_headers_wala(self, rows):

        try:
            heads = rows[0].css('td span')
        except:
            return []
        headlist = []
        data = []
        for h in heads:
            h = h.xpath('./text() | ./strong/text()').extract()[0].replace("\r\n", '').strip()
            headlist.append(h)
        rows = rows[1:]
        for row in rows:
            tds = row.css('td')
            count = 0
            d = {}
            for td in tds:
                try:
                    v = td.xpath("./span/text() | ./span/a/text()").extract()
                    l = v[0]
                except:
                    v = td.xpath("./text() | .//strong/text() | .//a/text() | .//div/text()").extract()
                value = ''
                for i in v:
                    value += ' ' + i.replace('\r\n', '').strip()
                value = value.strip()
                d[headlist[count]] = value
                count += 1
            data.append(d)
        return data

    def parse_amount_due(self, tds):
        d = {}
        for td in tds:
            key = td.css('u::text').extract()[0]
            value = td.xpath('./span/text()').extract()[0]
            d[key] = value
        return d
