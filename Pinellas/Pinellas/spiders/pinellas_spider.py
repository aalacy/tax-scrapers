import scrapy
from scrapy import Request
from scrapy.utils.response import open_in_browser
from Pinellas.pinellas import Pinellas
from copy import deepcopy



class PinellasSpiderSpider(scrapy.Spider):
    name = 'pine'
    base_url2 = 'https://pinellas.county-taxes.com/'
    base_url = 'https://www.pcpao.org/'

    def start_requests(self):
        input_file = open('pinellas.txt', 'r', encoding='utf-8')
        lines = input_file.readlines()
        c = 0
        for line in lines:
            try:
                line = line.replace(' ', '').replace('\n', '').strip()
                c += 1
                print(c," Going for ",line,'---------------------------------------')
                line = str(line)

                dic={}
                dic['folio']=line
                link='https://www.pcpao.org/general.php?strap={}'.format(line)
                headers = {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Cookie": "__utma=246215685.1233730601.1608052800.1608052800.1608052800.1; __utmc=246215685; __utmz=246215685.1608052800.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmt=1; __utmb=246215685.2.10.1608052800; clik=Y",
                    "Referer": "https://www.pcpao.org/clik.html?pg=https://www.pcpao.org/general.php?strap=152715341520022240",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"
                }
                yield Request(url=link, meta={'data':dic, 'proxy': 'http://95.211.175.167:13150'}, callback=self.parse, priority=1000,headers=headers)
            except:
                continue


    def parse(self,response):
        dic=response.meta['data']

        dic['owner_and_property_info'] = self.parse_owner_and_property_info(response)
        dic['exemptions'] = self.parse_exemption(response)
        dic['parcel_information'] = self.parcel_information(response)
        dic['interm_value_information'] = self.parcel_Interim_Value_Information(response)
        dic['Value_History_as_Certified'] = self.parcel_Value_History_as_Certified(response)
        dic['ranked_sales'] = self.parcel_ranked_sales(response)
        dic['land_use'] = self.parse_land_use(response)
        dic['buildings'] = self.parse_buildings(response)
        dic['extra_features'] = self.extra_features(response)
        dic['permit_number'] = self.permit_number(response)
        dic['trim_pdf'] = self.parse_trim(response)
        dic['tax_collector_link'] = self.tax_link(response)
        link=dic['tax_collector_link']
        yield Request(url=link, meta={'data': dic, 'proxy': 'http://95.211.175.167:13150'}, callback=self.parse_tax_collector, priority=1000)



    def parse_owner_and_property_info(self,response):
        dic = {}
        try:
            tds = response.xpath("//th[contains(text(),'Ownership/Mailing Address')]/parent::tr/following-sibling::tr/td")
            dic['name_and_address'] = tds[0].xpath('./text()').extract()[0].replace('<br>', ', ').replace("\t", '').strip()
            dic['site_address'] = tds[1].xpath('./text()').extract()[0].replace('<br>', ', ').replace("\t", '').strip()
        except:
            pass
        try:
            dic['property_use'] = response.xpath("//a[text()='Property Use']/parent::td/text()").extract()[0].replace('<br>', '').replace('\n', ' ').replace("&nbsp;", '').strip()
        except:
            pass
        try:
            dic['tax_district'] = response.xpath("//td[contains(text(),'Current Tax District')]/a/@alt").extract()[0].strip()
        except:
            pass
        try:
            dic['total_heated_sf'] = response.xpath("//td[contains(text(),'Total Heated SF:')]/text()").extract()[0].strip()
        except:
            pass
        try:
            dic['total_gross_sf'] = response.xpath("//td[contains(text(),'Total Gross SF:')]/text()").extract()[0].strip()
        except:
            pass
        try:
            dic['total_gross_sf'] = response.xpath("//td[contains(text(),'Total Units:')]").extract()[0].strip()
        except:
            pass
        try:
            dic['legal_description'] = response.css( "div#legal::text").extract()[0].strip()
        except:
            pass
        return dic

    def parse_exemption(self,response):
        rows = response.xpath("//th[text()='Exemption']//parent::table//tr")
        return self.parse_table_1(response,rows)

    def parse_table_1(self,response, rows):
        d = {}
        try:
            heads = response.xpath("//th[text()='Exemption']//parent::table//th")
        except:
            return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            try:
                h = h.css('b::text').extract()[0]
            except:
                try:
                    h = h.css('a::text').extract()[0]
                except:
                    h = h.xpath('./text()').extract()[0]

            headlist.append(h)
            headdict[h] = ''
        for row in rows[1:]:
            tds = row.css('td')
            count = 0
            temp = deepcopy(headdict)
            if len(tds) != len(headlist):
                continue
            for td in tds:
                value = td.xpath('./text()').extract()[0]
                temp[headlist[count]] = value
                count += 1
            data.append(temp)
        return data

    def parcel_information(self,response):
        rows = response.xpath("//th[text()='Most Recent Recording']//parent::tr//parent::table//tr")
        headdict = {}
        data = []
        headlist = ['Most Recent Recording', 'Sales Comparison', 'Census Tract', 'Evacuation Zone', 'Flood Zone',
                    'Plat Book/Page']
        for h in headlist:
            headdict[h] = {}
        for row in rows[1:]:
            tds = row.css('td')
            count = 0
            temp = deepcopy(headdict)
            for td in tds:
                try:
                    value = td.xpath('./a/@href').extract()[0]
                except:
                    try:
                        value = td.css('div::text').extract()[0]
                    except:
                        value = td.xpath('./text()').extract()[0]
                temp[headlist[count]] = value.replace('\n', '').strip()
                count += 1
            data.append(temp)
        return data

    def parcel_Interim_Value_Information(self,response):
        rows = response.xpath("//a[contains(text(),'Interim Value Information ')]/parent::u/parent::th/parent::tr/parent::table/tr")
        try:
            heads = rows[1].css('td')
        except:
            return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            try:
                h = h.css('a::text').extract()[0].replace('\xa0','')
            except:
                h = h.xpath("./text()").extract()[0].replace('\xa0','')
            h = h.replace('&nbsp;', '')
            headlist.append(h)
            headdict[h] = ''
        for row in rows[2:-1]:
            tds = row.css('td')
            count = 0
            temp = deepcopy(headdict)
            if len(tds) != len(headlist):
                continue
            for td in tds:
                try:
                    value = td.css('b::text').extract()[0]
                except:
                    value = td.xpath('./text()').extract()[0]
                temp[headlist[count]] = value.replace('\n', '').strip()
                count += 1
            try:
                del temp['']
            except:
                pass
            data.append(temp)
        return data

    def parcel_Value_History_as_Certified(self,response):
        rows = response.css("div#valhist table tr")
        try:
            heads = rows[0].css('td')
        except:
            return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            try:
                h = h.css('a::text').extract()[0]
            except:
                h = h.xpath('./text()').extract()[0]
            h = h.replace('&nbsp;', '')
            headlist.append(h)
            headdict[h] = ''
        for row in rows[1:]:
            tds = row.css('td')
            count = 0
            temp = deepcopy(headdict)
            if len(tds) != len(headlist):
                continue
            for td in tds:
                value = td.xpath('./text()').extract()[0]
                temp[headlist[count]] = value.replace('\n', '').strip()
                count += 1
            try:
                del temp['']
            except:
                pass
            data.append(temp)
        return data

    def parcel_ranked_sales(self,response):
        rows = response.xpath("//th[contains(text(),'Ranked Sales ')]/parent::tr/following-sibling::tr")
        try:
            heads = rows[0].css('th')
        except:
            return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            try:
                h = h.css('a::text').extract()[0]
            except:
                h = h.xpath('./text()').extract()[0]
            h = h.replace('&nbsp;', '')
            headlist.append(h)
            headdict[h] = ''
        for row in rows[1:]:
            tds = row.css('td')
            count = 0
            temp = deepcopy(headdict)
            if len(tds) != len(headlist):
                continue
            for td in tds:

                try:
                    value = td.css('a::text').extract()[0]
                except:
                    try:
                        value = td.xpath('./text()').extract()[0]
                    except:
                        value=''
                temp[headlist[count]] = value.replace('\n', '').strip()
                count += 1
            try:
                del temp['']
            except:
                pass
            data.append(temp)
        return data

    def parse_land_use(self,response):
        rows = response.xpath("//a[text()='Land Use']/parent::b/parent::th/parent::tr/parent::table//tr")
        try:
            heads = rows[0].css('th')
        except:
            return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            try:
                h = h.css('a::text').extract()[0]
            except:
                try:
                    h = h.css('b::text').extract()[0]
                except:
                    h =h.xpath('./text()').extract()[0]

            h = h.replace('&nbsp;', '')
            headlist.append(h)
            headdict[h] = ''
        for row in rows[1:]:
            tds = row.css('td')
            count = 0
            temp = deepcopy(headdict)
            if len(tds) != len(headlist):
                continue
            for td in tds:
                try:
                    value = td.css('a::text').extract()[0]
                except:
                    value = td.xpath('./text()').extract()[0]
                temp[headlist[count]] = value.replace('\n', '').strip()
                count += 1
            try:
                del temp['']
            except:
                pass
            data.append(temp)
        return data

    def extra_features(self,response):
        rows = response.css("div#xfsb tr")
        try:
            heads = rows[0].css('td')
        except:
            return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            h = h.xpath('./text()').extract()[0]
            h = h.replace('&nbsp;', '')
            headlist.append(h)
            headdict[h] = ''
        for row in rows[1:]:
            tds = row.css('td')
            count = 0
            temp = deepcopy(headdict)
            if len(tds) != len(headlist):
                continue
            for td in tds:
                value = td.xpath('./text()').extract()[0]
                temp[headlist[count]] = value.replace('\n', '').strip()
                count += 1
            try:
                del temp['']
            except:
                pass
            data.append(temp)
        return data

    def permit_number(self,response):
        rows = response.xpath("//th[text()='Permit Number']/parent::tr/parent::table/tr")
        try:
            heads = rows[0].css('th')
        except:
            return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            h = h.xpath('./text()').extract()[0]
            h = h.replace('&nbsp;', '')
            headlist.append(h)
            headdict[h] = ''
        for row in rows[1:]:
            tds = row.css('td')
            count = 0
            temp = deepcopy(headdict)
            if len(tds) != len(headlist):
                continue
            for td in tds:
                try:
                    value = td.css('a::text').extract()[0]
                except:
                    value = td.xpath('./text()').extract()[0]
                value = value.replace('&nbsp;', ' ')
                temp[headlist[count]] = value.replace('\n', '').strip()
                count += 1
            try:
                del temp['']
            except:
                pass
            data.append(temp)
        return data

    def parse_buildings(self,response ):
        tables = response.xpath("//td[contains(text(),'Building Type')]/parent::tr/parent::table/parent::span")
        buildings = response.xpath("//span[contains(@id,'bb') and not(contains(@id,'title'))]")
        data = []
        for building in buildings:
            d = {}
            tables = building.xpath('./table')
            try:
                rows = tables[0].xpath('.//tr')
                d['structural_elements'] = self.prase_building_structural_elements(rows)
            except:
                pass
            try:
                rows = tables[1].xpath('.//tr')
                d['sub_area_information'] = self.prase_building_sub_area(rows)
            except:
                pass
            try:
                d['sketch'] = building.xpath('./img/@href').extract()[0]
            except:
                pass
            try:
                d['compact_property_record_card_pdf'] = building.xpath(".//a[contains(text(),'Compact Property Record Card')]").get_attribute(
                    'href')
            except:
                pass
            data.append(d)
        return data

    def prase_building_sub_area(self, rows):
        try:
            heads = rows[1].css('th')
        except:
            return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            try:
                h = h.css('a::text').extract()[0]
            except:
                h = h.xpath('./text()').extract()[0]
            h = h.replace('&nbsp;', '')
            headlist.append(h)
            headdict[h] = ''
        for row in rows[2:-1]:
            tds = row.css('td')
            count = 0
            temp = deepcopy(headdict)
            if len(tds) != len(headlist):
                continue
            for td in tds:
                try:
                    value = td.css('a::text').extract()[0]
                except:
                    value = td.xpath('./text()').extract()[0]
                value = value.replace('&nbsp;', ' ')
                temp[headlist[count]] = value.replace('\n', '').strip()
                count += 1
            try:
                del temp['']
            except:
                pass
            data.append(temp)
        return data

    def prase_building_structural_elements(self, rows):
        d = {}
        for row in rows:
            td = row.css('td')
            try:
                value = td.css('b::text').extract()[0]
                key = td.xpath('./text()').extract()[0].replace('\n', '').strip()
            except:
                continue
            d[key] = value
        return d

    def parse_trim(self,response):
        try:
            return self.base_url+response.xpath("//a[contains(text(),'Property Taxes (TRIM Notice)')]/@href").extract()[0]
        except:
            return ''

    def tax_link(self,response):
        try:
            return  response.xpath("//a[contains(text(),'Tax Bill')]/@href").extract()[0]
        except:
            return ''

    def parse_tax_collector(self, response):
        d = response.meta['data']
        d2 = {
            'amount_due': '',
            'account_history': '',
            'parcel_details': '',
            'mailing_address': '',
            'latest_annual_bill': '',
            'combined_taxes_and_assessments': '',
        }
        d2['amount_due'] = self.parse_amount_due(response)
        d2['account_history'] = self.parse_anual_bill(response)
        d['tax_collector'] = d2
        parcel_link = self.base_url2 + response.css('div.parcel a::attr(href)').extract()[0]
        yield Request(url=parcel_link, meta={'data': d, 'proxy': 'http://95.211.175.167:13150' }, callback=self.parse_parcel_page)

    def parse_amount_due(self, response):
        try:
            total_amount = str(response.css("div.bills tfoot div.col::text").extract()[1]).replace('\n', '').strip()
        except:
            total_amount = ''
        rows = response.css("div.bills tr.regular")
        lst = []
        for row in rows:
            key = str(row.css("th").css("a::text").extract()[0]).replace('\n', '').strip()
            value = str(row.css("td.balance.for-cart::text").extract()[0]).replace('\n', '').strip()
            # print(key, value)
            lst.append({'bill': key, 'amount_due': value})
        return {'total_amount_due': total_amount, 'values': lst}

    def parse_anual_bill(self, response):
        rows = response.css("table.table.table-hover.bills tbody")
        account_history = dict()
        cyear = ''
        for row in rows:
            try:
                cname = row.css('tr::attr(class)').extract()[0]
            except:
                continue

            if 'regular' in cname:
                year = str(row.css("th.description a::text").extract()[0]).split()[0].strip()
                cyear = year + '_annual_bill'
                try:
                    amm_due = str(row.css('td.balance::text').extract()[0]).replace('\n', '').strip()
                except:
                    amm_due = ''
                account_history[cyear] = {
                    'status': {'payments': [], 'refund': [], 'installments': {}, 'tax_deed': [], 'certificates': []},
                    'amount_due': amm_due, }

                try:
                    paid = str(row.css('td.status::text').extract()[1]).replace('\n', '').strip()
                except:
                    paid = ''
                try:
                    date = str(row.css('td.as-of time::text').extract()[0]).replace('\n', '').strip()
                except:
                    date = ''
                try:
                    receipt_number = str(row.css('td.message::text').extract()[1]).replace('\n', '').strip()
                except:
                    receipt_number = ''
                d = {'paid': paid,
                     'date': date,
                     'receipt_number': receipt_number
                     }
                account_history[cyear]['status']['payments'].append(d)
            elif 'refund' in cname:
                try:
                    cleard = str(row.css('td.status::text').extract()[1]).replace('\n', '').strip()
                except:
                    cleard = ''
                try:
                    date2 = str(row.css('td.as-of::text').extract()[0]).replace('\n', '').strip()
                except:
                    date2 = ''
                try:
                    check_number = str(row.css('th.description::text').extract()[0]).replace('\n', '').split()[
                        -1].strip()
                except:
                    check_number = ''
                try:
                    recipient = str(row.css('td.message div::text').extract()[1]).replace('\n', '').strip()
                except:
                    recipient = ''
                try:
                    temp = row.css('td.message div::text').extract()[3:-1]
                    recipient_address = ''
                    for t in temp:
                        recipient_address += ' ' + t.replace('\n', '').strip()
                    recipient_address.strip()
                except:
                    recipient_address = ''
                d2 = {
                    "cleard": cleard,
                    "date": date2,
                    "check_number": check_number,
                    "recipient": recipient,
                    "recipient_address": recipient_address
                }
                # print(year, d2)
                account_history[cyear]['status']['refund'].append(d2)
            elif 'certificate' in cname:
                try:
                    cleard = str(row.css('td.status span::text').extract()[0]).replace('\n', '').strip()
                except:
                    cleard = ''
                try:
                    date2 = str(row.css('td.as-of::text').extract()[0]).replace('\n', '').strip()
                except:
                    date2 = ''
                try:
                    a = str(row.css('td.message::text').extract())
                    # print('HERE: ', a)
                    face = str(a[1]).replace('\n', '').strip()
                    rate = str(a[2]).replace('\n', '').strip()
                except:
                    face = rate = ''
                d5 = {
                    "status": cleard,
                    "date": date2,
                    'face': face,
                    'rate': rate,
                }
                try:
                    name = str(row.css("th.description a::text").extract()[0]).strip().replace(' #', '_').replace(' ',
                                                                                                                  '_').lower()
                except:
                    pass
                account_history[cyear]['status']['certificates'].append({name: d5})
            elif 'installment' in cname:
                iyear = str(row.css("th.description a::text").extract()[0]).strip().replace(' #', '_').replace(' ',
                                                                                                               '_').lower()
                cyear = iyear.split('_')[0] + '_annual_bill'
                try:
                    amm_due = str(row.css('td.balance::text').extract()[0]).replace('\n', '').strip()
                except:
                    amm_due = ''
                try:
                    paid = str(row.css('td.status::text').extract()[1]).replace('\n', '').strip()
                except:
                    paid = ''
                try:
                    date3 = str(row.css('td.as-of time::text').extract()[0]).replace('\n', '').strip()
                except:
                    date3 = ''
                try:
                    receipt_number2 = str(row.css('td.message::text').extract()[1]).replace('\n', '').strip()
                except:
                    receipt_number2 = ''

                d3 = {
                    'paid': paid,
                    'date': date3,
                    'receipt_number': receipt_number2,
                    'amount_due': amm_due
                }
                #
                try:
                    account_history[cyear]['status']['installments'][iyear].append(d3)
                except:
                    try:
                        account_history[cyear]['status']['installments'][iyear] = [d3]
                    except:
                        # cyear = iyear.split('_')[0]+'_annual_bill'
                        account_history[cyear] = {
                            'status': {'payments': [], 'refund': [], 'installments': {iyear: [d3]},
                                       'amount_due': amm_due, 'tax_deed': [], 'certificates': []}}
            elif 'deed' in cname:
                try:
                    cleard = str(row.css('td.status span::text').extract()[0]).replace('\n', '').strip()
                except:
                    cleard = ''
                try:
                    date2 = str(row.css('td.as-of::text').extract()[0]).replace('\n', '').strip()
                except:
                    date2 = ''
                d5 = {
                    "cleard": cleard,
                    "date": date2,
                }
                try:
                    name = str(row.css("th.description a::text").extract()[0]).strip().replace(' #', '_').replace(' ',
                                                                                                                  '_').lower()
                except:
                    pass
                account_history[cyear]['status']['tax_deed'].append({name: d5})
            try:
                row.css('tr.partial-payment').extract()[0]
                r = row.css('tr.partial-payment')
            except:
                continue
            try:
                amm_due = str(r.css('td.balance::text').extract()[0]).replace('\n', '').strip()
            except:
                amm_due = ''
            try:
                paid = str(r.css('td.status::text').extract()[1]).replace('\n', '').strip()
            except:
                paid = ''
            try:
                date = str(r.css('td.as-of time::text').extract()[0]).replace('\n', '').strip()
            except:
                date = ''
            try:
                receipt_number = str(r.css('td.message::text').extract()[1]).replace('\n', '').strip()
            except:
                receipt_number = ''
            d = {'paid': paid,
                 'date': date,
                 'receipt_number': receipt_number
                 }
            account_history[cyear]['status']['payments'].append(d)
        return account_history

    def parse_parcel_page(self, response):
        d = response.meta['data']
        # d['tax_collector'] = {
        # 'parcel_details': '',
        # 'ad_valorem_tax': '',
        # 'non_ad_valorem_assessments': '',
        # 'latest_annual_bill': '',
        # 'mailing_address': '',
        # 'combined_tax_collector': '',
        # }
        d['tax_collector']['parcel_details'] = self.parse_parcel_deatils(response)
        d['tax_collector']['ad_valorem_tax'] = self.parse_ad_valorem_tax(response)
        d['tax_collector']['non_ad_valorem_assessments'] = self.parse_non_ad_valorem_tax(response)
        d['tax_collector']['mailing_address'] = self.parse_mailing_address(response)
        d['tax_collector']['latest_annual_bill'] = self.parse_latest_annual_bill(response)
        d['tax_collector']['combined_tax_collector'] = self.parse_combined_tax_collector(response)
        yield {'data': d}

    def parse_latest_annual_bill(self, response):
        tr = response.css('table.bills tbody tr')
        try:
            bill = str(tr.css('th.description::text').extract()[0]).replace('\n', '')
        except:
            bill = ''
        try:
            escrow_code = str(tr.css('td.escrow::text').extract()[0]).replace('\n', '')
        except:
            escrow_code = ''
        try:
            millage_code = str(tr.css('td.millage::text').extract()[0]).replace('\n', '')
        except:
            millage_code = ''
        try:
            amount_due = str(tr.css('td.balance ::text').extract()[0]).replace('\n', '')
        except:
            amount_due = ''
        try:
            alternate = str(tr.css('td.alternate ::text').extract()[0]).replace('\n', '')
        except:
            alternate = ''

        total = {
            "bill": bill,
            "alternate_key": alternate,
            "escrow_code": escrow_code,
            "millage_code": millage_code,
            "amount_due": amount_due
        }
        pdf_urls = []
        pdf_names = []
        try:
            pdf_url = response.css('div.print-bill a::attr(href)').extract()[0]
            pdf_urls.append(self.base_url2 + str(pdf_url))
            pdf_names.append(str(pdf_url).split('parcels/')[1].replace('/print', '.pdf').replace('/', '_'))
        except:
            pdf_urls = ''
        latest_annual_bill = {
            "total": total,
            "pdfurls": pdf_urls,
            "pdfs": pdf_names,
            "notices": ''
        }
        return latest_annual_bill

    def parse_ad_valorem_tax(self, response):
        lt = {}
        authority = ''
        tbodies = response.css('div.advalorem table.table-hover tbody')
        for tbody in tbodies:
            # cname = tbody.xpath('@class').extract()[0]
            authority = tbody.css('th::text').extract()[0].strip().lower().replace(' ', '_')

            # if cname == 'district-group':
            #     authority = str(tbody.css('th::text').extract()[0]).strip().lower().replace(' ', '_')
            #     lt[authority] = []
            # elif cname == 'taxing-authority':
            auth = tbody
            try:
                taxing_authority = str(auth.css('th::text').extract()[0]).replace('\n', '')
            except:
                taxing_authority = ''
            try:
                milage = str(auth.css('td.millage::text').extract()[0]).replace('\n', '')
            except:
                milage = ''
            try:
                assessed = str(auth.css('td.assessed::text').extract()[0]).replace('\n', '')
            except:
                assessed = ''
            try:
                exemption = str(auth.css('td.exemption::text').extract()[0]).replace('\n', '')
            except:
                exemption = ''
            try:
                taxable = str(auth.css('td.taxable::text').extract()[0]).replace('\n', '')
            except:
                taxable = ''
            try:
                tax = str(auth.css('td.tax::text').extract()[0]).replace('\n', '')
            except:
                tax = ''
            lt[authority] = {
                "taxing_authority": taxing_authority,
                "millage": milage,
                "assessed": assessed,
                "exemption": exemption,
                "taxable": taxable,
                "tax": tax
            }
        totals = response.css('div.advalorem table.table-hover tfoot')
        t_milage = str(totals.css('td.millage::text').extract()[0]).replace('\n', '')
        t_tax = str(totals.css('td.tax::text').extract()[0]).replace('\n', '')
        total = {
            "millage": t_milage,
            "tax": t_tax
        }
        ad_valorem_taxes = {
            "table": lt,
            'total': total
        }
        return ad_valorem_taxes

    def parse_non_ad_valorem_tax(self, response):
        non_auth = []
        non_auths = response.css('div.nonadvalorem tbody tr')
        for tr in non_auths:
            name = str(tr.css('th.col::text').extract()[0]).replace('\n', '')
            rate = str(tr.css('td.rate::text').extract()[0]).replace('\n', '')
            amount = str(tr.css('td.amount::text').extract()[0]).replace('\n', '')
            non_auth.append({
                "name": name,
                "rate": rate,
                "amount": amount
            })
        totals = response.css('div.nonadvalorem tfoot')
        try:
            t_rate = str(totals.css('td.rate::text').extract()[0]).replace('\n', '')
        except:
            t_rate = ''
        try:
            t_amount = str(totals.css('td.amount::text').extract()[0]).replace('\n', '')
        except:
            t_amount = ''
        total = {
            "t_rate": t_rate,
            "t_amount": t_amount
        }
        nonad_valorem_taxes = {
            "table": non_auth,
            'total': total
        }

        return nonad_valorem_taxes

    def parse_parcel_deatils(self, response):
        try:
            owner = str(response.css('div.owner::text').extract()[0]).replace('\n', '').strip()
        except:
            owner = ''
        try:
            situs = ''
            situs = str(response.css('div.address.situs div.value::text').extract()[0]).replace('\n', '').strip()
            situs += ' ' + str(response.css('div.address.situs div.value::text').extract()[1]).replace('\n', '').strip()
        except:
            if situs == '':
                situs = ''
        divs = response.css('div.account-details div.value::text').extract()
        div2 = response.css('div.account-details div.value')
        try:
            account = str(divs[0]).replace('\n', '').strip()
        except:
            account = ''
        try:
            millage_code = str(divs[1]).replace('\n', '').strip()
        except:
            millage_code = ''
        try:
            millage_rate = str(divs[2]).replace('\n', '').strip()
        except:
            millage_rate = ''
        escrow_company = ''
        try:
            # .replace('\n', '').replace('\\n','').replace("'", "").replace('[', '').replace('],', '').replace('"', '').strip()
            escrow_companies = div2[3].css('div::text').extract()
            for es in escrow_companies:
                escrow_company += es.replace('\n', '').replace('\\n', '').strip()
        except:
            escrow_company = ''

        try:
            assessed_value = str(response.css('div.parcel-values div.value::text').extract()[0]).replace('\n',
                                                                                                         '').strip()
        except:
            assessed_value = ''
        try:
            school_assessed_value = str(response.css('div.parcel-values div.value::text').extract()[1]).replace('\n',
                                                                                                                '').strip()
        except:
            school_assessed_value = ''

        bills = response.css('div.bill-details div.value::text').extract()
        try:
            ad_valorem = str(bills[0]).replace('\n', '').strip()
        except:
            ad_valorem = ''
        try:
            non_ad_valorem = str(bills[1]).replace('\n', '').strip()
        except:
            non_ad_valorem = ''
        try:
            total_discountable = str(bills[2]).replace('\n', '').strip()
        except:
            total_discountable = ''
        try:
            no_discount_nava = str(bills[3]).replace('\n', '').strip()
        except:
            no_discount_nava = ''
        try:
            total_tax = str(bills[4]).replace('\n', '').strip()
        except:
            total_tax = ''
        annual_bill_2019 = {
                               "ad_valorem": ad_valorem,
                               "non-ad_valorem": non_ad_valorem,
                               "total_discountable": total_discountable,
                               "no_discount_nava": no_discount_nava,
                               "total_tax": total_tax
                           },
        try:
            legal_description = str(
                response.css('div.legal div.row.no-gutters.px-1 div.col-12::text').extract()[0]).replace('\n',
                                                                                                         '').strip()
            try:
                legal_description += '' + str(
                    response.css('div.legal div.row.no-gutters.px-1 div.col-12 span.expanded::text').extract()[
                        0]).replace('\n', '').strip()
            except:
                pass
        except:
            legal_description = ''
        rows = response.css('div.location div.row.px-1')
        location = {
        }
        for row in rows:
            key = str(row.css('div.label::text').extract()[0]).strip().replace(' ', '_').replace(',', '').replace(':',
                                                                                                                  '').lower()
            value = str(row.css('div.value::text').extract()[0]).strip().lower()
            location[key] = value

        parcel_details = {
            "owner": owner,
            "situs": situs,
            "account": account,
            "millage_code": millage_code,
            "millage_rate": millage_rate,
            "assessed_value": assessed_value,
            "school_assessed_value": school_assessed_value,
            "escrow_company": escrow_company,
            "2019_annual_bill": annual_bill_2019,
            "legal_description": legal_description,
            "location": location
        }
        return parcel_details

    def parse_mailing_address(self, response):
        try:
            miami_dade_county_tax_collector = str(
                response.css('div.mailing-address div.value::text').extract()[0]).replace('\n', '').strip()
        except:
            miami_dade_county_tax_collector = ''

        mailing_address = {
            "pinellas_county_tax_collector": miami_dade_county_tax_collector
        }
        return mailing_address

    def parse_combined_tax_collector(self, response):
        try:
            combined_taxes_and_assessments = str(response.css('div.message::text').extract()[0]).split(':')[-1].replace(
                '\n', '').strip()
        except:
            combined_taxes_and_assessments = ''
        return combined_taxes_and_assessments






