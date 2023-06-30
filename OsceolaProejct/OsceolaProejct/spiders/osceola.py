import scrapy
from scrapy import Request
from scrapy.utils.response import open_in_browser
from copy import deepcopy
from pymongo import MongoClient
from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options
from time import sleep
from selenium.webdriver.common.by import By

class OsceolaSpider(scrapy.Spider):
    name = 'osceola'
    # allowed_domains = ['https://ira.property-appraiser.org/']
    # start_urls = ['http://https://ira.property-appraiser.org//']
    def start_requests(self):
        input_file = open('osceola.txt', 'r', encoding='utf-8')
        self.base_url='https://ira.property-appraiser.org/'
        self.base_url2='https://osceola.county-taxes.com/'
        lines = input_file.readlines()
        for line in lines:
            dic={}
            dic['proxy'] = 'http://95.211.175.167:13150'
            line = str(line).replace('\n', '')
            search_link = 'https://ira.property-appraiser.org/PropertySearch/ajax/ParcelSearch.aspx?parcelid={}&owner=&streetlabel=&orderByColumnIndex=0&page=1&returncount=true'.format(line)
            url = search_link + str(line)
            print("Going For Folio",line,'------------------------')
            yield Request(url, meta=dic, callback=self.parse, priority=1000)
    def parse(self, response):

        maindict = {
            'folio': str(response.request.url).split('KeyValue=')[-1],
            'url': response.request.url,
            'parcel_summary': '',
            'owner_info': '',
            'land_info': '',
            'extra_features': '',
            'sales': '',
            'tax_collector': '',
        }
        maindict['owner_info'] = self.parse_owner_info(response)
        maindict['tax_value'] = self.parse_tax_value(response)
        maindict['sales'] = self.parse_sales(response)
        maindict['land_info'] = self.parse_land_info(response)
        maindict['extra_features'] = self.parse_etxra_info(response)
        maindict['leagl_desciption'] = self.parse_legal_info(response)
        maindict['building_info'] = self.parse_building_info(response)
        maindict['building_sub_area'] = self.parse_building_subareas(response)
        maindict['maps'] = self.parse_maps(response)
        maindict['property_card'] = self.parse_property_card(response)
        maindict['trim'] = self.parse_trim_notice(response)
        maindict['sketches'] = self.parse_sketches(response)
        maindict['photos'] = self.parse_photos(response)
        link = response.xpath("//a[contains(@id,'taxCollector')]/@href").extract()[0]
        yield Request(url=link, callback=self.parse_tax_collector, meta={'data':maindict, 'proxy': 'http://95.211.175.167:13150'}, priority=1000)
    def parse_owner_info(self,response):
        owner_info={}
        try:
            trs=response.css('div#owner-information table tr')
        except:
            return {}
        for tr in trs:
            key=str(tr.css('th::text').extract()[0]).replace(' ','_').lower().strip()
            value=tr.css('td::text').extract()[0].strip()
            owner_info[key]=value
        return owner_info
    def parse_tax_value(self,response):
        try:
            table=response.xpath("//h4[contains(text(),'Tax Values')]/parent::div/div/table")
        except:
            return []
        theader=table.css('thead tr')[0]
        heads=theader.css('th')
        headslist = []
        headsdic = {}
        for head in heads:
            t = head.xpath('./text()').extract()[0].replace('/', ' ').strip().replace(' ', '_').lower()
            headslist.append(t)
            headsdic[t] = {}
        trs=table.css('tbody tr')
        for tr in trs:
            keys=tr.css('th::text').extract()
            values=tr.css('td::text').extract()
            headsdic['current_values'][keys[0]]=values[0]
            headsdic['certified_values'][keys[1]]=values[1]
        return headsdic
    def parse_sales(self, response):
        try:
            table = response.xpath("//h4[contains(text(),'Sales Information')]/parent::div/div/table")
        except:
            return []
        lst = []
        lstdata = []
        headers=table.css('thead tr th::text').extract()
        for head in headers:
            name = head.strip().replace('.', '').replace(' ', '_').replace('/', '').lower()
            lst.append(name)
        rows = table.css('tbody tr')
        for row in rows:
            tds = row.css('td' )
            count = 0
            d = {}
            for td in tds[:-1]:
                txt=td.xpath('./text()').extract()[0].replace('\u00a0', '').strip()
                if txt=='':
                    try:
                        txt = td.xpath('.//a/text()').extract()[0].replace('\u00a0', '').strip()
                    except:
                        txt=''

                d[lst[count]] = txt
                count += 1
            lstdata.append(d)
        return lstdata
    def parse_land_info(self, response):
        try:
            table = response.xpath("//h4[contains(text(),'Land Information')]/parent::div/div/table")
        except:
            return []
        lst = []
        lstdata = []
        headers=table.css('thead tr th::text').extract()
        for head in headers:
            name = head.strip().replace('.', '').replace(' ', '_').replace('/', '').lower()
            lst.append(name)
        rows = table.css('tbody tr')
        for row in rows:
            tds = row.css('td' )
            count = 0
            d = {}
            for td in tds[:-1]:
                txt=td.xpath('./text()').extract()[0].replace('\u00a0', '').strip()
                if txt=='':
                    try:
                        txt = td.xpath('.//a/text()').extract()[0].replace('\u00a0', '').strip()
                    except:
                        txt=''

                d[lst[count]] = txt
                count += 1
            lstdata.append(d)
        return lstdata
    def parse_etxra_info(self, response):
        try:
            table = response.xpath("//h4[contains(text(),'Extra Features')]/parent::div/div/table")
        except:
            return
        lst = []
        lstdata = []
        headers=table.css('thead tr th::text').extract()
        for head in headers:
            name = head.strip().replace('.', '').replace(' ', '_').replace('/', '').lower()
            lst.append(name)
        rows = table.css('tbody tr')
        for row in rows:
            tds = row.css('td' )
            count = 0
            d = {}
            for td in tds:
                txt=td.xpath('./text()').extract()[0].replace('\u00a0', '').strip()
                if txt=='':
                    try:
                        txt = td.xpath('.//a/text()').extract()[0].replace('\u00a0', '').strip()
                    except:
                        txt=''

                d[lst[count]] = txt
                count += 1
            lstdata.append(d)
        return lstdata
    def parse_legal_info(self,response):
        legal_description={}
        try:
            table = response.xpath("//h4[contains(text(),'Legal Description')]/parent::div/div/table")
        except:
            return []
        trs = table.css('tbody tr')
        for tr in trs:
            key=str(tr.css('th::text').extract()[0]).replace(' ','_').lower().strip()
            value=tr.css('td::text').extract()[0].strip()
            legal_description[key]=value
        return legal_description
    def parse_trim_notice(self,response):
        try:
            trim=response.xpath("//li[contains(@id,'trim-notice')]/a/@href").extract()[0]
        except:
            trim=''
        return trim
    def parse_property_card(self,response):
        try:
            property_card=response.xpath("//li[contains(@id,'parcelPDF')]/a/@href").extract()[0]
        except:
            property_card=''
        return property_card
    def parse_maps(self,response):
        try:
            maps=response.xpath("//a[contains(@id,'agsFeatureImage')]/@href").extract()[0]
        except:
            maps=''
        return maps
    def parse_building_info(self,response):
        d={}
        headsdic={}
        try:
            tables = response.xpath("//h4[contains(text(),'Building Information')]/parent::div/div/table")
        except:
            return []
        for table in tables:
            th=table.css('thead th::text').extract()[0].strip().replace(':','_').lower().replace(' ','_')
            d[th]={}
            rows=table.css('tbody tr')
            for tr in rows:
                keys = tr.css('th::text').extract()
                values = tr.css('td::text').extract()
                try:
                    d[th][keys[0]] = values[0].replace('\r\n','')
                except:
                    pass
                try:
                    d[th][keys[1]] = values[1].replace('\r\n','')
                except:
                    pass
            headsdic[th]=d[th]
        try:
            del headsdic['building_1__photos']
            del headsdic['building_2__photos']
            del headsdic['building_3__photos']
            del headsdic['building_1__sketch']
            del headsdic['building_2__sketch']
            del headsdic['building_3__sketch']
        except:
            pass
        return headsdic
    def parse_sketches(self,response):
        all_sketches=[]
        try:
            sketches = response.xpath("//a[contains(@class,'building-sketches')]/@href").extract()
            for sketch in sketches:
                all_sketches.append(sketch)
                all_sketches=list(set(all_sketches))
        except:
            all_sketches = []
        return all_sketches
    def parse_photos(self,response):
        all_photos = []
        try:
            photos = response.xpath("//a[contains(@class,'building-photo')]/@href").extract()
            for sketch in photos:
                all_photos.append(sketch)

        except:
            all_photos = []
        return all_photos
    def parse_building_subareas(self,response):
        try:
            tables = response.xpath("//h4[contains(text(),'Building Information')]/parent::div/div/table")
        except:
            return []
        d = {}
        headsdic = {}
        for table in tables:
            th = table.css('thead th::text').extract()[0].strip().replace(':', '_').lower().replace(' ', '_')
            d[th] = {}
            rows=response.xpath("//th[contains(text(),'subarea')]/parent::tr/following-sibling::tr")
            d[th]=self.parse_horizontal_table(rows)
            headsdic[th] = d[th]

        try:
            del headsdic['building_1__photos']
            del headsdic['building_2__photos']
            del headsdic['building_3__photos']
            del headsdic['building_1__sketch']
            del headsdic['building_2__sketch']
            del headsdic['building_3__sketch']
        except:
            pass
        return headsdic
    def parse_horizontal_table(self, rows):
        headers = rows[0].css("th")
        rows = rows[1:]
        headlist = []
        headdict = {}
        data = []
        for h in headers:
            h = h.xpath('./text()').extract()
            h = ' '.join(h).replace('<br>', ' ').replace('\n', '').strip().replace(' ', '_').lower()
            #
            headlist.append(h)
            headdict[h] = ''

        for row in rows:
            tds = row.css("td, th")
            temp = deepcopy(headdict)
            count = 0
            for td in tds:
                # print(td)
                try:
                    value = td.css('a::text').extract()[0]
                except:
                    try:
                        value = td.xpath('./text()').extract()[0]
                    except:
                        value = ''
                # print('value', value)
                temp[headlist[count]] = value
                count += 1
            data.append(temp)
        return data
    def parse_tax_collector(self, response):
        d = response.meta['data']
        d2 = {
            'amount_due': '',
            'account_history': '',
            'parcel_details': '',
            'mailing_address': '',
            'latest_annual_bill': '',
        }
        d2['amount_due'] = self.parse_amount_due(response)
        d2['account_history'] = self.parse_anual_bill(response)
        d['tax_collector'] = d2
        parcel_link = self.base_url2 + response.css('div.parcel a::attr(href)').extract()[0]
        yield Request(url=parcel_link, meta={'data':d, 'proxy': 'http://95.211.175.167:13150'}, callback=self.parse_parcel_page)
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
            "osceola_county_tax_collector": miami_dade_county_tax_collector
        }
        return mailing_address
    def parse_combined_tax_collector(self, response):
        try:
            combined_taxes_and_assessments = str(response.css('div.message::text').extract()[0]).split(':')[-1].replace(
                '\n', '').strip()
        except:
            combined_taxes_and_assessments = ''
        return combined_taxes_and_assessments







    # def parse_tax_collector(self,response):
    #     taxes=[]
    #     count=0
    #     tbodies=response.xpath("//table[contains(@class,'bill-history')]/tbody[contains(@class,'paid') or contains(@class,' ')]")[:3]
    #     for tbody in tbodies:
    #         rows=tbody.css('tr')
    #         for row in rows:
    #             count+=1
    #             try:
    #                 bill='https://osceola.county-taxes.com/'+row.css('td.bill a::attr(href)').extract()[0].replace('\n','')
    #             except:
    #                 bill=''
    #             try:
    #                 balance=row.css('td.balance::text').extract()[0].replace('\n','')
    #             except:
    #                 balance=''
    #             try:
    #                 date=row.css('td.date::text').extract()[0].replace('\n','')
    #             except:
    #                 date=''
    #             try:
    #                 events=row.css('td.event::text').extract()
    #                 paid=events[1].replace('\n','')
    #                 reciept=events[2].replace('\n','')
    #             except:
    #                 paid = ''
    #                 reciept = ''
    #             if not (count >= 4):
    #                 try:
    #                     pdf_url='https://osceola.county-taxes.com/'+row.css('td.action a::attr(href)').extract()[0]
    #                 except:
    #                     pdf_url=''
    #             else:
    #                 pdf_url=''
    #             d={
    #                 'bill': bill,
    #                 'balance': balance,
    #                 'date': date,
    #                 'paid': paid,
    #                 'reciept': reciept,
    #                 'pdf_url': pdf_url,
    #             }
    #             taxes.append(d)
    #         response.meta['tax_collector'] = taxes
    #         count2=0
    #         if len(taxes) > 0:
    #             for da in response.meta['tax_collector']:
    #                 count2 += 1
    #                 if (count2 <= 4):
    #                     options = Options()
    #                     options.headless = True
    #                     browser = Firefox(options=options)
    #                     link = da['bill']
    #                     da['property_data'] = self.seleninum_parse_two(browser, link)
    #                     try:
    #                         browser.quit()
    #                     except:
    #                         pass
    #                 else:
    #                     break
    #     print(response.meta)
    #     del response.meta['download_timeout']
    #     del response.meta['download_slot']
    #     del response.meta['depth']
    #     del response.meta['download_latency']
    #     yield {'data': response.meta}
    # def seleninum_parse_two(self, browser, link):
    #     browser.get(link)
    #     dic = {}
    #     dic['ad_valorem_taxes'] = self.parse_table_two(browser.find_element(By.XPATH,"//th[.='Taxing authority']/parent::tr/parent::thead/parent::table"))
    #     dic['non_ad_valorem_taxes'] = self.parse_table_two(browser.find_element(By.XPATH,"//th[.='Levying authority']/parent::tr/parent::thead/parent::table"))
    #     dic['combined_taxes']=str(browser.find_element(By.CSS_SELECTOR,'p.combined').text).replace('Combined taxes and assessments:','').strip()
    #     dic['bill_identifiers']=self.parse_table_two(browser.find_element(By.CSS_SELECTOR,'table.bill-identifiers'))
    #     return dic
    # def parse_table_two(self, table):
    #     lst = []
    #     lstdata = []
    #     headers = table.find_elements(By.CSS_SELECTOR,'thead tr th')
    #     for head in headers:
    #         name = head.text.strip().replace('.', '').replace(' ', '_').replace('/', '').lower()
    #         lst.append(name)
    #     rows = table.find_elements(By.CSS_SELECTOR,'tbody tr')
    #     for row in rows:
    #         tds = row.find_elements(By.CSS_SELECTOR,'td')
    #         count = 0
    #         d = {}
    #         for td in tds:
    #             txt = td.text.replace('\u00a0', '').strip().replace('\u2014','')
    #             if txt == '':
    #                 try:
    #                     txt = td.find_element(By.CSS_SELECTOR,'a').text.replace('\u00a0', '').strip().replace('\u2014','')
    #                 except:
    #                     txt = ''
    #
    #             d[lst[count]] = txt
    #             count += 1
    #         lstdata.append(d)
    #     return lstdata



