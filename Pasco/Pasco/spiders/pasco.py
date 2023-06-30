import scrapy
from scrapy import Request
from copy import deepcopy
from scrapy.utils.response import open_in_browser


class PascoSpider(scrapy.Spider):
    name = 'pasco'
    # allowed_domains = ['http://search.pascopa.com/']
    start_urls = ['http://http://search.pascopa.com//']

    def start_requests(self):
        input_file = open('pasco.txt', 'r', encoding='utf-8')
        self.base_url='https://qpublic.schneidercorp.com/'
        lines = input_file.readlines()
        c=0
        for line in lines:
            try:
                dic={}
                line = str(line).replace('\n', '')
                # line='01-24-16-0000-00100-0000'
                l=line.split('-')
                dic['folio']=line
                dic['proxy'] = 'http://95.211.175.167:13150'
                search_link = 'http://search.pascopa.com/parcel.aspx?sec={}&twn={}&rng={}&sbb={}&blk={}&lot={}&action=Submit'.format(l[0],l[1],l[2],l[3],l[4],l[5])
                url = search_link
                print(search_link)
                c+=1
                print("Going For Folio",line,'==================================================')
                yield Request(url, meta=dic, callback=self.parse)
            except:
                continue

    def parse(self, response):
        maindict = {
            'folio': response.meta['folio'],
            'url': response.request.url,
            'parcel_location': '',
            'parcel_values': '',
            'land_info': '',
            'extra_features': '',
            'sales': '',
            'tax_collector': '',
            'maps': '',

        }
        maindict['parcel_location'] = self.parse_parcel_location(response)
        maindict['parcel_values'] = self.parse_parcel_values(response)
        maindict['land_info'] = self.parse_land_info(response)
        maindict['additional_land_info'] = self.parse_additional_land_info(response)
        maindict['building_info'] = self.parse_building_info(response)
        maindict['extra_features'] = self.parse_extra_features(response)
        maindict['sales'] = self.parse_sales(response)
        maindict['trim'] = self.parse_trim(response)
        maindict['maps'] = self.parse_map(response)
        maindict['proxy'] = 'http://95.211.175.167:13150'
        link='https://pasco.county-taxes.com/public/real_estate/parcels/{}'.format(response.meta['folio'])

        yield Request(url=link, callback=self.parse_tax_collector, meta=maindict, priority=1000)

    def parse_parcel_location(self,response):
        parcel_location={}
        try:
            mailing_address_values=response.xpath("//div[contains(text(),'Mailing Address')]/following-sibling::div[contains(@class,'L')]/text()").extract()
            mailing_address_value=''
            for v in mailing_address_values[:-3]:
                mailing_address_value = mailing_address_value + ' ' +v.replace('\u00a0', '').replace("\r\n", '')
        except:
            mailing_address_value=''
        try:
            physical_address_value=response.xpath("//div[contains(text(),'Physical Address')]/following-sibling::div[contains(@class,'L')]/text()").extract()[0].replace('\u00a0','').replace("\r\n",'')
        except:
            physical_address_value=''
        try:
            jurisdication_value=response.xpath("//div[contains(text(),'Jurisdiction')]/following-sibling::div[contains(@class,'L')]/a/text()").extract()[0].replace('\u00a0','').replace("\r\n",'')
        except:
            jurisdication_value=''
        try:
            try:
                legal_description_value=response.xpath("//div[@id='parcelLocation']//div[contains(@id,'parcelLegal')]/a/parent::div/following-sibling::div/text()").extract()[0].replace('\u00a0','').replace("\r\n",'')
            except:
                legal_description_value=response.xpath("//div[@id='parcelLocation']//div[contains(@id,'parcelLegal')]/a/parent::div/following-sibling::div/following-sibling::div/text()").extract()[0].replace('\u00a0','').replace("\r\n",'')
        except:
            legal_description_value=''

        parcel_location["mailing_address"]=mailing_address_value
        parcel_location["physical_address"]=physical_address_value
        parcel_location["jurisdication"]=jurisdication_value
        parcel_location["legal_description"]=legal_description_value

        return parcel_location

    def parse_parcel_values(self,response):
        parcel_values={}
        rows=response.xpath("//div[@id='parcelValues']//table//tr")
        for row in rows:
            try:
                tds=row.css('td')
                if len(tds)==1:
                    continue
                try:
                    key=tds[0].xpath('./text()').extract()[0].replace('\xa0','').replace("\r\n",'').lower().strip().replace(' ','_')
                except:
                    try:
                        key=tds[0].xpath('.//b/text()').extract()[0].replace('\xa0','').replace("\r\n",'').lower().strip().replace(' ','_')
                    except:
                        try:
                            key = tds[0].xpath('.//span/text()').extract()[0].replace('\xa0','').replace("\r\n",'').lower().strip().replace(' ','_')
                        except:
                            pass
                try:
                    value=tds[1].xpath('./text()').extract()[0].replace('\xa0','')
                except:
                    try:
                        value=tds[1].xpath('.//b/text()').extract()[0].replace('\xa0','')
                    except:
                        try:
                            value = tds[1].xpath('.//span/text()').extract()[0].replace('\xa0','')
                        except:
                            pass

                parcel_values[key]=value
            except:
                continue
        return parcel_values

    def parse_land_info(self,response):
        table=response.xpath("//div[text()='Land Detail ']/following-sibling::div/table")
        rows=table.css('tr')
        try:
            return self.parse_horizontal_table(rows)
        except:
            return []

    def parse_additional_land_info(self,response):
        table=response.xpath("//div[text()='Additional Land Information ']/following-sibling::div/table")
        rows=table.css('tr')
        try:
            return self.parse_horizontal_table(rows)
        except:
            return []

    def parse_building_info(self,response):
        building={}
        table=response.xpath("//div[contains(text(),'Building Information')]/following-sibling::div/table")
        rows=table.css('tr')
        for row in rows:
            tds=row.css('td')
            key1=tds[0].css('b::text').extract()[0]
            key2=tds[2].css('b::text').extract()[0]
            value1=tds[1].xpath('./text()').extract()[0]
            value2=tds[3].xpath('./text()').extract()[0]
            building[key1]=value1
            building[key2]=value2
        try:
            table2=response.xpath("//div[contains(text(),'Building Information')]/parent::div/following-sibling::div/table")
            building['subarea'] = self.parse_horizontal_table(table2.css('tr'))
        except:
            pass
        return building

    def parse_extra_features(self,response):
        table=response.xpath("//div[contains(text(),'Extra Features ')]/following-sibling::div/table")
        rows = table.css('tr')
        try:
            return self.parse_horizontal_table(rows)
        except:
            return []

    def parse_sales(self,response):
        table = response.xpath("//div[contains(text(),'Sales History ')]/following-sibling::div/table")
        rows = table.css('tr')
        try:
            return self.parse_horizontal_table(rows)
        except:
            return []

    def parse_horizontal_table(self, rows):
        headers = rows[0].css("th,td b")
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
                    value = td.css('a::text').extract()[0].replace('\u00a0','')
                except:
                    try:
                        value = td.xpath('./text()').extract()[0].replace('\u00a0','')
                    except:
                        value = ''
                # print('value', value)
                temp[headlist[count]] = value
                count += 1
            data.append(temp)
        return data

    def parse_trim(self,response):
        try:
            trim= response.css('a.pdf::attr(href)').extract()[0]
        except:
            trim=''
        return trim

    def parse_map(self,response):
        try:
            maps= response.xpath("//a[@title='Show Map']/@href").extract()[0]
        except:
            maps=''
        return maps

    def parse_tax_collector(self, response):
        response.meta['tax_collector'] = {
            'amount_due': '',
            'account_history': '',
            'parcel_details': '',
            'mailing_address': '',
            'ad_valorem_taxes': '',
            'nonad_valorem_taxes': '',
            'latest_annual_bill': '',
            'combined_taxes_and_assessments': '',
        }
        response.meta['tax_collector']['amount_due'] = self.parse_amount_due(response)
        response.meta['tax_collector']['account_history'] = self.parse_anual_bill(response)
        parcel_link = 'https://pasco.county-taxes.com/' + response.css('div.parcel a::attr(href)').extract()[0]
        yield Request(url=parcel_link, meta=response.meta, callback=self.parse_parcel_page)

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
        response.meta['tax_collector'] = {
            'parcel_details': '',
            'ad_valorem_tax': '',
            'non_ad_valorem_assessments': '',
            'latest_annual_bill': '',
            'mailing_address': '',
            'combined_tax_collector': '',
        }
        response.meta['tax_collector']['parcel_details'] = self.parse_parcel_deatils(response)
        response.meta['tax_collector']['ad_valorem_tax'] = self.parse_ad_valorem_tax(response)
        response.meta['tax_collector']['non_ad_valorem_assessments'] = self.parse_non_ad_valorem_tax(response)
        response.meta['tax_collector']['mailing_address'] = self.parse_mailing_address(response)
        response.meta['tax_collector']['latest_annual_bill'] = self.parse_latest_annual_bill(response)
        response.meta['tax_collector']['combined_tax_collector'] = self.parse_combined_tax_collector(response)
        del response.meta['depth']
        del response.meta['download_timeout']
        del response.meta['download_latency']
        del response.meta['download_slot']
        del response.meta['proxy']
        yield {'data': response.meta}

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
            pdf_urls.append('https://pasco.county-taxes.com/' + str(pdf_url))
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
            lt[authority]={
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
            "pasco_county_tax_collector": miami_dade_county_tax_collector
        }
        return mailing_address

    def parse_combined_tax_collector(self, response):
        try:
            combined_taxes_and_assessments = str(response.css('div.message::text').extract()[0]).split(':')[-1].replace(
                '\n', '').strip()
        except:
            combined_taxes_and_assessments = ''
        return combined_taxes_and_assessments
