import scrapy
from scrapy import Request
from scrapy.http import FormRequest
from scrapy.utils.response import open_in_browser
from copy import deepcopy


class VolusiaSpiderSpider(scrapy.Spider):
    name = 'volusia'
    base_url1 = 'http://publicaccess.vcgov.org/volusia'
    base_url2 = 'https://volusia.county-taxes.com/'

    def start_requests(self):
        input_file = open('volusia.txt', 'r', encoding='utf-8')
        lines = input_file.readlines()
        c = 0
        for line in lines:
            # try:
            line = line.replace(' ', '').replace('\n', '').strip()
            c += 1
            print(c, " Going for ", line, '---------------------------------------')
            line = str(line)
            d = {'folio': line}
            link = 'http://publicaccess.vcgov.org/volusia/search/commonsearch.aspx?mode=realprop'
            yield Request(url=link, meta={'data': d, 'proxy': 'http://95.211.175.167:13150'}, callback=self.first_page,dont_filter=True)


    def first_page(self, response):
        d = response.meta['data']
        formdata = {
            'ScriptManager1_TSM': ';;AjaxControlToolkit, Version=4.1.50731.0, Culture=neutral, PublicKeyToken=28f01b0e84b6d53e:en-US:f8fb2a65-e23a-483b-b20e-6db6ef539a22:ea597d4b:b25378d2;Telerik.Web.UI, Version=2020.2.512.45, Culture=neutral, PublicKeyToken=121fae78165ba3d4:en-US:88f9a2dc-9cbf-434f-a243-cf2dd9f642dc:16e4e7cd:33715776:58366029:f7645509:24ee1bba:f46195d3:c128760b:19620875:874f8ea2:b2e06756:92fe8ea0:fa31b949:4877f69a:490a9d4e',
            '__VIEWSTATE': response.css("#__VIEWSTATE::attr(value)").extract()[0],
            '__VIEWSTATEGENERATOR': response.css("#__VIEWSTATEGENERATOR::attr(value)").extract()[0],
            '__EVENTVALIDATION': response.css("#__EVENTVALIDATION::attr(value)").extract()[0],
            'hdAction': 'Search',
            'inpAltid': d['folio'],
            'mode': 'REALPROP',
        }
        link = 'http://publicaccess.vcgov.org/volusia/search/CommonSearch.aspx?mode=REALPROP'
        yield FormRequest(url=link, meta={'data': d, 'proxy': 'http://95.211.175.167:13150'}, callback=self.search, formdata=formdata,
                          method='POST',dont_filter=True)

    def search(self, response):
        d = response.meta['data']
        d['parcel'] = self.parse_parcel(response)
        d['owner'] = self.parse_owner(response)
        d['legal_description'] = self.parse_legal(response)
        rows = response.xpath("//table[@id='Sales']//tr")
        d['sales'] = self.parse_table_1(rows)
        # d['residentials'] = self.parse_residentials(response)

        land = self.base_url1 + response.xpath("//span[text()='Land & Agriculture']/parent::a/@href").extract()[
            0].replace('..', '')
        yield Request(url=land, meta={'data': d, 'proxy': 'http://95.211.175.167:13150'}, callback=self.parse_land_page)

    def parse_parcel(self, response):
        parcel = {}
        trs = response.css('#Parcel tr')[:-1]
        for tr in trs:
            key = tr.css('td.DataletSideHeading::text').extract()[0]
            value = tr.css('td.DataletData::text').extract()[0].strip()
            parcel[key] = value
        return parcel

    def parse_owner(self, response):
        tds = response.xpath("//table[@id='Primary Owner']//tr//td")
        try:
            owner_name = tds[1].xpath("./text()").extract()[0]
        except:
            owner_name = ''
        try:
            in_care_of = tds[5].xpath("./text()").extract()[0].replace("\u00a0", '')
        except:
            in_care_of = ''
        try:
            mailing_address = tds[7].xpath("./text()").extract()[0] + ' ' + tds[11].xpath("./text()").extract()[0]
        except:
            mailing_address = ''
        owner = {
            'owner_name': owner_name,
            'in_care_of': in_care_of,
            'mailing_address': mailing_address,
        }
        return owner

    def parse_legal(self, response):
        legal = {}
        trs = response.xpath("//table[@id='Legal']//tr")[:-1]
        for tr in trs:
            key = tr.css('td.DataletSideHeading::text').extract()[0].replace(" ", '_').strip().lower()
            value = tr.css('td.DataletData::text').extract()[0].strip()
            legal[key] = value
        return legal

    def parse_table_1(self, rows):
        try:
            heads = rows[0].css('td')
        except:
            return []
        data = []
        headlist = []
        headdict = {}
        for h in heads:
            h = h.xpath('./text()').extract()[0]
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
                try:
                    value = td.xpath('./text() | .//a/text()').extract()[0]
                except:
                    value = ''
                value = ''.join(value).replace('\r', '').replace('\n', '').strip()
                temp[headlist[count]] = value
                count += 1
            data.append(temp)
        return data

    def parse_table_2(self, rows):
        dictionary = {}
        try:
            for tr in rows:
                try:
                    key = tr.css('td.DataletSideHeading::text').extract()[0].replace(" ", '_').strip().lower()
                    value = tr.css('td.DataletData::text').extract()[0].replace('\u00a0', '')
                    dictionary[key] = value
                except:
                    pass
            return dictionary
        except:
            return []

    def parse_land_page(self, response):
        d = response.meta['data']
        rows = response.xpath("//table[@id='Land & Agriculture']//tr")[:-1]
        land_and_agriculture = self.parse_table_1(rows)

        land_summary = self.parse_table_2(response.xpath("//table[@id='Land Summary']//tr")[:-1])
        total_land_value = self.parse_table_2(response.xpath("//table[@id='Total Land Value']//tr")[:-1])
        agval_summary = self.parse_table_2(response.xpath("//table[@id='AGVAL Summary']//tr")[:-1])
        land = {
            'land_and_agriculture': land_and_agriculture,
            'land_summary': land_summary,
            'total_land_value': total_land_value,
            'agval_summary': agval_summary,
        }
        d['land_info'] = land
        residentials = self.base_url1 + \
                       response.xpath("//span[text()='Bldg(s) - Residential']/parent::a/@href").extract()[
                           0].replace('..', '')
        yield Request(url=residentials, meta={'data': d, 'proxy': 'http://95.211.175.167:13150'}, callback=self.parse_residentials)

    def parse_residentials(self, response):
        d = response.meta['data']
        residential = self.parse_table_2(response.xpath("//table[@id='Residential']//tr")[:-1])
        addition_to_base_area = self.parse_table_1(response.xpath("//table[@id='Additions to Base Area']//tr")[:-1])
        total_building_area = self.parse_table_2(response.xpath("//table[@id='Total Building Area']//tr")[:-1])
        building_rates = self.parse_table_1(response.xpath("//table[@id='Main Building Rates']//tr")[:-1])
        section_rates = self.parse_table_1(response.xpath("//table[@id='Section Rates']//tr")[:-1])
        resi = {
            'residential': residential,
            'addition_to_base_area': addition_to_base_area,
            'total_building_area': total_building_area,
            'building_rates': building_rates,
            'section_rates': section_rates,
        }
        d['residential_info'] = resi

        sketch = self.base_url1 + \
                 response.xpath("//span[text()='Bldg(s) - Sketch']/parent::a/@href").extract()[
                     0].replace('..', '')
        yield Request(url=sketch, meta={'data': d, 'proxy': 'http://95.211.175.167:13150'}, callback=self.parse_sketch)

    def parse_sketch(self, response):
        d = response.meta['data']
        try:
            sketch = response.css('img#BinImage::attr(src)').extract()[0]
        except:
            sketch = ''
        d['sketch'] = sketch
        values = self.base_url1 + \
                 response.xpath("//span[text()='Values']/parent::a/@href").extract()[
                     0].replace('..', '')
        yield Request(url=values, meta={'data': d, 'proxy': 'http://95.211.175.167:13150'}, callback=self.parse_values)

    def parse_values(self, response):
        d = response.meta['data']
        working_roll_values = self.parse_table_1(response.xpath("//table[@id='Working Tax Roll Values']//tr")[:-1])
        final_year_certifies_tax_values = self.parse_table_1(
            response.xpath("//table[@id='Final Years Certified Tax Roll Values']//tr")[:-1])
        working_tax_roll_taxable_values_by_authorities = self.parse_table_1(
            response.xpath("//table[@id='Working Tax Roll Taxable Values by Authority']//tr")[:-1])
        values = {
            'working_roll_values': working_roll_values,
            'working_tax_roll_taxable_values_by_authorities': working_tax_roll_taxable_values_by_authorities,
            'final_year_certifies_tax_values': final_year_certifies_tax_values,
        }
        d['values'] = values
        permits = self.base_url1 + \
                  response.xpath("//span[text()='Permits']/parent::a/@href").extract()[
                      0].replace('..', '')
        yield Request(url=permits, meta={'data': d, 'proxy': 'http://95.211.175.167:13150'}, callback=self.parse_permits)

    def parse_permits(self, response):
        d = response.meta['data']
        permit_summary = self.parse_table_1(response.xpath("//table[@id='Permit Summary']//tr")[:-1])
        permits = {
            'permit_summary': permit_summary,
        }
        d['permits'] = permits
        tax_collector = 'https://volusia.county-taxes.com/public/real_estate/parcels/{}/bills'.format(d['folio'])
        yield Request(url=tax_collector, meta={'data': d, 'proxy': 'http://95.211.175.167:13150'}, callback=self.parse_tax_collector)
        # yield {'data': d}

    # ----------------------------------------------------------
    def parse_tax_collector(self, response):
        d = response.meta['data']
        dd = {
            'amount_due': self.parse_amount_due(response),
            'account_history': self.parse_anual_bill(response),
        }
        d['tax_collector'] = dd
        parcel_link = self.base_url2 + response.css('div.parcel a::attr(href)').extract()[0]
        yield Request(url=parcel_link, meta={'data': d, 'proxy': 'http://95.211.175.167:13150'}, callback=self.parse_parcel_page)

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
        try:
            pdf_url = response.css('div.print-bill a::attr(href)').extract()[0]
            pdf_urls.append(self.base_url2 + str(pdf_url))
        except:
            pdf_urls = ''
        latest_annual_bill = {
            "total": total,
            "pdfurls": pdf_urls,
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
            "volusia_county_tax_collector": miami_dade_county_tax_collector
        }
        return mailing_address

    def parse_combined_tax_collector(self, response):
        try:
            combined_taxes_and_assessments = str(response.css('div.message::text').extract()[0]).split(':')[-1].replace(
                '\n', '').strip()
        except:
            combined_taxes_and_assessments = ''
        return combined_taxes_and_assessments
