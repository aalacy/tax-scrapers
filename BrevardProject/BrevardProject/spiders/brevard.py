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


class BrevardSpider(scrapy.Spider):
    name = 'bv'
    allowed_domains = ['https://www.bcpao.us/', 'brevard.county-taxes.com']
    start_urls = ['https://www.bcpao.us/PropertySearch/#/account/2000167']
    base_url2 = 'https://brevard.county-taxes.com/'

    def start_requests(self):
        input_file = open('Brevard.txt', 'r', encoding='utf-8')
        lines = input_file.readlines()
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-logging')
        for line in lines:
            id = str(line).replace('\n', '')
            url = 'https://www.bcpao.us/PropertySearch/#/nav/Search'
            print('lineeeee', id)
            browser = Chrome(options=options)
            dic = {'folio': id}
            self.selenium_parse(browser, url, id, dic)
            link = self.get_tax_link(browser)
            dic['tax_collector'] = {}
            try:
                browser.quit()
            except:
                pass
            yield Request(link, meta=dic, callback=self.tax_collector_scrapy)


    def selenium_parse(self, browser, url, id, maindict):
        browser.get(url)
        browser.find_element(By.CSS_SELECTOR, '#txtPropertySearch_Pid').send_keys(id)
        btn = browser.find_element(By.CSS_SELECTOR, '#btnPropertySearch_RealProperty_Go')
        browser.execute_script('arguments[0].click()', btn)
        sleep(2)
        try:
            first = browser.find_element(By.CSS_SELECTOR, '#divSearchResultsContainer table tbody tr')
            browser.execute_script('arguments[0].click()', first)
        except:
            pass
        maindict['folio']=str(id)
        maindict['owner_inforamtion'] = self.owner_inforamtion(browser)
        maindict['value_inforamtion'] = self.value_inforamtion(browser)
        maindict['sales_inforamtion'] = self.sales_inforamtion(browser)
        maindict['building_inforamtion'] = self.building_inforamtion(browser)
        maindict['sketch'], maindict['trim'] = self.sketch_and_trim(browser)

    def owner_inforamtion(self, browser):
        owners = WebDriverWait(browser, 60).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-bind*='publicOwners']")))
        try:
            owners = browser.find_element(By.CSS_SELECTOR, "div[data-bind*='publicOwners']").get_attribute('innerHTML')
        except:
            owners = ''
        try:
            mail_address = browser.find_element(By.XPATH,
                                                "//div[contains(@data-bind,'mailingAddress.formatted')]").get_attribute(
                'innerHTML')
        except:
            mail_address = ''
        try:
            site_addresses = browser.find_element(By.XPATH,
                                                  "//div[contains(@data-bind,'siteAddressFormatted')]").get_attribute(
                'innerHTML')
        except:
            site_addresses = ''
        try:
            parcel_id = browser.find_element(By.XPATH, "//div[contains(@data-bind,'parcelID')]").get_attribute(
                'innerHTML')
        except:
            parcel_id = ''

        try:
            taxing_district = browser.find_element(By.XPATH,
                                                   "//div[contains(@data-bind,'publicTaxDistrict')]").get_attribute(
                'innerHTML')
        except:
            taxing_district = ''

        try:
            exemptions = ''
            exemptions_list = browser.find_element(By.XPATH,
                                                   "//div[contains(@data-bind,'publicExemptions')]").get_attribute(
                'innerHTML')
            for ex in exemptions_list:
                exemptions = exemptions + str(ex)
        except:
            exemptions = ''

        try:
            property_use = browser.find_element(By.XPATH, "//div[contains(@data-bind,'publicUseCode')]").get_attribute(
                'innerHTML')
        except:
            property_use = ''

        try:
            total_acres = browser.find_element(By.XPATH, "//div[contains(@data-bind,'acreage')]").get_attribute(
                'innerHTML')
        except:
            total_acres = ''

        try:
            site_code = browser.find_element(By.XPATH, "//div[contains(@data-bind,'siteCodeDesc')]").get_attribute(
                'innerHTML')
        except:
            site_code = ''

        try:
            plat_book_page = browser.find_element(By.XPATH,
                                                  "//div[contains(@data-bind,'platBookPage')]')]").get_attribute(
                'innerHTML')
        except:
            plat_book_page = ''

        try:
            subdivision_name = browser.find_element(By.XPATH,
                                                    "//div[contains(@data-bind,'subdivisionName')]").get_attribute(
                'innerHTML')
        except:
            subdivision_name = ''

        try:
            land_description = browser.find_element(By.XPATH,
                                                    "//div[contains(@data-bind,'legalDescription')]").get_attribute(
                'innerHTML')
        except:
            land_description = ''

        owner_inforamtion = {
            "owners": owners,
            "mail_address": mail_address,
            "site_addresses": site_addresses,
            "parcel_id": parcel_id,
            "taxing_district": taxing_district,
            "exemptions": exemptions,
            "property_use": property_use,
            "total_acres": total_acres,
            "site_code": site_code,
            "plat_book/page": plat_book_page,
            "subdivision_name": subdivision_name,
            "land_description": land_description,
        }
        return owner_inforamtion

    def value_inforamtion(self, browser):

        table = browser.find_element(By.XPATH, "//table[contains(@data-bind,'valueSummary()')]")
        heads = table.find_elements(By.CSS_SELECTOR, 'thead tr th')
        dic = {}
        lst = []
        data = []
        for head in heads:
            head = str(head.text.strip().replace(' ', '_').lower().replace('#', 'ID')).replace('.', '')
            lst.append(head)
            dic[head] = ''
        print(lst)
        rows = table.find_elements(By.CSS_SELECTOR, 'tbody tr')
        print(len(rows))
        for row in rows:
            tds = row.find_elements(By.TAG_NAME, 'td')
            temp = deepcopy(dic)
            count = 0
            for td in tds:
                value = td.get_attribute('innerHTML').split('>')[-1].replace('\n', '').replace(':', '').strip()
                print(value)
                temp[lst[count]] = value
                count += 1
            data.append(temp)
        return data

    def sales_inforamtion(self, browser):
        table = browser.find_element(By.CSS_SELECTOR, "#divSales_Description table")
        heads = table.find_elements(By.CSS_SELECTOR, 'thead tr th')
        dic = {}
        lst = []
        data = []
        for head in heads:
            head = str(head.text.strip().replace(' ', '_').lower().replace('#', 'ID')).replace('.', '')
            lst.append(head)
            dic[head] = ''
        print(lst)
        rows = table.find_elements(By.CSS_SELECTOR, 'tbody tr')
        print(len(rows))
        for row in rows:
            tds = row.find_elements(By.TAG_NAME, 'td')
            temp = deepcopy(dic)
            count = 0
            for td in tds:
                try:
                    value = td.find_element(By.CSS_SELECTOR, 'a').get_attribute('innerHTML')
                except:
                    value = td.get_attribute('innerHTML').split('>')[-1].replace('\n', '').replace(':', '').strip()
                print(value)
                temp[lst[count]] = value
                count += 1
            data.append(temp)
        return data

    def building_inforamtion(self, browser):
        dic = {}
        try:
            table1 = browser.find_element(By.CSS_SELECTOR, "#divBldg_Materials table")
            dic['materials'] = {}
            rows = table1.find_elements(By.CSS_SELECTOR, 'tbody tr')
            for row in rows:
                kk = row.find_elements(By.CSS_SELECTOR, 'td')
                key = kk[0].get_attribute('innerHTML').replace(':', '').strip().replace(' ', '_').lower()
                value = kk[1].get_attribute('innerHTML').strip()
                dic['materials'][key] = value
        except:
            pass
        try:
            table1 = browser.find_element(By.CSS_SELECTOR, "#divBldg_Details table")
            dic['details'] = {}
            rows = table1.find_elements(By.CSS_SELECTOR, 'tbody tr')
            for row in rows:
                kk = row.find_elements(By.CSS_SELECTOR, 'td')
                try:
                    key = kk[0].find_element(By.CSS_SELECTOR, 'a').get_attribute('innerHTML')
                except:
                    key = kk[0].get_attribute('innerHTML')
                key = key.replace(':', '').strip().replace(' ', '_').replace('.', '').lower()
                value = kk[1].get_attribute('innerHTML').strip()
                dic['details'][key] = value
        except:
            pass
        try:
            tables = browser.find_elements(By.CSS_SELECTOR, "#divBldg_SubAreas table")
            if len(tables) > 0:
                dic['sub_areas'] = {}
            for table in tables:
                rows = table.find_elements(By.CSS_SELECTOR, 'tbody tr')
                for row in rows:
                    kk = row.find_elements(By.CSS_SELECTOR, 'td')
                    try:
                        key = kk[0].find_element(By.CSS_SELECTOR, 'a').get_attribute('innerHTML')
                    except:
                        key = kk[0].get_attribute('innerHTML')
                    key = key.replace(':', '').strip().replace(' ', '_').replace('.', '').lower()
                    value = kk[1].get_attribute('innerHTML').strip()
                    dic['sub_areas'][key] = value
        except:
            pass
        try:
            tables = browser.find_elements(By.CSS_SELECTOR, "#divBldg_ExtraFeatures  table")
            if len(tables) > 0:
                dic['extra_features'] = {}
            for table in tables:
                rows = table.find_elements(By.CSS_SELECTOR, 'tbody tr')
                for row in rows:
                    kk = row.find_elements(By.CSS_SELECTOR, 'td')
                    if len(kk) != 2:
                        continue
                    try:
                        key = kk[0].find_element(By.CSS_SELECTOR, 'a').get_attribute('innerHTML')
                    except:
                        key = kk[0].get_attribute('innerHTML')
                    key = key.replace(':', '').strip().replace(' ', '_').replace('.', '').lower()
                    value = kk[1].get_attribute('innerHTML').strip()
                    print('nimra', key, value)
                    dic['extra_features'][key] = value
        except:
            pass
        return dic

    def sketch_and_trim(self, browser):
        try:
            sk = browser.find_element(By.XPATH, "//a[@title='Building Sketches']").get_attribute('href')
        except:
            sk = ''
        try:
            trim = browser.find_element(By.XPATH, "//a[@title='TRIM Notice']").get_attribute('href')
        except:
            trim = ''
        return sk, trim

    def get_tax_link(self, browser):
        # Tax Collector
        try:
            link = browser.find_element(By.XPATH, "//a[@title='Tax Collector']").get_attribute('href')
        except:
            sk = ''
        return link

    def tax_collector_scrapy(self, response):
        # open_in_browser(response)
        response.meta['tax_collector']['amount_due'] = self.parse_amount_due(response)
        response.meta['tax_collector']['account_history'] = self.parse_anual_bill(response)
        parcel_link = self.base_url2 + response.css('div.parcel a::attr(href)').extract()[0]
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
        lt = []
        authority = ''
        tbodies = response.css('div.advalorem table.table-hover tbody')
        for auth in tbodies:
            # cname = tbody.xpath('@class').extract()[0]
            # if cname == 'taxing-authority':
            #     authority = str(tbody.css('th::text').extract()[0]).strip().lower().replace(' ', '_')
            #     lt[authority] = []
            # elif cname == 'taxing-authority':
            #     auth = tbody
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
            lt.append(
                {
                    "taxing_authority": taxing_authority,
                    "millage": milage,
                    "assessed": assessed,
                    "exemption": exemption,
                    "taxable": taxable,
                    "tax": tax
                })
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
            "alachua_county_tax_collector": miami_dade_county_tax_collector
        }
        return mailing_address

    def parse_combined_tax_collector(self, response):
        try:
            combined_taxes_and_assessments = str(response.css('div.message::text').extract()[0]).split(':')[-1].replace(
                '\n', '').strip()
        except:
            combined_taxes_and_assessments = ''
        return combined_taxes_and_assessments
