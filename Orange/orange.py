from builtins import set
from selenium.webdriver.firefox.options import Options
from selenium.webdriver import Firefox, FirefoxProfile
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from time import sleep
from copy import deepcopy
import requests
import pdb
import os
import boto3
from pymongo import MongoClient
from clint.textui import progress
from uuid import uuid1
import platform

mongocli = MongoClient('mongodb+srv://hbg:**cluster0.uetxd.mongodb.net/:27017')

class Orange:
    browser = ''
    basedir = os.path.abspath(os.path.dirname(__file__))

    AWS_BUCKET = 'aws-sdc-usage'
    AWS_ACCESS_KEY_ID = '**'
    AWS_SECRET_ACCESS_KEY = '**Ctyn15kykdT'
    AWS_REGION = 'us-west-1'
    data = []

    def __init__(self):
        if 'Microsoft' in platform.release():
            self.path = f"{self.basedir}/geckodriver.exe"
        else:
            self.path = f"{self.basedir}/geckodriver"

        options = FirefoxOptions()
        profile = FirefoxProfile()
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--headless')
        options.AddArgument("--disable-gpu");
        options.AddArgument("--disable-crash-reporter");
        options.AddArgument("--disable-extensions");
        options.AddArgument("--disable-in-process-stack-traces");
        options.AddArgument("--disable-logging");
        options.AddArgument("--disable-dev-shm-usage");
        options.AddArgument("--log-level=3");
        options.AddArgument("--output=/dev/null");
        profile.set_preference("permissions.default.image", 2)
        self.browser = Firefox(executable_path=self.path, options=options, firefox_profile=profile)

        self.s3_client = boto3.client('s3', aws_access_key_id=self.AWS_ACCESS_KEY_ID, region_name=self.AWS_REGION, aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY)
        self.session = requests.Session()

    def clean(self, s):
        s = s.split(
            '<a')[0].split('<font')[0].replace('\t', '').replace('\n', '').replace('<b>', '') \
            .replace('</b>', '').replace('<br>', '\n').replace("&nbsp;", ' ').strip()
        return s

    def clean_key(self, s):
        s = s.split(
            '<a')[0].split('<font')[0].replace('\t', '').replace('\n', '_').replace('<b>', '') \
            .replace('</b>', '').replace("&nbsp;", '_').replace('(', '').replace(')', '').replace(
            "<br>", '_').strip().replace(" ", '_')
        return s

    def search(self, id):
        try:
            self.browser.get('http://www.ocpafl.org/Searches/ParcelSearch.aspx/PID/' + id)
            btn = WebDriverWait(self.browser, 5).until(
                EC.visibility_of_element_located((By.XPATH, "//input[@id='popup_ok']")))
            self.browser.execute_script('arguments[0].click()', btn)
            btn = WebDriverWait(self.browser, 5).until(
                EC.visibility_of_element_located((By.XPATH, "//div[contains(@id,'_PopupPanel1_PopupBody')]/img")))
            self.browser.execute_script('arguments[0].click()', btn)

            # myElem = WebDriverWait(self.browser, 5).until(
            #     EC.visibility_of_element_located(
            #         (By.XPATH, "//input[contains(@id,'ParcelIDSearch1_ctl00_FullParcel')]")))
            # for _ in range(20):
            #     myElem.send_keys(Keys.ARROW_LEFT)
            # myElem.send_keys(id)
            # btn = self.browser.find_element(By.XPATH, "//input[contains(@id,'ParcelIDSearch1_ctl00_ActionButton1')]")
            # self.browser.execute_script('arguments[0].click()', btn)
            WebDriverWait(self.browser, 20).until(
                EC.presence_of_element_located((By.XPATH, "//div[@class='DetailsSummary_TitleContainer']")))
            return True
        except:
            return None

    def parse_owner_and_property_info(self, ):
        dic = {}
        try:
            dic['name'] = self.browser.find_element(By.XPATH,
                                                    "//legend[contains(text(),'Name(s)')]/following-sibling::div").get_attribute(
                'innerHTML').replace('\n', '').replace("\t", '').strip()

        except:
            pass
        try:
            dic['property_name'] = self.browser.find_element(By.XPATH,
                                                             "//fieldset[contains(@id,'ParcelSummary1_PropertyNameFieldSet')]").get_attribute(
                'innerHTML').split('legend>')[-1].split('<a')[0].replace('<br>', '').replace('\n',
                                                                                             ' ').replace(
                "&nbsp;", '').strip()
            if 'Click information icon to contribute' in dic['property_name']:
                dic['property_name'] = ''
        except:
            pass
        try:
            dic['mailing_address'] = self.browser.find_element(By.XPATH,
                                                               "//legend[contains(text(),'Mailing Address On ')]/parent::fieldset") \
                .get_attribute('innerHTML').split('legend>')[-1].split('<a')[0].replace('<br>', '').replace('\n',
                                                                                                            ' ').replace(
                "&nbsp;", '').strip()
        except:
            pass
        try:
            dic['physical_street_address'] = self.browser.find_element(By.XPATH,
                                                                       "//legend[contains(text(),'Physical Street Address')]/parent::fieldset") \
                .get_attribute('innerHTML').split('legend>')[-1].replace('<br>', '').replace('\n', ' ') \
                .replace("&nbsp;", '').strip()
        except:
            pass
        try:
            dic['postal_city_and_zip_code'] = self.browser.find_element(By.XPATH,
                                                                        "//legend[contains(text(),'Postal City and Zipcode')]/parent::fieldset") \
                .get_attribute('innerHTML').split('legend>')[-1].replace('<br>', '').replace('\n', ' ') \
                .replace("&nbsp;", '').strip()
        except:
            pass
        try:
            dic['property_use'] = self.browser.find_element(By.XPATH,
                                                            "//legend[contains(text(),'Property Use')]/parent::fieldset") \
                .get_attribute('innerHTML').split('legend>')[-1].replace('<br>', '').replace('\n', ' ') \
                .replace("&nbsp;", '').strip()
        except:
            pass
        try:
            dic['municipality'] = self.browser.find_element(By.XPATH,
                                                            "//legend[contains(text(),'Municipality')]/parent::fieldset") \
                .get_attribute('innerHTML').split('legend>')[-1].replace('<br>', '').replace('\n', ' ') \
                .replace("&nbsp;", '').strip()
        except:
            pass

        return dic

    def property_features(self):
        d = {}
        d['description'] = self.browser.find_element(By.XPATH,
                                                     "//legend[contains(.,'Property Description')]/parent::fieldset").get_attribute(
            'innerHTML').split('legend>')[-1].split('<a')[0].replace('<br>', '').replace('\n',
                                                                                         ' ').replace(
            "&nbsp;", '').strip()

        d['total_area_ft'] = self.browser.find_element(By.XPATH,
                                                       "//td[contains(@id,'_PropertySzInSqFt')]").get_attribute(
            'innerHTML')
        d['total_area_acr'] = self.browser.find_element(By.XPATH,
                                                        "//td[contains(@id,'_PropertySzInAcres')]").get_attribute(
            'innerHTML')
        d['land_info'] = self.parse_land_info()
        d['buildings'] = self.parse_building_info()
        d['extra_features'] = self.parse_extra_features()
        return d

    def parse_extra_features(self, ):
        rows = self.browser.find_elements(By.XPATH, "//table[contains(@id,'_PropertyFeatures1_xFOBGrid')]/tbody/tr")
        return self.parse_table_type1(rows)

    def parse_land_info(self, ):
        rows = self.browser.find_elements(By.XPATH, "//table[contains(@id,'_PropertyFeatures1_LandGrid')]/tbody/tr")
        return self.parse_table_type1(rows)

    def parse_building_info(self):
        rows = self.browser.find_elements(By.XPATH,
                                          "//table[contains(@id,'_PropertyFeatures1_BuildingGrid')]//table//tr")
        d = {}
        for row in rows:
            key = row.find_element(By.CSS_SELECTOR, 'th').get_attribute('innerHTML').replace('\n', '').strip()
            value = row.find_element(By.CSS_SELECTOR, 'td').get_attribute('innerHTML').replace('\n', '').strip()
            d[key] = value
        return d

    def parse_table_type1(self, rows):
        try:
            heads = rows[0].find_elements(By.CSS_SELECTOR, 'th')
        except:
            return []
        rows = rows[1:]
        dic = {}
        lst = []
        data = []
        for head in heads:
            try:
                head = head.find_element(By.CSS_SELECTOR, 'span').get_attribute('innerHTML')
            except:
                head = head.get_attribute('innerHTML')
            head = head.split("<a")[0].replace('\n', '').strip().replace('/', '_').replace(' ', '_').lower()
            lst.append(head)
            dic[head] = {}
        for row in rows:
            values = row.find_elements(By.CSS_SELECTOR, 'td')
            if len(values) != len(lst):
                continue
            tt = deepcopy(dic)
            count = 0
            for value in values:
                try:
                    value = value.find_element(By.CSS_SELECTOR, 'a').get_attribute('innerHTML')
                except:
                    try:
                        value = value.find_element(By.CSS_SELECTOR, 'span.LabelPart').get_attribute('innerHTML')
                    except:
                        try:
                            value = value.find_element(By.CSS_SELECTOR, 'span').get_attribute('innerHTML')
                        except:
                            value = value.get_attribute('innerHTML')
                tt[lst[count]] = value.replace('<br>', ',').replace('&nbsp;', '').replace('\n', '').strip()
                count += 1
            data.append(tt)
        return data

    def parse_taxes(self):
        d = {}
        d['ad_valorem_assessments'] = self.ad_valorem_assessment()
        d['non_ad_valorem_assessments'] = self.non_nad_valorem_assessment()
        try:
            d['total_tax'] = self.browser.find_element(By.XPATH,
                                                       "//span[contains(@id,'_ValueTax_ValuesTaxes1_NATotalTax')]").get_attribute(
                'innerHTML')
        except:
            d['total_tax'] = ''
        try:
            d['tax_break_down'] = self.browser.find_element(By.CSS_SELECTOR,
                                                            'div.ChartImageContainer img').get_attribute(
                'src')
        except:
            d['tax_break_down'] = ''
        return d

    def ad_valorem_assessment(self, ):
        rows = self.browser.find_elements(By.XPATH,
                                          "//table[contains(@id,'_ValueTax_ValuesTaxes1_Grid1')]/tbody/tr")
        return self.parse_table_type1(rows[:-1])

    def non_nad_valorem_assessment(self, ):
        rows = self.browser.find_elements(By.XPATH,
                                          "//table[contains(@id,'_ValuesTaxes1_NonAdValoremTaxes1_Grid1')]/tbody/tr")
        return self.parse_table_type1(rows[:-1])

    def parse_sales_history(self):
        rows = self.browser.find_elements(By.XPATH,
                                          "//table[contains(@id,'_SaleAnalysis_SalesAnalysis1_Grid1')]/tbody/tr")
        return self.parse_table_type1(rows)

    def parse_pictures(self):
        imgs = self.browser.find_elements(By.XPATH, "//td[@class='ImageD']/a/img[@alt='Parcel Photo']")
        l = []
        for i in imgs:
            l.append(i.get_attribute('src'))
        return l

    def parse_trim(self):
        try:
            link = self.browser.find_element(By.XPATH, "//a[@target='_trim']").get_attribute('href')
            res = requests.post('https://trimnet.ocpafl.org/Default.aspx/WSGetPDF', json={'p_page': link})
            return res.json()['d']
        except:
            return ''

    def parse_tax_collector(self):
        link = self.browser.find_element(By.XPATH,
                                         "//a[contains(@onclick,'Tax Collector - View Taxes')]").get_attribute(
            'href')
        self.browser.get(link)
        WebDriverWait(self.browser, 10).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "table#mainContent_tableUnpaidTaxes")))
        d = {}
        d['summary'] = self.summary()
        d['current_taxes_and_unpaid_delinquent_warrants'] = self.parse_table_1()
        d['unpaid_real_estate_certificates'] = self.parse_table_2('table#mainContent_tblUnpaidCerts tr')
        d['other_real_estate_certificates'] = self.parse_table_3('table#mainContent_tblOtherCerts tr')
        return d

    def summary(self):
        d = {}
        try:
            d['parcel_id'] = self.browser.find_element(By.CSS_SELECTOR,
                                                       'td#mainContent_cellDisplayIdentifier').get_attribute(
                'innerHTML')
        except:
            d['parcel_id'] = ''
        try:
            d['owner_and_address'] = self.browser.find_element(By.CSS_SELECTOR,
                                                               'td#mainContent_cellOwnerInformation').get_attribute(
                'innerHTML').replace('<br>', ', ').replace('\n', '').strip()
        except:
            d['owner_and_address'] = ''
        try:
            d['date'] = self.browser.find_element(By.XPATH,
                                                  "//strong[.='Date:']/parent::td").get_attribute(
                'innerHTML').split('strong>')[-1].replace('<br>', ', ').replace('\n', '').strip()
        except:
            d['date'] = ''
        try:
            d['tax_year'] = self.browser.find_element(By.CSS_SELECTOR,
                                                      "td#mainContent_cellTaxYearInfo u").get_attribute(
                'innerHTML').replace('\n', '').strip()
        except:
            d['tax_year'] = ''
        try:
            d['legal_description'] = self.browser.find_element(By.XPATH,
                                                               "//b[.='Legal Description:']/parent::td/following-sibling::td/b").get_attribute(
                'innerHTML').replace('\n', '').strip()
        except:
            d['legal_description'] = ''
        try:
            d['total_assessed_value'] = self.browser.find_element(By.CSS_SELECTOR,
                                                                  "td#mainContent_cellAssessedValue").get_attribute(
                'innerHTML').replace('\n', '').strip()
        except:
            d['total_assessed_value'] = ''
        try:
            d['taxable_value'] = self.browser.find_element(By.CSS_SELECTOR,
                                                           "td#mainContent_cellTaxableValue").get_attribute(
                'innerHTML').replace('\n', '').strip()
        except:
            d['taxable_value'] = ''
        try:
            d['location_address'] = self.browser.find_element(By.XPATH,
                                                              "//b[.='Location Address:']/parent::td/following-sibling::td").get_attribute(
                'innerHTML').replace('\n', '').strip()
        except:
            d['location_address'] = ''
        try:
            d['gross_tax_amount'] = self.browser.find_element(By.CSS_SELECTOR,
                                                              "td#mainContent_cellGrossTaxAmount").get_attribute(
                'innerHTML').replace('\n', '').strip()
        except:
            d['gross_tax_amount'] = ''
        try:
            d['millage_code'] = self.browser.find_element(By.CSS_SELECTOR,
                                                          "td#mainContent_cellMillageCode").get_attribute(
                'innerHTML').replace('\n', '').strip()
        except:
            d['millage_code'] = ''
        return d

    def parse_table_1(self, ):
        rows = self.browser.find_elements(By.CSS_SELECTOR, 'table#mainContent_tableUnpaidTaxes tr')
        try:
            heads = rows[0].find_elements(By.CSS_SELECTOR, 'strong')[:-2]
        except:
            return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            h = h.get_attribute('innerHTML')
            headlist.append(h)
            headdict[h] = ''
        for row in rows[1:]:
            tds = row.find_elements(By.CSS_SELECTOR, 'td')[:-2]
            count = 0
            temp = deepcopy(headdict)
            for td in tds:
                try:
                    value = td.find_element(By.CSS_SELECTOR, 'a').get_attribute('href')
                except:
                    try:
                        value = td.find_element(By.CSS_SELECTOR, 'span').get_attribute('innerHTML')
                    except:
                        try:
                            value = td.find_element(By.CSS_SELECTOR, 'center').get_attribute('innerHTML')
                        except:
                            value = td.get_attribute('innerHTML')
                temp[headlist[count]] = value
                count += 1
            data.append(temp)
        return data

    def parse_table_2(self, selector):
        rows = self.browser.find_elements(By.CSS_SELECTOR, selector)
        try:
            heads = rows[0].find_elements(By.CSS_SELECTOR, 'strong')[:-1]
        except:
            return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            h = h.get_attribute('innerHTML')
            headlist.append(h)
            headdict[h] = ''
        for row in rows[1:]:
            tds = row.find_elements(By.CSS_SELECTOR, 'td')[:-1]
            count = 0
            temp = deepcopy(headdict)
            for td in tds:
                try:
                    value = td.find_element(By.TAG_NAME, 'center').get_attribute('innerHTML')
                except:
                    try:
                        value = td.find_element(By.TAG_NAME, 'span').get_attribute('innerHTML')
                    except:
                        value = td.get_attribute('innerHTML')
                temp[headlist[count]] = value
                count += 1
            data.append(temp)
        return data

    def parse_table_3(self, selector):
        rows = self.browser.find_elements(By.CSS_SELECTOR, selector)
        try:
            heads = rows[0].find_elements(By.CSS_SELECTOR, 'strong')
        except:
            return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            h = h.get_attribute('innerHTML')
            headlist.append(h)
            headdict[h] = ''
        for row in rows[1:]:
            tds = row.find_elements(By.CSS_SELECTOR, 'td')
            count = 0
            temp = deepcopy(headdict)
            for td in tds:
                try:
                    value = td.find_element(By.TAG_NAME, 'center').get_attribute('innerHTML')
                except:
                    try:
                        value = td.find_element(By.TAG_NAME, 'span').get_attribute('innerHTML')
                    except:
                        value = td.get_attribute('innerHTML')
                temp[headlist[count]] = value
                count += 1
            data.append(temp)
        return data

    def close_browser(self):
        try:
            self.browser.close()
        except:
            pass

    def parse_map(self):
        try:
            link = self.browser.find_element(By.XPATH, "//a[contains(@onclick,'GIS Parcel')]").get_attribute('href')
            return link
        except:
            return ''

    def _download_file(self, link, file_name=None):
        print(f'=== _download_file {link}')
        vid = self.session.get(link, stream=True)
        try:
            if vid.status_code == 200:
                temp_filename = f'/tmp/orange_{uuid1()}'
                with open(temp_filename, "wb") as f:
                    if vid.headers.get('content-length'):
                        total_size = int(vid.headers.get('content-length'))
                        for chunk in progress.bar(vid.iter_content(chunk_size=1024),
                                                  expected_size=total_size/1024 + 1):
                            if chunk:
                                f.write(chunk)
                                f.flush()
                    else:
                        f.write(vid.content)

            name = os.path.basename(link)
            if file_name:
                name = file_name
            self._upload_dump_to_s3(temp_filename, name)
            os.remove(temp_filename)

            return name
        except Exception as E:
            print(str(E))

        return ''

    def _upload_dump_to_s3(self, tmp_name, name):
        print(f'== upload {tmp_name}')
        bucketkey = f'Orange/Tax_Properties/{name}'
        self.s3_client.upload_file(
            Filename=tmp_name,
            Bucket=self.AWS_BUCKET,
            Key=bucketkey)

    def save_to_mongo_bulk(self):
        col = mongocli['Orange_County']['property']
        try:
            col.insert_many(self.data)
        except Exception as E:
            logger.warning(str(E))

    def _save_to_mongo(self, data={}):
        col = mongocli['Orange_County']['property']
        try:
            col.insert_one(data).inserted_id
        except Exception as E:
            logger.warning(data)
        return True

    def RUN(self):
        input_file = open('orange.txt', 'r', encoding='utf-8')
        lines = input_file.readlines()
        c = 1
        for line in lines:
            try:
                line = line.replace('\n', '').strip()
                dd = self._run(line)
                self._download_file(dd['trim_pdf'])
                self._save_to_mongo(data=dd)
                self.data.append(dd)
                print(c,"Going for --------",line,'--------------------------')
                c += 1
            except:
                continue

    def _run(self, id):
        self.search(id)
        self.id = self.browser.find_elements(By.XPATH, '//div[@class="DetailsSummary_TitleContainer"]/span')[1].text
        self.id = self.id.replace('>', '').replace('<', '').strip()
        dic = {'folio': self.id, 'url': self.browser.current_url}
        dic['owner_and_property_info'] = self.parse_owner_and_property_info()
        dic['property_features'] = self.property_features()
        dic['taxes'] = self.parse_taxes()
        dic['sales_history'] = self.parse_sales_history()
        dic['pictures'] = self.parse_pictures()
        dic['trim_pdf'] = self.parse_trim()
        dic['map_link'] = self.parse_map()
        dic['tax_collector'] = self.parse_tax_collector()

        return dic

if __name__ == '__main__':
    obj = Orange()
    obj.RUN()
    obj.close_browser()
