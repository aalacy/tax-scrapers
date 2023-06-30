from builtins import set
from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from time import sleep
from copy import deepcopy
import requests


class Pinellas:
    browser = ''

    def __init__(self):
        opt=Options()
        opt.headless=True
        self.browser = Firefox(options=opt)

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
            self.browser.get('https://www.pcpao.org/general.php?strap=' + id)
            btn = WebDriverWait(self.browser, 5).until(
                EC.visibility_of_element_located((By.XPATH, "//button[@name='buttonName']")))
            self.browser.execute_script('arguments[0].click()', btn)
            frame = WebDriverWait(self.browser, 5).until(
                EC.visibility_of_element_located((By.XPATH, "//frame[@name='bodyFrame']")))
            self.browser.switch_to.frame(frame)
            return True
        except:
            return None

    def parse_owner_and_property_info(self, ):
        dic = {}
        try:
            tds = self.browser.find_elements(By.XPATH,
                                             "//th[contains(text(),'Ownership/Mailing Address')]/parent::tr/following-sibling::tr/td")
            dic['name_and_address'] = tds[0].get_attribute('innerHTML').replace('<br>', ', ').replace("\t", '').strip()
            dic['site_address'] = tds[1].get_attribute('innerHTML').replace('<br>', ', ').replace("\t", '').strip()

        except:
            pass
        try:
            dic['property_use'] = self.browser.find_element(By.XPATH,
                                                            "//a[text()='Property Use']/parent::td").get_attribute(
                'innerHTML').split('</a>')[-1].replace('<br>', '').replace('\n', ' ').replace("&nbsp;", '').strip()
        except:
            pass
        try:
            dic['tax_district'] = self.browser.find_element(By.XPATH,
                                                            "//td[contains(text(),'Current Tax District')]").find_element(
                By.TAG_NAME, 'a').get_attribute('alt').strip()
        except:
            pass
        try:
            dic['total_heated_sf'] = self.browser.find_element(By.XPATH,
                                                               "//td[contains(text(),'Total Heated SF:')]").get_attribute(
                'innerHTML').split(':')[-1].strip()
        except:
            pass
        try:
            dic['total_gross_sf'] = self.browser.find_element(By.XPATH,
                                                              "//td[contains(text(),'Total Gross SF:')]").get_attribute(
                'innerHTML').split(':')[-1].strip()
        except:
            pass
        try:
            dic['total_gross_sf'] = self.browser.find_element(By.XPATH,
                                                              "//td[contains(text(),'Total Units:')]").get_attribute(
                'innerHTML').split(':')[-1].strip()
        except:
            pass
        try:
            dic['legal_description'] = self.browser.find_element(By.CSS_SELECTOR,
                                                                 "div#legal").get_attribute('innerHTML').strip()
        except:
            pass
        return dic

    def parse_exemption(self):
        rows = self.browser.find_elements(By.XPATH, "//th[text()='Exemption']/parent::tr/parent::tbody/tr")
        return self.parse_table_1(rows)

    def parcel_information(self):
        rows = self.browser.find_elements(By.XPATH, "//th[text()='Most Recent Recording']/parent::tr/parent::tbody/tr")
        headdict = {}
        data = []
        headlist = ['Most Recent Recording', 'Sales Comparison', 'Census Tract', 'Evacuation Zone', 'Flood Zone',
                    'Plat Book/Page']
        for h in headlist:
            headdict[h] = {}
        for row in rows[1:]:
            tds = row.find_elements(By.CSS_SELECTOR, 'td')
            count = 0
            temp = deepcopy(headdict)
            for td in tds:
                try:
                    value = td.find_element(By.TAG_NAME, 'a').get_attribute('href')
                except:
                    try:
                        value = td.find_element(By.TAG_NAME, 'div').get_attribute('innerHTML')
                    except:
                        value = td.get_attribute('innerHTML')
                temp[headlist[count]] = value.replace('\n', '').strip()
                count += 1
            data.append(temp)
        return data

    def parcel_Interim_Value_Information(self):
        rows = self.browser.find_elements(By.XPATH,
                                          "//a[contains(text(),'Interim Value Information ')]/parent::u/parent::th/parent::tr/parent::tbody/tr")
        try:
            heads = rows[1].find_elements(By.CSS_SELECTOR, 'td')
        except:
            return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            try:
                h = h.find_element(By.TAG_NAME, 'a').get_attribute('innerHTML')
            except:
                h = h.get_attribute('innerHTML')
            h = h.replace('&nbsp;', '')
            headlist.append(h)
            headdict[h] = ''
        for row in rows[2:-1]:
            tds = row.find_elements(By.CSS_SELECTOR, 'td')
            count = 0
            temp = deepcopy(headdict)
            if len(tds) != len(headlist):
                continue
            for td in tds:
                try:
                    value = td.find_element(By.TAG_NAME, 'b').get_attribute('innerHTML')
                except:
                    value = td.get_attribute('innerHTML')
                temp[headlist[count]] = value.replace('\n', '').strip()
                count += 1
            try:
                del temp['']
            except:
                pass
            data.append(temp)
        return data

    def parcel_Value_History_as_Certified(self):
        rows = self.browser.find_elements(By.CSS_SELECTOR,
                                          "div#valhist table tr")
        try:
            heads = rows[0].find_elements(By.CSS_SELECTOR, 'td')
        except:
            return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            try:
                h = h.find_element(By.TAG_NAME, 'a').get_attribute('innerHTML')
            except:
                h = h.get_attribute('innerHTML')
            h = h.replace('&nbsp;', '')
            headlist.append(h)
            headdict[h] = ''
        for row in rows[1:]:
            tds = row.find_elements(By.CSS_SELECTOR, 'td')
            count = 0
            temp = deepcopy(headdict)
            if len(tds) != len(headlist):
                continue
            for td in tds:
                value = td.get_attribute('innerHTML')
                temp[headlist[count]] = value.replace('\n', '').strip()
                count += 1
            try:
                del temp['']
            except:
                pass
            data.append(temp)
        return data

    def parcel_ranked_sales(self):
        rows = self.browser.find_elements(By.XPATH,
                                          "//th[contains(text(),'Ranked Sales ')]/parent::tr/following-sibling::tr")
        try:
            heads = rows[0].find_elements(By.CSS_SELECTOR, 'th')
        except:
            return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            try:
                h = h.find_element(By.TAG_NAME, 'a').get_attribute('innerHTML')
            except:
                h = h.get_attribute('innerHTML')
            h = h.replace('&nbsp;', '')
            headlist.append(h)
            headdict[h] = ''
        for row in rows[1:]:
            tds = row.find_elements(By.CSS_SELECTOR, 'td')
            count = 0
            temp = deepcopy(headdict)
            if len(tds) != len(headlist):
                continue
            for td in tds:

                try:
                    value = td.find_element(By.TAG_NAME, 'a').get_attribute('href')
                except:
                    value = td.get_attribute('innerHTML')
                temp[headlist[count]] = value.replace('\n', '').strip()
                count += 1
            try:
                del temp['']
            except:
                pass
            data.append(temp)
        return data

    def parse_land_use(self):
        rows = self.browser.find_elements(By.XPATH,
                                          "//a[text()='Land Use']/parent::b/parent::th/parent::tr/parent::tbody/tr")
        try:
            heads = rows[0].find_elements(By.CSS_SELECTOR, 'th')
        except:
            return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            try:
                h = h.find_element(By.TAG_NAME, 'a').get_attribute('innerHTML')
            except:
                try:
                    h = h.find_element(By.TAG_NAME, 'b').get_attribute('innerHTML')
                except:
                    h = h.get_attribute('innerHTML')

            h = h.replace('&nbsp;', '')
            headlist.append(h)
            headdict[h] = ''
        for row in rows[1:]:
            tds = row.find_elements(By.CSS_SELECTOR, 'td')
            count = 0
            temp = deepcopy(headdict)
            if len(tds) != len(headlist):
                continue
            for td in tds:
                try:
                    value = td.find_element(By.TAG_NAME, 'a').get_attribute('href')
                except:
                    value = td.get_attribute('innerHTML')
                temp[headlist[count]] = value.replace('\n', '').strip()
                count += 1
            try:
                del temp['']
            except:
                pass
            data.append(temp)
        return data

    def extra_features(self):
        rows = self.browser.find_elements(By.CSS_SELECTOR, "div#xfsb tr")
        try:
            heads = rows[0].find_elements(By.CSS_SELECTOR, 'td')
        except:
            return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            h = h.get_attribute('innerHTML')
            h = h.replace('&nbsp;', '')
            headlist.append(h)
            headdict[h] = ''
        for row in rows[1:]:
            tds = row.find_elements(By.CSS_SELECTOR, 'td')
            count = 0
            temp = deepcopy(headdict)
            if len(tds) != len(headlist):
                continue
            for td in tds:
                value = td.get_attribute('innerHTML')
                temp[headlist[count]] = value.replace('\n', '').strip()
                count += 1
            try:
                del temp['']
            except:
                pass
            data.append(temp)
        return data

    def permit_number(self):
        rows = self.browser.find_elements(By.XPATH, "//th[text()='Permit Number']/parent::tr/parent::tbody/tr")
        try:
            heads = rows[0].find_elements(By.CSS_SELECTOR, 'th')
        except:
            return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            h = h.get_attribute('innerHTML')
            h = h.replace('&nbsp;', '')
            headlist.append(h)
            headdict[h] = ''
        for row in rows[1:]:
            tds = row.find_elements(By.CSS_SELECTOR, 'td')
            count = 0
            temp = deepcopy(headdict)
            if len(tds) != len(headlist):
                continue
            for td in tds:
                try:
                    value = td.find_element(By.TAG_NAME, 'a').get_attribute('href')
                except:
                    value = td.get_attribute('innerHTML')
                value = value.replace('&nbsp;', ' ')
                temp[headlist[count]] = value.replace('\n', '').strip()
                count += 1
            try:
                del temp['']
            except:
                pass
            data.append(temp)
        return data

    def parse_table_1(self, rows):
        d = {}
        try:
            heads = rows[0].find_elements(By.CSS_SELECTOR, 'th')
        except:
            return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            try:
                h = h.find_element(By.TAG_NAME, 'b').get_attribute('innerHTML').split('<br>')[0]
            except:
                try:
                    h = h.find_element(By.TAG_NAME, 'a').get_attribute('innerHTML').split('<br>')[0]
                except:
                    h = h.get_attribute('innerHTML')

            headlist.append(h)
            headdict[h] = ''
        for row in rows[1:]:
            tds = row.find_elements(By.CSS_SELECTOR, 'td')
            count = 0
            temp = deepcopy(headdict)
            if len(tds) != len(headlist):
                continue
            for td in tds:
                value = td.get_attribute('innerHTML')
                temp[headlist[count]] = value
                count += 1
            data.append(temp)
        return data

    def parse_buildings(self, ):
        tables = self.browser.find_elements(By.XPATH,
                                            "//td[contains(text(),'Building Type')]/parent::tr/parent::tbody/parent::table/parent::span")
        buildings = self.browser.find_elements(By.XPATH, "//span[contains(@id,'bb') and not(contains(@id,'title'))]")
        data = []
        for building in buildings:
            d = {}
            tables = building.find_elements(By.XPATH, './table')
            try:
                rows = tables[0].find_elements(By.XPATH, './tbody/tr')
                d['structural_elements'] = self.prase_building_structural_elements(rows)
            except:
                pass
            try:
                rows = tables[1].find_elements(By.XPATH, './tbody/tr')
                d['sub_area_information'] = self.prase_building_sub_area(rows)
            except:
                pass
            try:
                d['sketch'] = building.find_element(By.TAG_NAME, 'img').get_attribute('href')
            except:
                pass
            try:
                d['compact_property_record_card_pdf'] = building.find_element(By.XPATH,
                                                                              ".//a[contains(text(),'Compact Property Record Card')]").get_attribute(
                    'href')
            except:
                pass
            data.append(d)
        return data

    def prase_building_structural_elements(self, rows):
        d = {}
        for row in rows:
            td = row.find_element(By.TAG_NAME, 'td')
            value = td.find_element(By.TAG_NAME, 'b').get_attribute('innerHTML')
            key = td.get_attribute('innerHTML').split("<b>")[0].replace('\n', '').strip()
            d[key] = value
        return d

    def prase_building_sub_area(self, rows):
        try:
            heads = rows[1].find_elements(By.CSS_SELECTOR, 'th')
        except:
            return []
        headlist = []
        headdict = {}
        data = []
        for h in heads:
            try:
                h = h.find_element(By.TAG_NAME, 'a').get_attribute('innerHTML')
            except:
                h = h.get_attribute('innerHTML')
            h = h.replace('&nbsp;', '')
            headlist.append(h)
            headdict[h] = ''
        for row in rows[2:-1]:
            tds = row.find_elements(By.CSS_SELECTOR, 'td')
            count = 0
            temp = deepcopy(headdict)
            if len(tds) != len(headlist):
                continue
            for td in tds:
                try:
                    value = td.find_element(By.TAG_NAME, 'a').get_attribute('innerHTML')
                except:
                    value = td.get_attribute('innerHTML')
                value = value.replace('&nbsp;', ' ')
                temp[headlist[count]] = value.replace('\n', '').strip()
                count += 1
            try:
                del temp['']
            except:
                pass
            data.append(temp)
        return data

    def close_browser(self):
        try:
            self.browser.quit()
        except:
            pass

    def parse_trim(self):
        try:
            return self.browser.find_element(By.XPATH,
                                             "//a[contains(text(),'Property Taxes (TRIM Notice)')]").get_attribute(
                'href')
        except:
            return ''

    def tax_link(self):
        try:
            return self.browser.find_element(By.XPATH, "//a[contains(text(),'Tax Bill')]").get_attribute(
                'href')
        except:
            return ''

    def RUN(self, id):
        try:
            dic = {}
            self.search(id)
            WebDriverWait(self.browser, 5).until(
                EC.visibility_of_element_located((By.XPATH, "//a[text()='Compact Property Record Card']/parent::b/parent::font/parent::td/font[1]")))
            self.id = self.browser.find_element(By.XPATH,
                                                "//a[text()='Compact Property Record Card']/parent::b/parent::font/parent::td/font[1]").get_attribute(
                'innerHTML').replace('&nbsp;', '').strip()
            dic = {'folio': self.id, 'url': self.browser.current_url}
            dic['owner_and_property_info'] = self.parse_owner_and_property_info()
            dic['exemptions'] = self.parse_exemption()
            dic['parcel_information'] = self.parcel_information()
            dic['interm_value_information'] = self.parcel_Interim_Value_Information()
            dic['Value_History_as_Certified'] = self.parcel_Value_History_as_Certified()
            dic['ranked_sales'] = self.parcel_ranked_sales()
            dic['land_use'] = self.parse_land_use()
            dic['buildings'] = self.parse_buildings()
            dic['extra_features'] = self.extra_features()
            dic['permit_number'] = self.permit_number()
            dic['trim_pdf'] = self.parse_trim()
            dic['tax_collector_link'] = self.tax_link()
            # print(dic)
            return dic
        except:
            return {}

#
# input_file = open('ids.txt', 'r', encoding='utf-8')
# lines = input_file.readlines()
# c = 1
# for line in lines:
#     obj = Pinellas()
#     # line = '152711000004101900'
#     print('-------------------------------------------')
#     print('----', c, '=', line, '----')
#     print('-------------------------------------------')
#     line = line.replace(' ', '').replace('\n', '').strip()
#     obj.RUN(line)
#     obj.close_browser()
#     c += 1
#     # break
