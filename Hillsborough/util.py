import boto3
from pymongo import MongoClient
from clint.textui import progress
import csv
import random
import pdb

from logger import logger

AWS_BUCKET = 'aws-sdc-usage'
AWS_ACCESS_KEY_ID = '**'
AWS_SECRET_ACCESS_KEY = '**Ctyn15kykdT'
AWS_REGION = 'us-west-1'

s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, region_name=AWS_REGION, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

mongocli = MongoClient('mongodb+srv://hbg:**cluster0.uetxd.mongodb.net/:27017')

class Util:

	def __init__(self, county='', city=''):
		self.county = county
		self.city = city
		collection = f'{county}_County'
		self.db = mongocli[collection]

	def _upload_dump_to_s3(self, tmp_name, name):
		logger.info(f"[{city}] Starting upload to Object Storage")
		bucketkey = f'Permits/{self.city}/{name}'
		s3_client.upload_file(
			Filename=tmp_name,
			Bucket=AWS_BUCKET,
			Key=bucketkey)

		logger.info(f"[{city}] Uploaded file to s3 {name}")

	def _save_to_mongo(self, permit_type="property", data={}):
		col = self.db[permit_type]
		try:
			col.insert_one(data).inserted_id
		except Exception as E:
			logger.warning(data)
		return True

	def _save_to_mongo_bulk(self, permit_type="permits", data=[]):
		col = self.db[permit_type]
		try:
			col.insert_many(data)
		except Exception as E:
			logger.warning(str(E))

	def _download_pdf(self, data):
		pdf1=data['property_record_card_pdf']
		all_pdfs=[]
		for pdf in pdf1:
			all_pdfs.append(pdf)
		print("PDFS",len(all_pdfs))
		print(all_pdfs)
		folio = data['folio']
		if len(all_pdfs)!=0:
			self._download_and_upload(all_pdfs, folio, s3, bucket_name)
			
	def _download_and_upload(self,pdfs, folio, bucket, bucket_name):
		if len(pdfs)=='0':
			return
		path = os.path.join('/tmp', folio)
		try:
			os.mkdir(path)
		except:
			pass

		for pdf in pdfs:
			pdf = pdf.strip()
			if 'parcel' in pdf:
				name = str(pdf).split('parcels/')[1].replace('/print', '.pdf').replace('/', '_').replace('%', '-')
			else:
				name = pdf.rsplit('/', 1)[-1]
			filename = os.path.join('/tmp/{}'.format(folio), name)
			if not os.path.isfile(filename):
				print('Downloading: ' + filename)
				try:
					r = requests.get(pdf, stream=True)
					if r.ok:
						filename = filename
						print("saving to", os.path.abspath(filename))
						with open(filename, 'wb') as f:
							for chunk in r.iter_content(chunk_size=1024 * 8):
								if chunk:
									f.write(chunk)
									f.flush()
									os.fsync(f.fileno())
					else:  # HTTP status code 4XX/5XX
						print("Download failed: status code {}\n{}".format(r.status_code, r.pdf))
				except Exception as inst:
					print(inst)
					print(' Encountered unknown error. Continuing.')
				transfer = S3Transfer(bucket)
				transfer.ALLOWED_UPLOAD_ARGS.append('Tagging')
				arr = os.listdir('/tmp/{}'.format(folio))
				print(arr)
				for file in arr:
					transfer.upload_file('/tmp/{}/'.format(folio) + file, bucket_name,
										 '{}/{}/{}'.format(county, folio, file))

					os.remove('/tmp/{}/'.format(folio) + file)

	def _get_viewstategenerator(self, response):
		return response.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value').get()

	def _get_viewstate(self, response):
		return response.xpath('//input[@id="__VIEWSTATE"]/@value').get()

	def _get_previouspage(self, response):
		return response.xpath('//input[@id="__PREVIOUSPAGE"]/@value').get()

	def _get_eventvalidation(self, response):
		return response.xpath('//input[@id="__EVENTVALIDATION"]/@value').get()

	def _get_eventargument(self, response):
		return response.xpath('//input[@id="__EVENTARGUMENT"]/@value').get()

	def _get_ACA_CS_FIELD(self, response):
		return response.xpath('//input[@id="ACA_CS_FIELD"]/@value').get()

	def _get__LASTFOCUS(self, response):
		return response.xpath('//input[@id="__LASTFOCUS"]/@value').get()

	def _get__AjaxControlToolkitCalendarCssLoaded(self, response):
		return response.xpath('//input[@id="__AjaxControlToolkitCalendarCssLoaded"]/@value').get()

	# parse from text
	def _split_hidden_text(self, txt, key):
		return txt.split(key)[1].strip().split('|')[1]

	def _get_form_action(self, response):
		action = response.xpath('//form[@id="aspnetForm"]/@action').get()
		return action[1:]

	def _get_ua(self):
		user_agent_list = ["Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0 Safari/605.1.15",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.3 Safari/605.1.15",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/601.7.8 (KHTML, like Gecko) Version/11.1.2 Safari/605.3.8",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1.2 Safari/605.1.15",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/604.5.6 (KHTML, like Gecko) Version/11.0.3 Safari/604.5.6",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/604.3.5 (KHTML, like Gecko) Version/11.0.1 Safari/604.3.5",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.1 Safari/603.1.30",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.3538.77 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4083.0 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4083.0 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4078.2 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.43 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.136 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.116 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.106 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.106 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.116 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.116 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:75.0) Gecko/20100101 Firefox/75.0",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:75.0) Gecko/20100101 Firefox/75.0",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:74.0) Gecko/20100101 Firefox/74.0",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:74.0) Gecko/20100101 Firefox/74.0",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:74.0) Gecko/20100101 Firefox/74.0",
			"Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0",
			"Mozilla/5.0 (Windows NT 10.0; rv:76.0) Gecko/20100101 Firefox/76.0",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0",
			"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:75.0) Gecko/20100101 Firefox/75.0",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:75.0) Gecko/20100101 Firefox/75.0",
			"Mozilla/5.0 (Windows NT 6.2; rv:74.0) Gecko/20100101 Firefox/74.0",
			"Mozilla/5.0 (Windows NT 10.0; ) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4086.0 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; ) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4085.4 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4085.6 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4085.5 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4085.2 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4085.2 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4085.6 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4083.0 Safari/537.36",
			"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4068.4 Safari/537.36",
			"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.62 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.62 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.62 Safari/537.36",
			"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
			"Mozilla/5.0 (Windows NT 6.3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.116 Safari/537.36"]

		user_agent = random.choice(user_agent_list)
		return user_agent

	def _strip_list(self, arr):
		new_list = []
		for item in arr:
			item = item.replace(u'\xa0', u' ')
			if item.strip():
				new_list.append(item.strip())

		return new_list

	def strip_list1(self, arr):
		new_list = []
		for item in arr:
			if item:
				new_list.append(self._valid(item))
			else:
				new_list.append(' ')

		return new_list


	def _valid(self, val):
		if val:
			return val.strip()
		else:
			return ''

	def _extract_validate(self, el):
		res = ''
		try:
			val = el.extract_first()
			if val:
				res = val.strip()
		except:
			self.log(f'Cannot get the text')

		return res

	def _normalizeKeys(self, obj):
		if type(obj) is list:
			for o in obj:
				self._normalizeKeys(o)
			return
		elif type(obj) is dict:
			keys = list(obj.keys())
			for key in keys:
				_key = '_'.join(self.strip_list1(key.lower().replace('.', '').split(' ')))
				newKey = _key.strip()
				obj[newKey] = obj.pop(key)
			for val in obj.values():
				self._normalizeKeys(val)
			return
		else:
			return

	def _normalize_keys_list(self, arr):
		new_arr = []
		for obj in arr:
			self._normalizeKeys(obj)
			new_arr.append(obj)

		return new_arr

	def _parse_summary_table(self, response, class_name, summary_name):
		try:
			headers = self._strip_list(response.xpath(f'//div[@class="{class_name}"]//fieldset[contains(./legend/text(), "{summary_name}")]//table//tbody//tr//th//text()').extract())
			values = self._strip_list(response.xpath(f'//div[@class="{class_name}"]//fieldset[contains(./legend/text(), "{summary_name}")]//table//tbody//tr//td//text()').extract())
			return dict(zip(headers, values))
		except Exception as E:
			logger.warning(str(E))

		return {}

	def _parse_table_in_section(self, response, section_id):
		table = response.xpath(f"//section[@id='{section_id}']//table")
		return self._parse_table_with(table)

	def _parse_table_with_id(self, response, table_id):
		table = response.xpath(f"//table[@id='{table_id}']")
		return self._parse_table_with(table)
		
	def _parse_table_with(self, table):
		res = []
		# headers
		headers = self._strip_list(table.xpath('.//thead//text()').extract())
		
		# values
		value_trs = table.xpath('.//tr')[1:]
		for tr in value_trs:
			values = self._strip_list(tr.xpath('.//text()').extract())
			res.append(dict(zip(headers, values)))

		return res

	def _parse_table_with_tbody(self, response, table_id):
		res = []
		try:
			trs = response.xpath(f"//table[@id='{table_id}']//tr")
			if trs:
				headers = self._strip_list(trs[0].xpath(".//text()").extract())

				for tr in trs[1:]:
					values = self._strip_list(tr.xpath(".//text()").extract())
					res.append(dict(zip(headers, values)))
		except:
			pass

		return res

	def _parse_table_with_h1(self, response, table_id):
		res = []
		try:
			tds = response.xpath("//table[@id='ctl00_PlaceHolderMain_PermitDetailList1_TBPermitDetailTest']//tr/td[@class='td_parent_left']")
			for td in tds:
				h1 = self._valid(td.xpath(".//h1//text").get())
				val = ' '.join(self._strip_list(response.xpath('.//table//text()').extract()))
				res.append({
					h1: val
				})
		except:
			pass

		return res

	def _parse_keys_values_with_span(self, response, span_id):
		res = []
		try:
			keys = response.xpath(f"//span[@id='{span_id}']//table//div[contains(@class, 'MoreDetail_ItemCol')]//h2//text()").extract()
			values = response.xpath(f"//span[@id='{span_id}']//table//div[contains(@class, 'MoreDetail_ItemCol')]//span/text()").extract()
			res = dict(zip(keys, values))
		except Exception as err:
			logger.warning(str(err))

		return res

	def _parse_keys_values_with_div(self, response, span_id):
		res = []
		try:
			keys = response.xpath(f"//span[@id='{span_id}']//table//div[contains(@class, 'MoreDetail_ItemCol')]//h2//text()").extract()
			values = response.xpath(f"//span[@id='{span_id}']//table//div[contains(@class, 'MoreDetail_ItemCol')]//div//text()").extract()
			res = dict(zip(keys, values))
		except Exception as err:
			logger.warning(str(err))

		return res

	def _parse_label_text(self, table):
		res = {}
		try:
			tds = table.xpath('.//td')
			for td in tds:
				key = self._valid(td.xpath('.//span[@class="labeltext"]/text()').get())
				value = self._valid(td.xpath('.//span[@class="linetext"]/span/text()').get())
				if key:
					res[key] = value
		except Exception as err:
			logger.warning(str(err))

		return res

	def _parse_label_text_arr(self, table):
		res = {}
		try:
			name = self._valid(table.xpath('.//tr[1]//td//text()').get())
			keys = self._strip_list(table.xpath('.//tr[2]//td/span[@class="labeltext"]//text()').extract())
			values = [e.xpath('.//text()').get() for e in table.xpath('.//tr[2]//td/span[@class="linetext"]/span')]
			values = self.strip_list1(values)
			res[name] = dict(zip(keys, values))

		except Exception as err:
			logger.warning(str(err))

		return res

	def parse_table_tab(self, tab_id, response):
		res = []
		try:
			headers = self._strip_list(response.xpath(f'//div[@id="{tab_id}"]//table/thead//text()').getall())

			trs = response.xpath(f'//div[@id="{tab_id}"]//table/tbody/tr')
			for tr in trs:
				values = self._strip_list(tr.xpath('.//td/font//text()').getall())
				if values:
					res.append(dict(zip(headers, values)))
		except Exception as err:
			logger.warning(str(err))

		return res

	def _parse_table_search_results(self, table):
		item = {}
		title = ''
		try:
			title = table.xpath('.//td[@class="hd1_sep"]/text()').get()
			labels = [self._valid(el.css('::text').get()) for el in table.xpath('.//td[@class="field_label"]')]
			data = [self._valid(el.css('::text').get()) for el in table.xpath('.//td[@class="field_data"]')]
			item = dict(zip(labels, data))
		except Exception as err:
			logger.warning(str(err))

		return title, item

	def write2csv(self, data=[], name="output.csv"):
		keys = data[0].keys()
		with open(f'./data/{name}', 'w+', newline='')  as output_file:
			dict_writer = csv.DictWriter(output_file, keys)
			dict_writer.writeheader()
			dict_writer.writerows(data)

	def decodeEmail(self, e):
		de = ""
		k = int(e[:2], 16)

		for i in range(2, len(e)-1, 2):
			de += chr(int(e[i:i+2], 16)^k)

		return de

	def _proxies(self):
		input_file = open('US Proxies.txt', 'r', encoding='utf-8')
		lines = input_file.readlines()
		proxies = [line.replace('\n', '') for line in lines]
		return 'http://' + random.choice(proxies)