# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import logging
from itemadapter import ItemAdapter
import json
from pymongo import MongoClient
import json
import boto3
from s3transfer import S3Transfer
import requests
import os.path

logging.basicConfig(filename='collier.logs', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
mongocli = MongoClient('mongodb+srv://hbg:**cluster0.uetxd.mongodb.net/:27017')
county='orange'
bucket_name='pdw-database'

s3 = boto3.client('s3', region_name='us-west-2', aws_access_key_id='AKIAT34F3EZ4VO3PMWRA',
                  aws_secret_access_key='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')


try:
    os.mkdir('downloads')
except:
    pass

class OrangePipeline:
    def __init__(self):
        proxies = {
          'http': '95.211.175.167:13150',
          'https': '95.211.175.167:13150',
        }
        self.session = requests.Session()
        self.session.proxies = proxies
        
    def process_item(self, item, spider):
        all_pdfs=[]
        self._normalizeKeys(item['data'])
        self.save_to_mongo(item['data'])
        pdf1 = item['data']['tax_collector']['current_taxes_and_unpaid_delinquent_warrants']

        for value in pdf1:
            all_pdfs.append(value['view_bill/receipt'])
        folio = item['data']['folio']
        if len(all_pdfs) != 0:
            self.download_and_upload(all_pdfs, folio, s3, bucket_name)
        return item

    def download_and_upload(self,pdfs, folio, bucket, bucket_name):
        if len(pdfs)=='0':
            return
        path = os.path.join('/tmp', folio)
        try:
            os.mkdir(path)
        except:
            pass

        for pdf in pdfs:
            pdf = pdf.strip()
            if 'ViewTaxReceipt' in pdf:
                name='tax_pdf'+pdf.rsplit('TaxSummaryId=')[-1].split('&')[0]
            elif 'parcel' in pdf:
                name = str(pdf).split('parcels/')[1].replace('/print', '.pdf').replace('/', '_').replace('%', '-')
            else:
                name = pdf.rsplit('/', 1)[-1]

            filename = os.path.join('/tmp/{}'.format(folio), name)

            if not os.path.isfile(filename):
                print('Downloading: ' + filename)
                try:
                    r = self.session.get(pdf, stream=True)
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

    def save_to_mongo(self, data):
        col = mongocli['Orange_County']['property']
        try:
            col.insert(data)
        except Exception as E:
            logging.error('Error: ' + str(E), exc_info=True)

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

