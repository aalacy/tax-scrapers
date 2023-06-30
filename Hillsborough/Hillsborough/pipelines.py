# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json
from itemadapter import ItemAdapter
import json
from pymongo import MongoClient
import boto3
from s3transfer import S3Transfer
import requests
import os.path
import urllib.request
from pymongo import MongoClient

county='hillsborough'
bucket_name='pdw-database'

s3 = boto3.client('s3', region_name='us-west-2', aws_access_key_id='AKIAT34F3EZ4VO3PMWRA',
                  aws_secret_access_key='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')

mongocli = MongoClient('mongodb+srv://hbg:**cluster0.uetxd.mongodb.net/:27017')

try:
    os.mkdir('downloads')
except:
    pass


class HillsboroughPipeline:
    def process_item(self, item, spider):
        self._normalizeKeys(item['data'])
        self.save_to_mongo(data=item['data'])

        all_pdfs=[]
        pdf1 = item['data']['trim']
        pdf2 = item['data']['tax_collector']['latest_annual_bill']['pdfurls']
        for pdf in pdf2:
            all_pdfs.append(pdf)
        all_pdfs.append(pdf1)
        folio = item['data']['folio']
        count = 0
        if len(all_pdfs) != 0:
            for pdf in all_pdfs:
                count += 1
                if count == 1:
                    pdf_name = ''
                if count == 2:
                    pdf_name = 'trim_'
                self.download_and_upload(pdf, folio, s3, bucket_name, pdf_name)
        return item

    def save_to_mongo(self, data):
        col = mongocli['Hillsborough_County']['property']
        try:
            col.insert(data)
        except Exception as E:
            logger.warning(str(E))

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

    def download_and_upload(self, pdf, folio, bucket, bucket_name,pdf_name):

        path = os.path.join('./downloads', folio)
        try:
            os.mkdir(path)
        except:
            pass
        pdf = pdf.strip()
        if 'parcel' in pdf:
            name = pdf_name+str(pdf).split('parcels/')[1].replace('/print', '.pdf').replace('/', '_').replace('%', '-')
        else:
            name = pdf_name+pdf.rsplit('/', 1)[-1]
        filename = os.path.join('./downloads/{}'.format(folio), name)
        if not os.path.isfile(filename):
            print('Downloading: ' + filename)
            try:
                r = requests.get(pdf, stream=True)
                if r.ok:
                    filename = filename
                    print("saving to", os.path.abspath(filename))
                    with open(filename, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=2000):
                            if chunk:
                                f.write(chunk)
                                f.flush()
                                os.fsync(f.fileno())
                else:  # HTTP status code 4XX/5XX
                    pass
                    # print("Download failed: status code {}\n{}".format(r.status_code, r.pdf))
            except Exception as inst:
                name = pdf_name+pdf.split('filename%3D%')[-1].replace('%22', '')
                if 'parcel' in pdf:
                    name = pdf_name+str(pdf).split('parcels/')[1].replace('/print', '.pdf').replace('/', '_').replace('%',
                                                                                              '-')
                if 'dmz.hcpafl' in pdf:
                        name=pdf_name+ str(pdf.split('=')[-1])
                filename = './downloads/{}/{}'.format(folio, name)
                with open(filename, 'wb') as fd:
                    for chunk in r.iter_content(2000):
                        fd.write(chunk)
            transfer = S3Transfer(bucket)
            transfer.ALLOWED_UPLOAD_ARGS.append('Tagging')
            arr = os.listdir('./downloads/{}'.format(folio))
            for file in arr:
                transfer.upload_file('./downloads/{}/'.format(folio) + file, bucket_name,
                                     '{}/{}/{}'.format('hillsborough', folio, file))

                os.remove('./downloads/{}/'.format(folio) + file)

