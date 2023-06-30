from itemadapter import ItemAdapter
import json
import boto3
import requests
import os.path
from s3transfer import S3Transfer
from pymongo import MongoClient



# client = MongoClient(port=27017)
# mydb = client["Brevard_NEw"]
# mycol3 = mydb["Tax_Properties"]
county='brevard'
bucket_name='pdw-database'


s3 = boto3.client('s3', region_name='us-west-2', aws_access_key_id='**',
                  aws_secret_access_key='**+')

from util import Util

myutil = Util('Brevard')

try:
    os.mkdir('downloads')
except:
    pass


class BrevardprojectPipeline:
    def __init__(self):
        proxies = {
          'http': '95.211.175.167:13150',
          'https': '95.211.175.167:13150',
        }
        self.session = requests.Session()
        self.session.proxies = proxies
        
    def process_item(self, item, spider):
        myutil._normalizeKeys(item['data'])
        myutil._save_to_mongo(data=item['data'])

        all_pdfs = []
        pdf1 = item['data']['trim']
        pdfs = item['data']['tax_collector']['latest_annual_bill']['pdfurls']
        for pdf in pdfs:
            all_pdfs.append(pdf)
        all_pdfs.append(pdf1)
        folio = item['data']['folio']
        # self.writer(item['data'])
        self.download_and_upload(all_pdfs, folio, s3, bucket_name)
        return item

    # def writer(self, item):
    #     mycol3.insert_one(item)


    def download_and_upload(self,pdfs, folio, bucket, bucket_name):

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

