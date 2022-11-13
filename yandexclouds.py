# %%
import boto3
import datetime
import json
import ast
import time

# %%
from func import *
from creds import *

# %%
path = './'#точка указывает, что нужно искать в текущей папке
file_name = 'file.json'
path_and_file_name= path+file_name

bucket = 'kmtestbucket'
updated_bucket = 'updatedkmtestbucket'

# %%
session = boto3.session.Session()
s3 = session.client(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    service_name=SERVICE_NAME,
    endpoint_url=ENDPOINT_URL
)

# %%
#основная часть работы
def infinity_request():
    print('START')
    print(datetime.datetime.now(),'pre step')

    uploaded_path_and_file_name, uploaded_bucket = upload_new_file(path_and_file_name, bucket, s3)
    print(datetime.datetime.now(), 'after upload time')

    downloaded_file_data = get_object(file_name, uploaded_bucket, s3)
    #из bytes превращаем в словарь, чтобы дальше с ним работать
    normal_downloaded_file_data = ast.literal_eval(downloaded_file_data.decode('utf-8')) 
    print(datetime.datetime.now(), 'after download time')

    changed_file, new_file_name = change_object(normal_downloaded_file_data, file_name)
    print(datetime.datetime.now(), 'after change time')

    new_file_in_yandex = upload_data(updated_bucket, key=new_file_name, body = changed_file, s3=s3)
    print(datetime.datetime.now(), 'after new upload time')
    print('END')

# %%
c = 0
while True:
    infinity_request()
    time.sleep(5 * 60)
    if c == 2:
        break
    c += 1

# %%



