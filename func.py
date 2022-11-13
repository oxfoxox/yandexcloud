# %%
import logging
import boto3
from botocore.exceptions import ClientError
import os
import datetime
import json

# %%
def upload_new_file(path_and_file_name, bucket, s3, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(path_and_file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3.upload_file(path_and_file_name, bucket, object_name) 
        print('File was uploaded')
    except ClientError as e:
        logging.error(e)
        return False
    return path_and_file_name, bucket

# %%
def get_object_list(bucket, s3):
    objects = []
    try:
        for key in s3.list_objects(Bucket=bucket)['Contents']:
            objects.append(key['Key'])
    except Exception as e: 
        print(e)
        return False
    return objects

# %%
# Удалить несколько объектов
# НЕ возвращает ошибку, если попытаться удалить несуществующий файл
def delete_object(object_name, bucket, s3):
    try:
        forDeletion = [{'Key':object_name}]
        response = s3.delete_objects(Bucket=bucket, Delete={'Objects': forDeletion})
        print('File was deleted')
    except Exception as e: 
        print(e)
        return False
    return object_name
#delete_object(bucket,'folder1/')

# %%
# Получить объект (покажет данные внутри объекта)
def get_object(object_name, bucket, s3):
    object_data = ''
    try:
        get_object_response = s3.get_object(Bucket=bucket,Key=object_name)
        object_data = get_object_response['Body'].read()
        print('File was downloaded')
    except Exception as e: 
        print(e)
        return False
    return object_data

# %%
# Изменить объект
def change_object(file, file_name):
    try:
        changed_file = {"test" : 'try_new_title'}
        new_file_name = str(datetime.datetime.now())+ ' ' +file_name
        print('Changes have been made')
    except Exception as e: 
        print(e)
        return False
    return changed_file, new_file_name

# %%
#загрузить 
def upload_data(bucket, key, body,s3,storageclass='STANDARD'):
    """bucket - бакет, куда грузим
    key - как файл будет называться
    body - что за строку грузим
    StorageClass - класс хранения:
                Стандартное хранилище — STANDARD.
                Холодное хранилище — COLD, STANDARD_IA или NEARLINE (последние два — только при загрузке объектов в бакет).
                Ледяное хранилище — ICE, GLACIER (последний — только при загрузке объектов в бакет).
    """
    try:
        new_body = json.dumps(body, indent=2).encode('utf-8')
        s3.put_object(Bucket=bucket, Key=key, Body=new_body, StorageClass=storageclass)
        print('Data was uploaded')
    except Exception as e: 
        print(e)
        return False


