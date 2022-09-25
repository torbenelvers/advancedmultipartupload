#Multipart Upload 1.1 (c) Torben Elvers, 2022
#AWS allows multipart uploads which is much more performant when uploading files, e.g.
#when you decide to upload large zip files in order to save costs when usind
#S3 deep archive
#AWS S3 returns an Etag for the uploaded file after the multipart upload which is basically
#the combined MD5 of the single MD5 hashes of the uploaded parts
#Advanced Multipart Upload calculates the ETag for uploaded files on the local drive after upload
#and compares to the Etag returned from the AWS multipart upload.
#A log file outputlog.txt is written after the upload  

import logging 
import boto3
from boto3.s3.transfer import TransferConfig
import os
import threading
import sys
import argparse
import hashlib
import json
from datetime import datetime

def md5_checksum(filename):
    m = hashlib.md5()
    with open(filename, 'rb') as f:
        for data in iter(lambda: f.read(1024 * 1024), b''):
            m.update(data)
   
    return m.hexdigest()

def etag_checksum(filename, partsize):
    
    chunk_size=partsize * 1024 * 1024
    md5s = []
    with open(filename, 'rb') as f:
        for data in iter(lambda: f.read(chunk_size), b''):
            md5s.append(hashlib.md5(data).digest())
    m = hashlib.md5(b"".join(md5s))
    return '{}-{}'.format(m.hexdigest(), len(md5s))

def multipart_upload_boto3(filename, bucketname, partsize):
    file_path = filename
    key = os.path.basename(file_path) 

    config = TransferConfig(
                        max_concurrency=10,
                        multipart_chunksize=1024 * partsize * 1024,
                        use_threads=True,
                        )
    s3_resource.Object(bucketname, key).upload_file(file_path,
                            Config=config,
                            Callback=ProgressPercentage(file_path)
                            )

class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()

#main

#logging
logging.basicConfig(filename="std.log", 
					format='%(asctime)s %(message)s', 
					filemode='w') 
logger=logging.getLogger()
logger.setLevel(logging.INFO)

#logger.debug("This is just a harmless debug message") 
#logger.info("This is just an information for you") 
#logger.warning("OOPS!!!Its a Warning") 
#logger.error("Have you try to divide a number by zero") 
#logger.critical("The Internet is not working....")

print('Advanced Multipart Upload 1.1 T.Elvers 2022')

parser = argparse.ArgumentParser()

parser.add_argument('--mode',
                    help="Mode is either 'upload' or 'getlocaletag' or 'gets3etag'.")
parser.add_argument('--filename',
                    help='File to be uploaded.')
parser.add_argument('--destbucket',
                    help='S3 bucket for uploading or for reading the s3 etag if in gets3etag mode.')
parser.add_argument('--partsize',
                    type=int,
                    help='Size of individual parts in GB.')
parser.add_argument('--accesskey',
                    help='AWS accesskey')
parser.add_argument('--secretkey',
                    help='AWS secret key')
parser.add_argument('--region',
                    help='region')
parser.add_argument('--example',
                    help='Get an example for upload or getlocaletag or gets3etag')

cli_options = parser.parse_args()

s3_resource = boto3.resource('s3')

if (cli_options.mode != 'upload') and (cli_options.mode != 'getlocaletag') and (cli_options.mode != 'gets3etag'):
    print('py advancedmultipartupload.py --mode upload --filename file.7z --destbucket testbucket --partsize 5 --accesskey 1234ABCD --secretkey 1234ABCD --region eu-central-1')
    print('py advancedmultipartupload.py --mode gets3etag --filename file.7z --destbucket testbucket')
    print('py advancedmultipartupload.py --mode getlocaletag --filename file.7z --partsize 5')
    sys.exit(1)

if cli_options.example == 'upload':
    print('py advancedmultipartupload.py --mode upload --filename file.7z --destbucket testbucket --partsize 5 --accesskey 1234ABCD --secretkey 1234ABCD --region eu-central-1')
if cli_options.example == 'gets3etag':
    print('py advancedmultipartupload.py --mode gets3etag --filename file.7z --destbucket testbucket')
if cli_options.example == 'getlocaletag':
    print('py advancedmultipartupload.py --mode getlocaletag --filename file.7z --partsize 5')

if cli_options.mode == 'getlocaletag':
    print('Get etag from local file.')
    fetag = etag_checksum(cli_options.filename, cli_options.partsize*1024)
    print('Etag of local file:    ', '"' + fetag + '"')

if cli_options.mode == 'gets3etag':
    print('Get Etag from s3 object.')
    s3_client = boto3.client('s3')
    response = s3_client.get_object_attributes(
        Bucket=cli_options.destbucket,
        Key=cli_options.filename,
        ObjectAttributes=[
            'ETag',
        ]
    )
    
    print("Etag: " + response['ETag'])

if cli_options.mode == 'upload':
    print('Initializing upload...') 
    try:
        f=open(cli_options.filename, 'rb')
    except IOError:
        print ('Cannot open file.')
        sys.exit(1)

    session = boto3.Session(
        aws_access_key_id=cli_options.accesskey,
        aws_secret_access_key=cli_options.secretkey,
        region_name=cli_options.region
    )
    
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    logger.info('Multipartupload of: ' + cli_options.filename + ' into bucket: ' + cli_options.destbucket + ' started.')
    print(current_time +' Multipartupload of: ' + cli_options.filename + ' into bucket: ' + cli_options.destbucket + ' started.')
    
    multipart_upload_boto3(cli_options.filename,cli_options.destbucket, cli_options.partsize*1024)

    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print('')
    print(current_time + ' Multipartupload of: ' + cli_options.filename + ' into bucket: ' + cli_options.destbucket + ' finished.')
    logger.info('Multipartupload of: ' + cli_options.filename + ' into bucket: ' + cli_options.destbucket + ' finished.')

    file_path2 = cli_options.filename
    key2 = os.path.basename(file_path2) 
    s3 = session.client('s3')
    obj_dict = s3.get_object(Bucket=cli_options.destbucket, Key=key2)

    etag = (obj_dict['ETag'])
    print('Fetched Etag(Based on MD5) of uploaded file: ' + etag)
    logger.info('Fetched Etag(Based on MD5) of uploaded file: ' + etag)

    fetag = etag_checksum(cli_options.filename, cli_options.partsize*1024)    
    print('Calculated Etag(Based on MD5) of local file:', '"' + fetag + '"')
    logger.info('Calculated Etag(Based on MD5) of local file: ' + '"' + fetag + '"')

    fetag = '"' + fetag + '"'
    if etag == fetag:
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print(current_time + ' Upload succeeded.')
        logger.info('Upload succeeded.')
    else:
         print(current_time + ' Upload failed.')
         logger.error('Upload failed.')
