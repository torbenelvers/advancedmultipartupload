# advancedmultipartupload
#Advanced Multipart Upload for AWS S3

#Multipart Upload 1.1 (c) Torben Elvers, 2022
#AWS allows multipart uploads which is much more performant when uploading large files, e.g.
#when you decide to upload large zip files in order to save costs when using
#S3 deep archive.

#AWS S3 returns an Etag for the uploaded file after the multipart upload which is basically
#the combined MD5 of the single MD5 hashes of the uploaded parts.

#Advanced Multipart Upload calculates the ETag for uploaded files on the local drive after the upload
#and compares the calcultaed Etag from the local file to the Etag returned from the AWS multipart upload.

#A log file outputlog.txt is written after the upload 

#python required

Examples:

Uploading files:
py advancedmultipartupload.py --mode upload --filename file.7z --destbucket testbucket --partsize 5 --accesskey 1234ABCD --secretkey 1234ABCD --region eu-central-1'

Only fetch Etag from file on S3:
py advancedmultipartupload.py --mode gets3etag --filename file.7z --destbucket testbucket'

Only calculate Etag for local file:
py advancedmultipartupload.py --mode getlocaletag --filename file.7z --partsize 5'

Execute:

py advancedmultipartupload.py

options:
  -h, --help            show this help message and exit
  --mode MODE           Mode is either 'upload' or 'getlocaletag' or 'gets3etag'.
  --filename FILENAME   File to be uploaded.
  --destbucket DESTBUCKET
                        S3 bucket for uploading or for reading the s3 etag if in gets3etag mode.
  --partsize PARTSIZE   Size of individual parts in GB
  --accesskey ACCESSKEY
                        AWS accesskey
  --secretkey SECRETKEY
                        AWS secret key
  --region REGION       region
  --example EXAMPLE     Get an example for upload or getlocaletag or gets3etag
  --shutdown yes        Shut down PC after upload
