import boto3
import datetime

def email_extract(filename, s3_bucket):
    #add yyyy_MM_dd_HH_mm_ss to filename
    #lowercase filename
    s3_client = boto3.resource('s3')
    bucket = s3_client.Bucket(s3_bucket)
    now = datetime.now()
    ending = now.strftime("Y_%m_%d_%H_%S")
    #name = file
    for name in bucket.glob('*[!0123456789]/.csv'):
        new_path = name.with_stem(name.stem + ending)
        if not new_path.exists():
            newFileName = name.rename(new_path)
            newFileName = newFileName.lower()
            return newFileName