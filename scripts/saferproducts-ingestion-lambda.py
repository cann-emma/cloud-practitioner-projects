import json
import boto3
import urllib.request
from datetime import datetime

def lambda_handler(event, context):
    
    
    url = 'https://www.saferproducts.gov/RestWebServices/Recall'
    query = '?format=json&ProductType=Phone&Manufacturer=Samsung&RemedyOption=Refund'

 
    print("Making API request...")
    response= urllib.request.urlopen(url+query)
    print("Reading response...")
    response_bytes= response.read()
  
    print("Parsing response...")
    phone_data= json.loads(response_bytes)
    response.close()

    print("Uploading to S3...")
    s3= boto3.client('s3', region_name= 'us-east-1')
    bucket_name = 'samsung-refund-data-ingestion'

    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    key = f'raw/samsung_refund_data_{timestamp}.json'

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(phone_data))

    
    return {
    'statusCode': 200,
    'body': f'Uploaded file to {bucket_name}/{key}'
    }
