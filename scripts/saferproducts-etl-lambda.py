import boto3
import json
import csv
import io
from datetime import datetime


def flatten_dict(d, parent_key='', sep='_'):
    items = []

    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k

        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())

        elif isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    items.extend(flatten_dict(item, f"{new_key}{sep}{i}", sep=sep).items())
                else:
                    items.append((f"{new_key}{sep}{i}", item))

        else:
            items.append((new_key, v))

    return dict(items)

def lambda_handler(event, context):

    s3 = boto3.client('s3')
    bucket_name = 'samsung-refund-data-ingestion'
    key = 'raw/samsung_refund_data_2026-03-30-03-26-31.json'

    obj = s3.get_object(Bucket=bucket_name, Key=key)
    data = json.loads(obj['Body'].read())


    flattened_data = [flatten_dict(record) for record in data]


    # data= pd.DataFrame(data)
    
    # data_csv = data.to_csv(index=False)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=flattened_data[0].keys())
    writer.writeheader()
    writer.writerows(flattened_data)

    data_csv = output.getvalue()

   
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    key = f'processed/samsung_refund_data_{timestamp}.csv'

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=data_csv
    )

    return {
        'statusCode': 200,
        'body': 'Onward!'
    }
