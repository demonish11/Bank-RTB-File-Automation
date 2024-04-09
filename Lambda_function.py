import json
import boto3
import io
from io import BytesIO

def lambda_handler(event, context):
    s3 = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    client = boto3.client('glue')
    if event:
        s3_everec = event["Records"][0]
        bucket_name = str(s3_everec["s3"]["bucket"]["name"])
        file_name = str(s3_everec["s3"]["object"]["key"])
        if '+' in file_name:
            file_name = file_name.replace('+',' ')
        print(file_name)
        
        file_obj = s3.get_object(Bucket=bucket_name, Key=file_name)
        data = BytesIO(file_obj['Body'].read())
        
        if "RTB_File" in file_name:
            print('-----triggerstarts for Refund_hold_mapping-----')
            response = client.start_job_run(JobName='RTB_RIC_automation')
            print(json.dumps(response,indent=4))
        else:
            print('Trigger not triggered')