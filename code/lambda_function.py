import json
import pandas as pd
import os
import boto3
import logging
import urllib3
import io

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

url = "https://jsonplaceholder.typicode.com/comments"
http = urllib3.PoolManager()

def lambda_handler(event, context):
    source_bucket = os.environ['SOURCE_BUCKET_NAME']
    destination_bucket = os.environ['DESTINATION_BUCKET_NAME']
    
    logger.info(msg="*****Lambda initialized****")
    response = http.request('GET',url)
    
    data = json.loads(response.data.decode('utf-8'))
        
    # Store the JSON data in an S3 bucket    
    s3.put_object(Body=response.data, Bucket = source_bucket, Key='etl-raw-data/comments-raw.json')
    
    logger.info(msg="Dumped into source bucket successfully")
        
    df = pd.DataFrame(data)
    df['email'] = df['email'].str.lower()
    transformed_df = df[['postId', 'name', 'email']].rename(columns={'postId': 'Post ID', 'name': 'Name', 'email': 'Email'})
    
    csv_buffer = io.StringIO()
    transformed_df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    s3.put_object(Body=csv_data.encode('utf-8'), Bucket = destination_bucket, Key='etl-cleaned-data/comments-cleaned.csv')
    
    logger.info(msg="Dumped into destination bucket successfully")

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }