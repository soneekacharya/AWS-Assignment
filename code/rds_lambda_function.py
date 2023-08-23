import json
import pandas as pd
import os
import boto3
import logging
import urllib3
import io
import psycopg2

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


    # Retrieve environment variables
    dbname = os.environ['DB_NAME']
    user = os.environ['DB_USER']
    password = os.environ['DB_PASSWORD']
    host = os.environ['DB_HOST']
    
    # Establish a connection
    connection = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
    
    try:
        # Create a cursor
        cursor = connection.cursor()
        
        # Insert each row into the table
        insert_query = "INSERT INTO etl_training_sonika_comments (PostID, Name, Email) VALUES (%s, %s, %s)"
        
        for index, row in transformed_df.iterrows():
            cursor.execute(insert_query, (row['Post ID'], row['Name'], row['Email']))
        
        # Commit the transaction
        connection.commit()
        return "Data inserted successfully"
        
    except (Exception, psycopg2.Error) as error:
        return f"Error: {error}"
        
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }