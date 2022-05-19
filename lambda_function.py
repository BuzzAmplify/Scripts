import json
import boto3
import os
import csv
import codecs
import sys
import uuid
from urllib.parse import unquote_plus
import re

client = boto3.client("s3")
dynamodb = boto3.resource('dynamodb')
tableName="contacts_master1"
archive = "buzzidata-archive"

regex = re.compile(r'([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+')


def lambda_handler(event, context):
    if event:
        print("Event", event)
        # print("Records", event["Records"][0])
        records = event["Records"][0]
        
        print("bucketname", records["s3"]["bucket"]["name"])
        print("Processing ", records["s3"]["object"]["key"])
        bucket = str(records["s3"]["bucket"]["name"])
        key = unquote_plus(str(records["s3"]["object"]["key"]))
        
        batch_size = 1000
        batch = []
        
        try:
            
            data = client.get_object(Bucket=bucket, Key= key)["Body"]
            # print("S3 object", data)
            
            for row in csv.DictReader(codecs.getreader("utf-8-sig")(data)):
               
                if len(batch) >= batch_size:
                    write_to_dynamo(batch)
                    batch.clear()
                
                row['uuid'] = str(uuid.uuid4()) # Generating random uuid for unique record
               
              
                row['email_status'] = "Valid" if re.fullmatch(regex, row['Email']) else "Invalid"
                
                # print("Row:", row)
                batch.append(row)
        
            if batch:
                print("Loading batch: ", len(row))
                write_to_dynamo(batch)
                
            # we can move file to processed folder
            moveFileToArchive(bucket, key)
            return {
                'statusCode': 200,
                'body': json.dumps('Uploaded to DynamoDB Table')
            }
            
        except Exception as e:
            print("Error while processing file from S3 bucket: ", str(e))
        
        return {
                'statusCode': 500,
                'body': json.dumps('Reading file and uploading to DynamoDB Table have a problem')
            }
            
        
        
    
def write_to_dynamo(rows):
   try:
      table = dynamodb.Table(tableName)
   except:
      print("Error loading DynamoDB table. Check if table was created correctly and environment variable.")

   try:
      with table.batch_writer() as batch:
         for i in range(len(rows)):
            # print("Writing into the dynamodb table", rows[i])
            
            batch.put_item(
               Item=rows[i]
            )
   except Exception as ex:
      print("Error executing batch_writer", str(ex))
      
      
def moveFileToArchive(bucket, key):
    copy_source = {
        'Bucket': bucket,
        'Key': key
    }
    print("Archiving file ...", key)
    client.copy(copy_source, archive, key)
    
    print("Deleting Original file...", bucket)
    client.delete_object(Bucket=bucket, Key=key)
