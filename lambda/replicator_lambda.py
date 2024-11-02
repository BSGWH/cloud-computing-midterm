import boto3
import os
import time
import uuid
from boto3.dynamodb.conditions import Key

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])

def handler(event, context):
    for record in event["Records"]:
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]

        if record["eventName"] == "ObjectCreated:Put":
            # Create a unique copy_key
            copy_id = f"copy-{uuid.uuid4()}"
            dst_bucket = os.environ["DST_BUCKET"]
            
            # Copy object to BucketDst
            s3.copy_object(
                Bucket=dst_bucket,
                CopySource={"Bucket": bucket_name, "Key": object_key},
                Key=copy_id
            )
            
            # Query existing copies
            existing_copies = table.query(
                KeyConditionExpression=Key('object_name').eq(object_key)
            )['Items']
            
            if existing_copies:
                # Find the oldest copy
                oldest_copy = min(existing_copies, key=lambda x: x['copy_id'])  # Assuming copy_id can be ordered
                # Delete the oldest copy from BucketDst
                s3.delete_object(Bucket=dst_bucket, Key=oldest_copy['copy_id'])
                # Remove the oldest copy record from DynamoDB
                table.delete_item(
                    Key={
                        "object_name": object_key,
                        "copy_id": oldest_copy['copy_id']
                    }
                )
            
            # Insert the new copy record
            table.put_item(Item={
                "object_name": object_key,
                "copy_id": copy_id,
                "status": "active"
            })

        elif record["eventName"] == "ObjectRemoved:Delete":
            # Query all copies for the object
            copies = table.query(
                KeyConditionExpression=Key('object_name').eq(object_key)
            )['Items']
            
            # Update each copy as disowned
            for copy in copies:
                table.update_item(
                    Key={
                        "object_name": object_key,
                        "copy_id": copy['copy_id']
                    },
                    UpdateExpression="SET disown_timestamp = :time, #s = :status",
                    ExpressionAttributeNames={
                        "#s": "status"
                    },
                    ExpressionAttributeValues={
                        ":time": int(time.time()),
                        ":status": "disowned"
                    }
                )
