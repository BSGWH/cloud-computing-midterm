import boto3
import os
import time
from boto3.dynamodb.conditions import Attr

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])

def handler(event, context):
    current_time = int(time.time())
    dst_bucket = os.environ["DST_BUCKET"]

    # Define the threshold (items disowned for more than 10 seconds)
    threshold = current_time - 10  # 10 seconds ago

    # Scan for disowned items older than the threshold
    response = table.scan(
        FilterExpression=Attr('status').eq("disowned") & Attr('disown_timestamp').lte(threshold)
    )

    if response['Items']:
        for item in response['Items']:
            copy_id = item['copy_id']
            object_name = item['object_name']

            # Delete the copy from BucketDst
            try:
                s3.delete_object(Bucket=dst_bucket, Key=copy_id)
                print(f"Deleted copy: {copy_id} from bucket: {dst_bucket}")
            except Exception as e:
                print(f"Error deleting {copy_id}: {e}")
                continue  # Proceed with next item

            # Delete the item from DynamoDB
            try:
                table.delete_item(
                    Key={
                        "object_name": object_name,
                        "copy_id": copy_id
                    }
                )
                print(f"Deleted DynamoDB item for copy_id: {copy_id}")
            except Exception as e:
                print(f"Error deleting DynamoDB item for {copy_id}: {e}")

    # Re-invoke the Lambda after 5 seconds
    lambda_client = boto3.client('lambda')
    try:
        lambda_client.invoke(
            FunctionName=context.function_name,
            InvocationType='Event',
            Payload=b'{}',
        )
    except Exception as e:
        print(f"Error re-invoking Cleaner Lambda: {e}")
