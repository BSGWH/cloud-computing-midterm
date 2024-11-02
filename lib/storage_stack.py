from aws_cdk import (
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    Stack
)
from constructs import Construct

class StorageStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        
        # S3 Buckets
        self.bucket_src = s3.Bucket(self, "BucketSrc")
        self.bucket_dst = s3.Bucket(self, "BucketDst")

        # DynamoDB Table T
        self.table = dynamodb.Table(
            self, "TableT",
            partition_key=dynamodb.Attribute(name="object_name", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="copy_id", type=dynamodb.AttributeType.STRING)
        )

        # Adding GSI for disowned items
        self.table.add_global_secondary_index(
            index_name="DisownedIndex",
            partition_key=dynamodb.Attribute(name="object_name", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="disown_timestamp", type=dynamodb.AttributeType.NUMBER)
        )
