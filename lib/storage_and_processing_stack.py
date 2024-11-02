from aws_cdk import (
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_event_sources,
    Stack,
    Duration,
)
from constructs import Construct

class StorageAndProcessingStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Define S3 buckets
        self.bucket_src = s3.Bucket(self, "BucketSrc")
        self.bucket_dst = s3.Bucket(self, "BucketDst")

        # Define DynamoDB table
        self.table = dynamodb.Table(
            self, "TableT",
            partition_key=dynamodb.Attribute(name="object_name", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="copy_id", type=dynamodb.AttributeType.STRING)
        )
        self.table.add_global_secondary_index(
            index_name="DisownedIndex",
            partition_key=dynamodb.Attribute(name="object_name", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="disown_timestamp", type=dynamodb.AttributeType.NUMBER)
        )

        # Define Replicator Lambda
        replicator_lambda = _lambda.Function(
            self, "ReplicatorHandler",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="replicator_lambda.handler",
            code=_lambda.Code.from_asset("lambda"),
            environment={
                "TABLE_NAME": self.table.table_name,
                "DST_BUCKET": self.bucket_dst.bucket_name,
                "SRC_BUCKET": self.bucket_src.bucket_name
            }
        )

        # Grant permissions
        self.table.grant_read_write_data(replicator_lambda)
        self.bucket_dst.grant_put(replicator_lambda)

        # Add S3 event source
        replicator_lambda.add_event_source(
            lambda_event_sources.S3EventSource(
                self.bucket_src,
                events=[s3.EventType.OBJECT_CREATED, s3.EventType.OBJECT_REMOVED]
            )
        )

        # Define Cleaner Lambda
        cleaner_lambda = _lambda.Function(
            self, "CleanerHandler",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="cleaner_lambda.handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.seconds(30),
            environment={
                "TABLE_NAME": self.table.table_name,
                "DST_BUCKET": self.bucket_dst.bucket_name
            }
        )

        # Grant permissions
        self.table.grant_read_write_data(cleaner_lambda)
        self.bucket_dst.grant_delete(cleaner_lambda)
