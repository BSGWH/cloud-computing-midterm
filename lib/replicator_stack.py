from aws_cdk import (
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_event_sources,
    aws_iam as iam,
    aws_s3 as s3,
    Stack
)
from constructs import Construct
from .storage_stack import StorageStack


class ReplicatorStack(Stack):
    def __init__(self, scope: Construct, id: str, storage_stack: StorageStack, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Lambda function
        replicator_lambda = _lambda.Function(
            self, "ReplicatorHandler",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="replicator_lambda.handler",
            code=_lambda.Code.from_asset("lambda"),
            environment={
                "TABLE_NAME": storage_stack.table.table_name,
                "DST_BUCKET": storage_stack.bucket_dst.bucket_name,
                "SRC_BUCKET": storage_stack.bucket_src.bucket_name
            }
        )

        # Grant permissions
        storage_stack.table.grant_read_write_data(replicator_lambda)
        storage_stack.bucket_dst.grant_put(replicator_lambda)

        # Add S3 event source
        replicator_lambda.add_event_source(
            lambda_event_sources.S3EventSource(storage_stack.bucket_src, events=[s3.EventType.OBJECT_CREATED, s3.EventType.OBJECT_REMOVED])
        )
