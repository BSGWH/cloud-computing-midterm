from aws_cdk import (
    aws_lambda as _lambda,
    Stack,
    Duration,
)
from constructs import Construct
from .storage_and_processing_stack import StorageStack

class CleanerStack(Stack):
    def __init__(self, scope: Construct, id: str, storage_stack: StorageStack, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Lambda function
        cleaner_lambda = _lambda.Function(
            self, "CleanerHandler",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="cleaner_lambda.handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.seconds(30),
            environment={
                "TABLE_NAME": storage_stack.table.table_name,
                "DST_BUCKET": storage_stack.bucket_dst.bucket_name
            }
        )

        # Grant permissions
        storage_stack.table.grant_read_write_data(cleaner_lambda)
        storage_stack.bucket_dst.grant_delete(cleaner_lambda)
