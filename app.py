#!/usr/bin/env python3
import os

import aws_cdk as cdk
from lib.storage_stack import StorageStack
from lib.replicator_stack import ReplicatorStack
from lib.cleaner_stack import CleanerStack
from cloud_computing_midterm.cloud_computing_midterm_stack import CloudComputingMidtermStack


app = cdk.App()

storage_stack = StorageStack(app, "StorageStack")
ReplicatorStack(app, "ReplicatorStack", storage_stack=storage_stack)
CleanerStack(app, "CleanerStack", storage_stack=storage_stack)

app.synth()