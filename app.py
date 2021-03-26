#!/usr/bin/env python3

from aws_cdk import core

from vre_data_lake.vre_data_lake_stack import VreDataLakeStack


app = core.App()

data_lake_name = "vre_data_lake" # Must be alphanumeric with underscores only for Athena
VreDataLakeStack(app, "vre-data-lake", data_lake_name=data_lake_name)

app.synth()
