#!/usr/bin/env python3
import aws_cdk as cdk
from pipeline_stack import NycTaxiSfnStack

app = cdk.App()
NycTaxiSfnStack(app, "NycTaxiSfnStack")
app.synth()
