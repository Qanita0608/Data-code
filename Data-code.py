import json
import boto3
import csv
import io

def lambda_handler(event, context):
    # Initialize the S3 client
    s3 = boto3.client('s3')
    return("Hello world")
