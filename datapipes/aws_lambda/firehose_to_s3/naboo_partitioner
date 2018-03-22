from __future__ import print_function

import os
import base64
import boto3
import logging
import time
from datetime import datetime

# environment variables
S3_BUCKET = os.environ['S3_BUCKET']
S3_PATH = os.environ['S3_PATH']
debug_mode = os.environ.get('DEBUG_MODE', "False")

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG if debug_mode == "True" else logging.INFO)


def lambda_handler(event, context):
    """
    This function invokes when an incoming batch of logs passes through firehose.
    It extracts and decodes every data string, append them together and sends to S3 bucket with partitioned file path

    :param event:
        In the format of:
        {
          "invocationId": "4433b41a-57b5-43b1-9631-414678256b70",
          "deliveryStreamArn": "arn:aws:firehose:us-west-2:170553093583:deliverystream/NabooDevFeedEventToS3",
          "region": "us-west-2",
          "records": [
                {
                  "recordId": "49582788558165909651861901833947708776876477421386203138000000",
                  "approximateArrivalTimestamp": 1521682446773,
                  "data": "eyJ0cyI6IjIwMTgtMDMtMTNUMDE6MTI6MTAuNTE5ODEzWiIsInByb3BzIjp7InZpZGVvX2lkIjoxLCJ1c2VyX2
                        lkIjoxMSwiaWQiOjF9LCJldmVudCI6InNob3dfdmlkZW8ifQ=="
                },
                {
                  "recordId": "49582788558165909651861901833950126628515706679735615490000000",
                  "approximateArrivalTimestamp": 1521682446775,
                  "data": "eyJ0cyI6IjIwMTgtMDMtMTNUMDE6MTI6MTAuNTI1NTMzWiIsInByb3BzIjp7InZpZGVvX2lkIjoxLCJ1c2VyX2l
                        kIjoyMywiaWQiOjJ9LCJldmVudCI6InNob3dfdmlkZW8ifQ=="
                },
                ...
          ]
        }
    :param context:
    :return: None
    """

    s3 = boto3.client('s3')
    json_str = ''

    for record in event['records']:
        json_str += base64.b64decode(record['data']).decode("utf-8") + "\n"

    date = datetime.fromtimestamp(time.time())

    file_path = S3_PATH + "/year=" + str(date.year) + "/month=" + str(date.month) + "/day=" + str(
        date.day) + "/hour=" + str(date.hour) + "/minute=" + str(date.minute) + "/" + \
        event['deliveryStreamArn'].split('/')[1] + "-" + str(date.year) + "-" + str(date.month) + "-" + str(
        date.day) + "-" + str(date.minute) + "-" + str(date.second) + "-" + str(date.microsecond) + "-" + event[
                    'invocationId'] + ".json"

    s3.put_object(ContentType="application/json", Bucket=S3_BUCKET, Key=file_path, Body=json_str)

    logger.debug("Successfully sent %s records to S3:%s/%s" % (len(event["records"]), S3_BUCKET, file_path))
