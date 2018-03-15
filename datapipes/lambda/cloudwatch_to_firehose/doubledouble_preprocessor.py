from __future__ import print_function

import json
import zlib
import boto3
import os
import logging

log_config = {
    'response_log=': {
        'stream_name': os.environ.get('RESPONSE_DELIVERY_STREAM_NAME', "DoubleDoubleSandboxResponseToS3"),
        'batch_size': os.environ.get('DELIVERY_STREAM_BATCH_SIZE', 499)
    },
    'event_tracking=': {
        'stream_name': os.environ.get('EVENT_TRACKING_DELIVERY_STREAM_NAME', "DoubleDoubleSandboxTrackingToS3"),
        'batch_size': os.environ.get('DELIVERY_STREAM_BATCH_SIZE', 499)
    }
}
HEALTH_CHECK = 'health_check'
firehose = boto3.client('firehose')

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def stream_gzip_decompress(stream):
    dec = zlib.decompressobj(32 + zlib.MAX_WBITS)
    buf = ''
    for chunk in stream:
        rv = dec.decompress(chunk)
        if rv:
            buf += rv
    return buf


def extract_controller_json_str(log_line):
    """
    Log format:
    {
        "logEvents": [
            {
                "id": "33800085991367471500975998592917336325415151522589114368",
                "message": "iex(hibiki@172.31.25.128)1> 2018-01-11 05:24:35.917 request_id=h660ptntri278mlr4p8l7s4hc4uftpqb [info] GET /",
                "timestamp": 1515648275000
            },
            {
                "id": "33800085991367471500975998592917336325415151522589114369",
                "message": "iex(hibiki@172.31.25.128)1> 2018-01-11 05:24:35.918 request_id=h660ptntri278mlr4p8l7s4hc4uftpqb [info] Sent 200 in 708us",
                "timestamp": 1515648275000
            },
            {
                "id": "33800085991367471500975998592917336325415151522589114370",
                "message": "2018-01-11 05:24:35.918 request_id=h660ptntri278mlr4p8l7s4hc4uftpqb [info] response_log={\"user_agent\":\"Mozilla/5.0+(compatible; MxToolbox/Beta7; http://www.mxtoolbox.com/)\",\"status\":200,\"requested_at\":\"2018-01-11T05:24:35.918092Z\",\"request_id\":\"h660ptntri278mlr4p8l7s4hc4uftpqb\",\"remote_ip\":\"64.20.227.137\",\"path\":\"/\",\"params\":{},\"method\":\"GET\"}",
                "timestamp": 1515648275000
            },
            {
                "id": "33800086013668216699506621734453054598063513028569530371",
                "message": "iex(hibiki@172.31.25.128)1> 2018-01-11 05:24:36.192 request_id=niovbt97f4h2gje5jq28bvbn21tsjohm [info] GET /",
                "timestamp": 1515648276000
            },
            {
                "id": "33800086013668216699506621734453054598063513028569530372",
                "message": "iex(hibiki@172.31.25.128)1> 2018-01-11 05:24:36.193 request_id=niovbt97f4h2gje5jq28bvbn21tsjohm [info] Sent 200 in 648us",
                "timestamp": 1515648276000
            },
            {
                "id": "33800086013668216699506621734453054598063513028569530373",
                "message": "2018-01-11 05:24:36.193 request_id=niovbt97f4h2gje5jq28bvbn21tsjohm [info] response_log={\"user_agent\":\"Mozilla/5.0+(compatible; MxToolbox/Beta7; http://www.mxtoolbox.com/)\",\"status\":200,\"requested_at\":\"2018-01-11T05:24:36.193544Z\",\"request_id\":\"niovbt97f4h2gje5jq28bvbn21tsjohm\",\"remote_ip\":\"64.20.227.137\",\"path\":\"/\",\"params\":{},\"method\":\"GET\"}",
                "timestamp": 1515648276000
            },
            {
                "id": "33800086102871197493629114300595927688656959052491194376",
                "message": "2018-01-11 05:24:40.681 request_id=aan1tpituomcne84aue74cunfc75s0fn [info] response_log={\"user_agent\":\"Mozilla/5.0 (iPhone; CPU iPhone OS 11_2_1 like Mac OS X) AppleWebKit/604.4.7 (KHTML, like Gecko) Mobile/15C153/DoubleDouble/1.3.1\",\"status\":200,\"requested_at\":\"2018-01-11T05:24:40.681679Z\",\"request_id\":\"aan1tpituomcne84aue74cunfc75s0fn\",\"remote_ip\":\"76.204.214.70\",\"path\":\"/api/system/apps_metadata\",\"params\":{},\"method\":\"GET\"}",
                "timestamp": 1515648280000
            }
        ],
        "logGroup": "logging",
        "logStream": "hibiki-prod",
        "messageType": "DATA_MESSAGE",
        "owner": "067246364203",
        "subscriptionFilters": [
            "LambdaStream_doubledouble-log-preprocess"
        ]
    }

    :param log_line: a JSON string log line sent from ASG
    :return: A tuple or None if not found. Returns tag for the json, and the json part of the line.
    """
    
    json_log = (None, None)

    if HEALTH_CHECK not in log_line:
        tag = _log_has_tags_of_interest(log_config.keys(), log_line)

        if tag:
            (_, _, json_str) = log_line.partition(tag)

            # make sure it's a valid json
            try:
                json.loads(json_str)
                json_log = (tag, json_str + "\n")
            except ValueError:
                logger.warn("Invalid json found: " + log_line)

    return json_log


def _log_has_tags_of_interest(tags, line):
    """
    Check if this line has one of registered tags. This search is exclusive.

    :param tags: Array of strings that we want to look for in the log line
    :param line: A log line
    :return: The tag first found in the log line. None, otherwise.
    """
    for tag in tags:
        if tag in line:
            return tag

    return None


def lambda_handler(event, context):
    """
    :param event:
        In the format of:
        {
            "awslogs": {
                "data": "H4sIAAAAAAAAAO1c227jOBL9FcPPiYdkkSwywDw0....[gzipped base64 string]"
        }
    :param context:
    :return: None
    """
    if not event['awslogs']['data']:
        return

    stream = event['awslogs']['data']
    payload = json.loads(stream_gzip_decompress(stream.decode('base64')))
    records = dict((t, []) for t in log_config.keys())
    
    if not payload["logStream"] or "hibiki-prod" != payload["logStream"]:
        return

    for log_event in payload['logEvents']:
        json_log = extract_controller_json_str(log_event['message'])

        if all(json_log):
            logger.info("====" + str(json_log))

            (tag, json_str) = json_log
            records[tag].append({'Data': json_str})
            records[tag] = write_records(
                                log_config[tag]['stream_name'],
                                records[tag],
                                log_config[tag]['batch_size'])

    # Flush
    for tag in log_config.keys():
        write_records(
            log_config[tag]['stream_name'],
            records[tag],
            0)

    return True


def write_records(stream_name, records, batch_size):
    leftover = records
    if len(records) > batch_size:
        firehose.put_record_batch(
            DeliveryStreamName=stream_name,
            Records=records
        )
        leftover = []

    return leftover

if __name__ == '__main__':
    import sys

    logging.basicConfig()
    logger.setLevel(logging.DEBUG)