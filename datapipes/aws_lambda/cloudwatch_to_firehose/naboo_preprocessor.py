from __future__ import print_function

import json
import zlib
import boto3
import os
import logging
import base64

# environment variables
log_config = {
    'response_log=': {
        'stream_name': os.environ.get('RESPONSE_DELIVERY_STREAM_NAME', "NabooDevResponseToS3"),
        'batch_size': os.environ.get('DELIVERY_STREAM_BATCH_SIZE', 499)
    },
    'feed_event=': {
        'stream_name': os.environ.get('EVENT_TRACKING_DELIVERY_STREAM_NAME', "NabooDevFeedEventToS3"),
        'batch_size': os.environ.get('DELIVERY_STREAM_BATCH_SIZE', 499)
    },
}
log_stream_name = os.environ.get('LOG_STREAM_NAME', "test-stream")
debug_mode = os.environ.get('DEBUG_MODE', False)

HEALTH_CHECK = 'health_check'
firehose = boto3.client('firehose', region_name='us-west-2')

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def extract_controller_json_str(log_line):
    """
    Log format:
    {
        "logEvents": [
            {
                'id': '33932602058168217326485955733116808031206164863324984556',
                'timestamp': 1521590500949,
                'message': '18:12:17.594 [info] feed_event={"ts":"2018-03-13T01:12:17.594577Z","props":{"video_id":79,
                            "user_id":1,"id":1239},"event":"show_video"}'
            },
            {
                'id': '33932602058168217326485955733116808031206164863324984557',
                'timestamp': 1521590500949,
                'message': '18:12:17.594 [info] feed_event={"ts":"2018-03-13T01:12:17.594655Z","props":{"watched_till"
                    :23,"video_id":79,"user_id":1,"inserted_at":"2018-03-13T01:12:17.594627Z","id":null,"duration":23
                    ,"completed":true},"event":"engage_video"}'
            },
            {
                'id': '33932602058168217326485955733116808031206164863324984558',
                'timestamp': 1521590500949,
                'message': '18:12:17.594 [debug] QUERY OK db=0.1ms'
            },
            {
                'id': '33932602058168217326485955733116808031206164863324984559',
                'timestamp': 1521590500949,
                'message': 'begin []'
            },
            {
                'id': '33932602058168217326485955733116808031206164863324984560',
                'timestamp': 1521590500949,
                'message': '18:12:17.595 [debug] QUERY OK source="videos" db=0.2ms'
            }
        ],
        "logGroup": "shawn-test",
        "logStream": "test-stream",
        "messageType": "DATA_MESSAGE",
        "owner": "",
        "subscriptionFilters": [
        ]
    }
    :param log_line: a JSON string log line sent from ASG
    :return: A tuple or None if not found. Returns tag for the json, and the json part of the line.
    """

    json_log = (None, None)

    if HEALTH_CHECK not in log_line:
        tag = log_has_tags_of_interest(log_config.keys(), log_line)

        if tag:
            (_, _, json_str) = log_line.partition(tag)

            # make sure it's a valid json
            try:
                json.loads(json_str)
                json_log = (tag, json_str)
            except ValueError:
                logger.warn("Invalid json found: " + log_line)

    return json_log


def log_has_tags_of_interest(tags, line):
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
    # TODO: data stream is not being chunked under python 3.6
    payload = json.loads(zlib.decompress(base64.b64decode(stream), 32 + zlib.MAX_WBITS).decode('utf-8'))
    records = dict((t, []) for t in log_config.keys())

    if not payload["logStream"] or log_stream_name != payload["logStream"]:
        return

    extract_and_push_records(records, payload)
    flush_records(records)
    return True


def extract_and_push_records(records, payload):
    """
    loop through every log event within the event log, extract the actual log message, reformat into compatible
    format and push to firehose

    :param records:
        In the format of:
        {
            'response_log=': [],
            'feed_event=': [
                {
                    'Data': '{"ts":"2018-03-13T01:12:17.605578Z","props":{"watched_till":21,"video_id":79,"user_id":31,
                        "inserted_at":"2018-03-13T01:12:17.605538Z","id":null,"duration":23,"completed":false},
                        "event":"engage_video"}'
                }
            ]
        }
    :param payload:
        In the format of:
        {
            'messageType': 'DATA_MESSAGE',
            'owner': '170553093583',
            'logGroup': 'shawn-test',
            'logStream': 'test-stream',
            'subscriptionFilters': ['LambdaStream_naboo-preprocessor'],
            'logEvents':
                [
                    {
                        'id': '33932602058168217326485955733116808031206164863324984556',
                        'timestamp': 1521590500949,
                        'message': '18:12:17.594 [info] feed_event={"ts":"2018-03-13T01:12:17.594577Z","props":
                            {"video_id":79,"user_id":1,"id":1239},"event":"show_video"}'
                    },
                    ...
                ]
        }
    :return:
    """
    for log_event in payload['logEvents']:
        json_log = extract_controller_json_str(log_event['message'])

        if all(json_log):
            if debug_mode:
                logger.info("====" + str(json_log))
            (tag, json_str) = json_log
            records[tag].append({'Data': json_str})
            records[tag] = write_records(
                log_config[tag]['stream_name'],
                records[tag],
                log_config[tag]['batch_size'])


def flush_records(records):
    """
    If there's leftover records after the preprocessing, send all of them in a batch to firehose
    before existing the pipeline

    :param records: a list of objects in the format of
        {
           "DeliveryStreamName": "string",
           "Records": [
              {
                 "Data": blob
              }
           ]
        }
    :return:
    """
    if records is not []:
        for tag in log_config.keys():
            write_records(
                log_config[tag]['stream_name'],
                records[tag],
                0)


def write_records(stream_name, records, batch_size):
    leftover = records
    if len(records) > int(batch_size):
        firehose.put_record_batch(
            DeliveryStreamName=stream_name,
            Records=records
        )
        leftover = []

    return leftover
