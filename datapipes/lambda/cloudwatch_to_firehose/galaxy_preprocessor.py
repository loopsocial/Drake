from __future__ import print_function

import json
import zlib
import boto3
import os
import logging

# GALAXY_CONTROLLER_TAGS = ('response_log=', 'event_tracking=')
log_config = {
    'response_log=': {
        'stream_name': os.environ.get('RESPONSE_DELIVERY_STREAM_NAME', "SandboxResponseToS3"),
        'batch_size': os.environ.get('DELIVERY_STREAM_BATCH_SIZE', 499)
    },
    'event_tracking=': {
        'stream_name': os.environ.get('EVENT_TRACKING_DELIVERY_STREAM_NAME', "SandboxTrackingToS3"),
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
    Exclude health check later
    FluentD format:
    {
        "log": "2017-07-31 21:27:14.326 request_id=8ilinul11ucajst6qcpqeqjlurdsaihd [info] response_log={\"status\":200,\"requested_at\":\"2017-07-31T21:27:14.326013Z\",\"request_id\":\"8ilinul11ucajst6qcpqeqjlurdsaihd\",\"remote_ip\":\"10.42.52.226\",\"path\":\"/health_check\",\"params\":{},\"method\":\"GET\"}\n",
        "stream": "stdout",
        "container_id": "aa953de5b6690e62a52b04d17a4bac5c379db5f192ca34705dd2a46f1d344179",
        "container_name": "r-master-galaxy-1-7910a1a2"
    }

    :param log_line: a JSON string log line sent from FluentD
    :return: A tuple or None if not found. Returns tag for the json, and the json part of the line.
    """
    json_log = (None, None)
    fluentd_log_key = "log"
    log_dict = json.loads(log_line)

    if fluentd_log_key in log_dict and HEALTH_CHECK not in log_dict[fluentd_log_key]:
        tag = _log_has_tags_of_interest(log_config.keys(), log_dict[fluentd_log_key])

        if tag:
            (_, _, json_str) = log_dict[fluentd_log_key].partition(tag)

            # make sure it's a valid json
            try:
                json.loads(json_str)
                json_log = (tag, json_str)
            except ValueError:
                logger.warn("Invalid json found: " + log_dict[fluentd_log_key])

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
    # logger.debug(json.dumps(payload, indent=4, sort_keys=True))
    records = dict((t, []) for t in log_config.keys())

    for log_event in payload['logEvents']:
        json_log = extract_controller_json_str(log_event['message'])

        if all(json_log):
            logger.debug("====" + str(json_log))

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
    log_event = {
        "awslogs": {
            "data": "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"
        }
    }

    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        with open(input_file, 'r') as f:
            data = f.read()
        event = json.loads(data)

    lambda_handler(log_event, None)