from datapipes.aws_lambda.cloudwatch_to_firehose.naboo_preprocessor import *
import unittest
import json


class TestNabooPreprocessor(unittest.TestCase):

    def setUp(self):
        self.complete_log_event_message = "18:12:17.594 [info] feed_event={\"ts\":\"2018-03-13T01:12:17.594577Z\"," \
                                          "\"props\":{\"video_id\":79,\"user_id\":1,\"id\":1239},\"event\":" \
                                          "\"show_video\"}"

        self.partitioned_log_event_message = "{\"ts\":\"2018-03-13T01:12:17.594577Z\",\"props\":{\"video_id\":79," \
                                             "\"user_id\":1,\"id\":1239},\"event\":\"show_video\"}"

        with open('./naboo_test_assets/sample_log_events.json') as json_data:
            self.log_events = json.load(json_data)

        with open('./naboo_test_assets/sample_log_config.json') as json_data:
            self.log_config = json.load(json_data)

    def test_extract_controller_json_str(self):
        (tag, json_str) = extract_controller_json_str(self.log_events[0]["message"])

        # return equal when 'json_str' is compared to a partition of the entire log message
        self.assertEqual(json_str, self.partitioned_log_event_message)

        # return not equal when 'json_str' is compared to a complete single log message
        self.assertNotEqual(json_str, self.complete_log_event_message)

    def test_log_has_tags_of_interest(self):
        # return tag if it's found in the log message
        self.assertTrue(log_has_tags_of_interest(self.log_config.keys(), self.log_events[0]["message"]) is not None)

        # return none if none of the tags is found in the log message
        self.assertIsNone(
            log_has_tags_of_interest(["tracking_event=", "user_behavior="], self.log_events[0]["message"]))


if __name__ == "__main__":
    unittest.main()
