from unittest import TestCase
from extractor.extractor_util import ExtractorUtil


class TestExtractorUtil(TestCase):
    def test_google_analytics_date_to_athena_date(self):
        self.assertEquals(ExtractorUtil.parse_ga_field_value("ga:date", "20180808"), "2018-08-08")

    def test_parse_ga_field_value(self):
        self.assertEquals(ExtractorUtil.parse_ga_field_value("ga:socialNetwork", "(not set)"), "Unknown")
        self.assertEquals(ExtractorUtil.parse_ga_field_value("ga:someRate", u"99.222"), "99.2")
        self.assertEquals(ExtractorUtil.parse_ga_field_value("ga:someRate", "99.222"), "99.2")