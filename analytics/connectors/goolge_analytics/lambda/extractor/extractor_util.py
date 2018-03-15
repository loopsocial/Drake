"""Common extractor utility libraries.
"""
import datetime


class ExtractorUtil(object):
    """Static or class utility functions
    """

    @staticmethod
    def google_analytics_date_to_athena_date(date):
        """
        Convert a Google Analytics day YYYYMMDD to an Athena (hive) date YYYY-MM-DD

        :param date: Google Analytics date, which is YYYYMMDD
        :return: Athena-supported (Hive) date, which is YYYY-MM-DD
        """

        return datetime.datetime.strptime(date, "%Y%m%d").strftime('%Y-%m-%d')

    @classmethod
    def parse_ga_field_value(cls, header, value):
        """
        Rules for process Google Analytics values.

        Current rules:
        - Convert "(not set)" to "Unknown"
        - Round decimals to 1

        :param header: Name of the field
        :param value: Value of the given field
        :return: Converted value if any.
        """

        def notset_to_unknown(v):
            return v if v != '(not set)' else 'Unknown'

        def default(v):
            try:
                v = "%.1f" % round(float(v), 1) if '.' in v else v
            except:
                pass
            return notset_to_unknown(v)

        parser = {
            'ga:date': cls.google_analytics_date_to_athena_date,
            'ga:socialNetwork': notset_to_unknown
        }

        return parser.get(header, default)(value)


