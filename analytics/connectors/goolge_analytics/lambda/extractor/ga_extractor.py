"""
    Google Analytics API Extractors

    To get data, simply create a GoogleAnalyticsAPIExtractorBase and write your query in get_api_query().

    TODO Move to 3.6
"""
from __future__ import print_function
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from collections import OrderedDict, defaultdict
import boto3
import os, sys, errno, io
from abc import ABCMeta, abstractmethod, abstractproperty
from time import gmtime, strftime
import csv, gzip
import ast, json
import logging
from extractor_util import ExtractorUtil

ENV = os.environ.get('ENV', 'DEV')
DEV_MODE = True if ENV == 'DEV' else False
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    logger.setLevel(logging.INFO)

    try:
        try:
            extractors = [
                GADailyUsersNewUsersBounceRateBySocialNetwork(SimpleDatetimeOutputWriter(DEV_MODE)),
                GAHourlyUsersPageviewsByPagepath(SimpleDatetimeOutputWriter(DEV_MODE))
            ]

            for analytics in extractors:
                analytics.process()

        except Exception as e:
            logger.error("Google Analytics Extractor failed.")
            logger.error(str(e))

    except Exception as e:
        logger.error("Failed to clean up temporary files.")
        logger.error(str(e))
        return False

    return True


class GoogleAnalyticsAPIExtractorBase:
    __metaclass__ = ABCMeta

    def __init__(self, writer):

        # set up context and clean up
        self.analytics_context = self.create_analytics_context()
        self.output_writer = writer
        self.output_writer.clean_up()

    @abstractproperty
    def report_type(self):
        raise NotImplementedError

    @abstractmethod
    def get_api_query(self):
        pass

    def process(self):

        # Call Google Analytics API
        response = self.fetch_report_data()

        # Parse results
        self.format_and_save_report_data(response)

        # Push to S3 for downstream uses (Athena, QuickInsights, etc)
        self.push_data()

        # clean up temporary files
        self.output_writer.clean_up()

    def format_and_save_report_data(self, response):
        """
        Parses and save the Analytics Reporting API V4 response.

        :param response: Results from API
        :return: None
        :raise Exception: Throw an error if writer is not initialized
        """

        if not self.output_writer:
            raise Exception('Invalid writer passed in for writing fetcher results.')

        for report in response.get('reports', []):
            column_header = report.get('columnHeader', {})
            dimension_headers = column_header.get('dimensions', [])
            metric_headers = column_header.get('metricHeader', {}).get('metricHeaderEntries', [])

            tabular_data = OrderedDict((d, []) for d in dimension_headers)
            for m in metric_headers:
                tabular_data[m['name']] = []
            logger.debug(tabular_data)

            for row in report.get('data', {}).get('rows', []):
                dimensions = row.get('dimensions', [])
                date_range_values = row.get('metrics', [])

                # Dimensions
                for header, dimension in zip(dimension_headers, dimensions):
                    tabular_data[header].append(ExtractorUtil.parse_ga_field_value(header, dimension))

                # Metrics
                for i, values in enumerate(date_range_values):
                    for header, value in zip(metric_headers, values.get('values')):
                        tabular_data[header.get('name')].append(ExtractorUtil.parse_ga_field_value(header.get('name'), value))

            logger.debug(" ".join(tabular_data.keys()))
            num_of_rows = len(tabular_data[dimension_headers[0]])
            for i in range(num_of_rows):
                row = [tabular_data[k][i] for k in tabular_data.keys()]
                date = tabular_data['ga:date'][i] if 'ga:date' in tabular_data else None
                hour = tabular_data['ga:hour'][i] if 'ga:hour' in tabular_data else None

                self.output_writer.write_data(row,
                                              self.report_type,
                                              date=date,
                                              hour=hour)

    def push_data(self):
        """
        Purging data to its final destination(s).

        TODO Create directory structure on S3.
        TODO Check to see if we need some kind of cron lock
        """

        self.output_writer.push_data(self.report_type)
        # self.output_writer.clean_up()

    def create_analytics_context(self):
        """Initializes an Analytics Reporting API V4 service object.

        Returns:
          An authorized Analytics Reporting API V4 service object.
        """

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.KEY_FILE_LOCATION, self.SCOPES)

        # Build the service object.
        analytics = build('analyticsreporting', 'v4', credentials=credentials)

        return analytics

    def fetch_report_data(self):
        """
        Wrapping metrics and dimensions needed for the query to fetch for the google analytics data.

        Returns:
          The Analytics Reporting API V4 response.
        """

        return self.analytics_context.reports().batchGet(
            body={
                'reportRequests': self.get_api_query()
            }
        ).execute()


class GAHourlyUsersPageviewsByPagepath(GoogleAnalyticsAPIExtractorBase):
    # TODO Move to S3 config
    SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
    KEY_FILE_LOCATION = 'Loop-a298a758fb8f.json'
    VIEW_ID = '146839166'

    def __init__(self, writer):
        # Pass writer to parent
        super(GAHourlyUsersPageviewsByPagepath, self).__init__(writer)

    @property
    def report_type(self):
        return self.__class__.__name__

    def get_api_query(self):
        return [
            {
                'viewId': self.VIEW_ID,
                'dateRanges': [{'startDate': '2daysAgo', 'endDate': 'today'}],
                'metrics': [{'expression': 'ga:uniquePageviews'},
                            {'expression': 'ga:sessions'},
                            {"expression": "ga:pageviews"},
                            {"expression": "ga:users"}],
                'dimensions': [{'name': 'ga:date'},
                               {'name': 'ga:hour'},
                               {'name': 'ga:sessionCount'},
                               {"name": "ga:pagePath"}]
            }]


class GADailyUsersNewUsersBounceRateBySocialNetwork(GoogleAnalyticsAPIExtractorBase):
    # TODO Move to S3 config
    SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
    KEY_FILE_LOCATION = 'Loop-a298a758fb8f.json'
    VIEW_ID = '146839166'

    def __init__(self, writer):
        # Pass writer to parent
        super(GADailyUsersNewUsersBounceRateBySocialNetwork, self).__init__(writer)

    @property
    def report_type(self):
        return self.__class__.__name__

    def get_api_query(self):
        return [
            {
                'viewId': self.VIEW_ID,
                'dateRanges': [{'startDate': '2daysAgo', 'endDate': 'today'}],
                'metrics': [{"expression": "ga:newUsers"},
                            {"expression": "ga:users"},
                            {'expression': 'ga:bounceRate'}],
                'dimensions': [{'name': 'ga:date'},
                               {'name': 'ga:socialNetwork'}]
            }]


class DatetimeFileHandler(object):
    # TODO move to s3 config
    DATA_FILE_OUTPUT_DIR = '/tmp/'
    DATA_FILE_DEFAULT_PREFIX = 'GA_'
    DATA_FILE_EXT = '.csv.gz'

    def __init__(self):
        self.default_date = strftime("%Y%m%d", gmtime())
        self.default_hour = strftime("%H", gmtime())
        self.data_files = defaultdict(lambda: defaultdict(lambda: None))
        self.writers = defaultdict(lambda: defaultdict(lambda: None))

    def get_writer(self, report_type='', date=None, hour=None):
        """
        Get the data writer, given the date and hour.
        Only CSV is supported and returning a csv.writer explicitly

        :param report_type: Report type name. This should be different for each analytics query type.
        :param date: String - YYMMDD
        :param hour: String - HH
        :return: csv.writer
        """

        # if not time-sensitive
        if not date and not hour:
            if not self.writers[self.default_date][self.default_hour]:
                self.writers[self.default_date][self.default_hour] = \
                    csv.writer(self.__get_file(
                        report_type=report_type, date=self.default_date, hour=self.default_hour),
                        lineterminator='\n')

            writer = self.writers[self.default_date][self.default_hour]
        else:
            if not self.writers[date][hour]:
                self.writers[date][hour] = csv.writer(self.__get_file(
                    report_type=report_type, date=date, hour=hour),
                    lineterminator='\n')
            writer = self.writers[date][hour]

        return writer

    def __get_file(self, report_type='', date=None, hour=None):
        """
        Get or create the data writer, given the date and hour.
        File schema is {report_type}_YYMMDDHH.EXTENSION.

        :param report_type: Report type name
        :param date: String - YYMMDD
        :param hour: String - HH
        :return: a file object
        """

        # Check if files are time sensitive or not
        if not date and not hour:
            if not self.data_files[self.default_date][self.default_hour]:
                fp = gzip.open(self._get_file_path(report_type=report_type, date=date, hour=hour), 'wb')
                self.data_files[self.default_date][self.default_hour] = fp
        else:
            if not self.data_files[date][hour]:
                fp = gzip.open(self._get_file_path(report_type=report_type, date=date, hour=hour), 'wb')
                self.data_files[date][hour] = fp

        return fp

    def _get_file_path(self, report_type='', date=None, hour=None):
        file_prefix = report_type + '_' if report_type else self.DATA_FILE_DEFAULT_PREFIX
        date = self.default_date if not date else date
        hour = '' if not hour else '-' + hour

        # no directory structure
        return self.DATA_FILE_OUTPUT_DIR + file_prefix + date + hour + self.DATA_FILE_EXT

    def get_filer_handles(self):
        return [f for date, h in self.data_files.iteritems() for hour, f in h.iteritems()]

    def write_row(self, report_type, date, hour, data):
        self.get_writer(report_type, date, hour).writerow(data)

    def clean_up(self):
        """
        Remove all files this file handler has kept track of
        """

        for date, hour, f in [(date, hour, f) for date, h in self.data_files.iteritems() for hour, f in h.iteritems()]:
            os.remove(f.name)

        # less del operations
        map(lambda key: self.data_files.pop(key), self.data_files.keys())
        map(lambda key: self.writers.pop(key), self.writers.keys())

    def has_files_to_process(self):

        if not self.data_files:
            print("Nothing uploaded to S3")
            return False

        return len(self.get_filer_handles()) > 0


class SimpleDatetimeOutputWriter(object):
    """
    High level IO abstraction.
    """
    S3_GOOGLE_ANALYTICS_BASE_PATH = 'google_analytics'
    S3_LOG_BUCKET = 'loop-logs'
    s3 = boto3.resource('s3')
    s3_client = boto3.client('s3')

    def __init__(self, dev_mode=False):

        self.file_handler = DatetimeFileHandler()
        self.dev_mode = dev_mode

    def write_data(self, data, report_type='', date=None, hour=None):
        """
        The only choice is CVS and no buffering for now.
        Lazily create the file and csv writer object.

        :param data: String list of data to be written to CSV
        :param report_type:
        :param date: YYYYMMDD of the data getting pulled out from Google Analtyics (not the data generation date)
        :param hour: HH 24-hour based String

        :raise Exception: Throw an error if only passed in Hour without Date

        :return: return code if any from the target source. Raise exception on error.
        """

        if not date and hour:
            raise Exception('Cannot pass in hour only')

        if self.dev_mode:
            out = ", ".join(data) if isinstance(data, list) else data
            sys.stdout.write(out + "\n")

        return self.file_handler.write_row(report_type, date, hour, data)

    def push_data(self, report_type=''):
        """
        Purging data to its final destination(s).

        TODO Create directory structure on S3.
        """
        if not self.file_handler.has_files_to_process():
            logger.warning("Nothing uploaded to S3")
            return None

        report_type = '/test/' if not report_type else "/" + report_type

        for f in self.file_handler.get_filer_handles():
            f.flush()
            f.close()

            # TODO Add directory structure and partition later
            if not self.dev_mode:
                print("MODE: " + str(self.dev_mode) + " " + f.name)

                self.s3_client.upload_file(f.name,
                                           self.S3_LOG_BUCKET,
                                           self.S3_GOOGLE_ANALYTICS_BASE_PATH +
                                           report_type + '/' + os.path.basename(f.name))

        if not self.dev_mode:
            bucket = self.s3.Bucket(self.S3_LOG_BUCKET)
            for key in bucket.objects.filter(Prefix=self.S3_GOOGLE_ANALYTICS_BASE_PATH):
                logger.debug(key)

    def clean_up(self):
        self.file_handler.clean_up()
