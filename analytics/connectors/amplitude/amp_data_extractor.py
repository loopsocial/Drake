#! /usr/bin/python
#-*- coding:utf-8 -*-

'''
Author(s): Yi Jin
File: amp_data_extractor.py
Creation: 2017/08/08
'''

import fnmatch
import ConfigParser
import os
import requests
import shutil
import zipfile

from requests.auth import HTTPBasicAuth
from shutil import copyfile

class AmpDataExtractor(object):

    def __init__(self, config_file):

        self._config = ConfigParser.ConfigParser()
        self._config.read(config_file)

        self._url = self._config.get("AMP", "URL")
        self._api_key = self._config.get("AMP", "API_KEY")
        self._api_secret = self._config.get("AMP", "API_SECRET")
        self._output_path = self._config.get("AMP", "OUTPUT_PATH")

        self._temp_zip_file = "test_requests.zip"
        self._temp_unzipped_path = "temp_unzipped_files"
        self._file_formats = (".gz")

    def process(self, start_date, end_date):

        # step 1: get response from Amplitude API
        resp = self.__get_response_from_amp_api(start_date, end_date)

        # step 2: write response to temporary zip file
        self.__write_resp_to_temp_zip_file(resp, self._temp_zip_file)

        # step 3: unzip the temp_zip_file and extract files into temp_unzipped_path
        self.__unzip_extract_file(self._temp_zip_file, self._temp_unzipped_path)

        # step 4: extract gz_files from the unzipped_path
        gz_files = self.__get_gz_files_from_unzipped_path(self._temp_unzipped_path, self._file_formats)

        # step 5: create the output paths
        self.__create_output_path(self._output_path)

        # step 6: move the gz files into the output path and remove temporary path
        self.__move_files_and_remove_temp_path(gz_files, self._temp_unzipped_path, self._output_path)

        # step 7: removes the temporary zip file
        self.__remove_temp_zip_file(self._temp_zip_file)

    def __get_response_from_amp_api(self, start_date, end_date):
        '''
        :objective: this sub-function will return the response from the amplitude api
        :param start_date: YYYYMMDD
        :param end_date: YYYYMMDD
        :return: the response from the amplitude api
        '''
        url = self._url % (start_date, end_date)
        resp = requests.get(url, auth=HTTPBasicAuth(self._api_key,
                                                    self._api_secret))
        return resp

    def __write_resp_to_temp_zip_file(self, resp, temp_zip_file):
        '''
        :objective: this sub-function will write the response to temporary zip file
        :param resp: response from the Amplitude API
        :return: writes the response to file
        '''
        if resp.status_code == 200:
            with open(temp_zip_file, "wb") as code:
                code.write(resp.content)

    def __unzip_extract_file(self, temp_zip_file, temp_unzipped_path):
        '''
        :objective: this sub-function will unzip and extract files from the temp_zip_file and
        :           save into temp_unzipped_path
        :param temp_zip_file:
        :param temp_unzipped_path:
        :return: saves the extract files from temp_zip_file
        '''
        zip_ref = zipfile.ZipFile(temp_zip_file)
        zip_ref.extractall(temp_unzipped_path)

    def __get_gz_files_from_unzipped_path(self, temp_unzipped_path, file_formats):
        '''
        :objective: this sub-function will recursively walk the unzipped path and extract all gz files
        :param temp_unzipped_path: the temporary unzipped path
        :param file_formats: set of file formats
        :return: a list of gz file paths
        '''
        matches = []
        for root, dir_name, file_names in os.walk(temp_unzipped_path):
            for file_name in file_names:
                if file_name.endswith(file_formats):
                    matches.append( os.path.join(".", root, file_name))
        return matches

    def __create_output_path(self, output_path):
        '''
        :objective: this sub-function will create the output_path
        :param output_path: output path
        :return: creates the output path
        '''
        if not os.path.exists(output_path):
            os.makedirs(output_path)

    def __move_files_and_remove_temp_path(self, matched_files, temp_path, output_path):
        '''
        :objective: this sub-function will modify the file names of the list of matched files
        :param matched_files: list of matched files
        :param temp_path: the temporary unzipped path
        :param output_path: the final output path
        :return: list of renamed matched files
        '''
        amp_id, amp_name, amp_blacklist = "165789", "AMP", "#47"
        rename_patterns = [ (amp_id, amp_name),
                            (amp_blacklist, "")]
        for f in matched_files:
            new_f = f.split("/")[-1].strip()
            for val, replace_val in rename_patterns:
                new_f = new_f.replace(val, replace_val)
            new_f = os.path.join(".", output_path, new_f)
            shutil.move(f, new_f)

        shutil.rmtree(temp_path)

    def __remove_temp_zip_file(self, temp_zip_file):
        '''
        :objective: this sub-function will remove the temp_zip_file
        :param temp_zip_file: temporary zip file
        :return: removes the temp_zip_file
        '''
        os.remove(temp_zip_file)


def unit_test(start_date, end_date):
    '''
    :objective: unit test for Amp Data Extractor
    :param start_date: start date
    :param end_date: end ate
    :return: runs the data extractor script
    '''
    AmpDataExtractor(config_file = "amp.cfg").process(start_date, end_date)

if __name__ == "__main__":
    unit_test(
        start_date="20170801",
        end_date="20170808"
    )