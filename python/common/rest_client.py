import json

import requests

from common_logging import CommonLogging


class RestClient(object):
    def __init__(self):
        self.logger = CommonLogging.get_logger("RestClient")
        self.response = None
        self.default_headers = None
        self.verify = False

    def set_verify(self, verify):
        self.verify = verify

    def set_default_headers(self, headers):
        self.default_headers = headers

    def __get_headers(self, headers=None):
        request_headers = self.default_headers
        if headers is not None:
            request_headers = headers
            self.logger.debug('headers: %s', request_headers)
        return request_headers

    def __get_data(self, data=None):
        if data is None:
            return None
        return json.dumps(data)

    def handle_error(self):
        if 200 <= self.response.status_code < 300:
            return

        if self.response.status_code == 409 and '"errorCode"' not in self.response.text:
            # Import cases
            return

        if '"errorCode"' in self.response.text:
            error = json.loads(self.response.text)
            message = 'request failed due to code {0}: {1}'.format(error['errorCode'], error['message'])
            self.logger.error(message)
            self.logger.error(error['stackTrace'])
            raise RuntimeError(message)
        else:
            raise RuntimeError(self.response.reason)

    def get(self, url, headers=None):
        self.logger.debug('get: %s', url)
        self.response = requests.get(
            url, headers=self.__get_headers(headers), verify=self.verify)
        self.handle_error()
        return self.response

    def post(self, url, data=None, headers=None):
        self.logger.debug('post: %s', url)
        self.response = requests.post(
            url, headers=self.__get_headers(headers), data=self.__get_data(data), verify=self.verify)
        self.handle_error()
        return self.response

    def put(self, url, data=None, headers=None):
        self.logger.debug('put: %s', url)
        self.response = requests.put(
            url, headers=self.__get_headers(headers), data=self.__get_data(data), verify=self.verify)
        self.handle_error()
        return self.response

    def delete(self, url, headers=None):
        self.logger.debug('delete: %s', url)
        self.response = requests.delete(
            url, headers=self.__get_headers(headers), verify=self.verify)
        self.handle_error()
        return self.response

    def post_file(self, url, files):
        self.logger.debug('post files: %s', url)
        self.response = requests.post(url, files=files, verify=self.verify)
        self.handle_error()
        return self.response
