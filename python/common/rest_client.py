import json

import requests

from .common_logging import CommonLogging


class RestClient(object):
    def __init__(self):
        self.logger = CommonLogging.get_logger("RestClient")
        self.response = None
        self.default_headers = None
        self.verify = False
        self.cert = None

    def set_verify(self, verify):
        self.verify = verify

    # Required if target server using self-signed certs and the verify is True
    # E.g. set_cert(('/path/client.cert', '/path/client.key'))
    def set_cert(self, cert):
        self.cert = cert

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

        response_object = json.loads(self.response.text)
        if self.response.status_code == 409 and 'errorCode' not in response_object:
            # Import cases
            return

        if 'errorCode' in response_object:
            message = 'request failed due to code {0}: {1}'.format(response_object['errorCode'], response_object['message'])
            self.logger.error(message)
            self.logger.error(response_object['stackTrace'])
            raise RuntimeError(message)
        else:
            raise RuntimeError(self.response.reason)

    def get(self, url, headers=None):
        self.logger.debug('get: %s', url)
        self.response = requests.get(
            url, headers=self.__get_headers(headers), verify=self.verify, cert=self.cert)
        self.handle_error()
        return self.response

    def post(self, url, data=None, headers=None):
        self.logger.debug('post: %s', url)
        self.response = requests.post(
            url, headers=self.__get_headers(headers), data=self.__get_data(data), verify=self.verify, cert=self.cert)
        self.handle_error()
        return self.response

    def put(self, url, data=None, headers=None):
        self.logger.debug('put: %s', url)
        self.response = requests.put(
            url, headers=self.__get_headers(headers), data=self.__get_data(data), verify=self.verify, cert=self.cert)
        self.handle_error()
        return self.response

    def delete(self, url, headers=None):
        self.logger.debug('delete: %s', url)
        self.response = requests.delete(
            url, headers=self.__get_headers(headers), verify=self.verify, cert=self.cert)
        self.handle_error()
        return self.response

    def post_file(self, url, files):
        self.logger.debug('post files: %s', url)
        self.response = requests.post(url, files=files, verify=self.verify, cert=self.cert)
        self.handle_error()
        return self.response
