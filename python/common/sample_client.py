import json
import ntpath
import urllib
import uuid

from common_logging import CommonLogging
from rest_client import RestClient


class SampleClient(object):
    def __init__(self, token, base_url, certs=None):
        self.logger = CommonLogging.get_logger("SampleClient")

        if token is None:
            raise RuntimeError('token is required to instantiate SampleClient')
        if base_url is None:
            raise RuntimeError('base_url is required to instantiate SampleClient')
        self.token = token
        self.base_url = base_url
        self.certs = certs

        self.logger.info('token: %s', self.token)
        self.logger.info('baseUrl: %s', self.base_url)

        self.rest_client = RestClient()
        self.rest_client.set_default_headers({
            'Content-Type': 'application/json',
            'Authorization': 'token ' + self.token
        })
        if self.certs is not None:
            self.rest_client.set_verify(self.certs)

    @staticmethod
    def url_join(base_url, *paths):
        url = base_url
        for path in paths:
            url = url + path if url.endswith('/') else url + '/' + path
        return url

    def get_url(self, list_path, params=None, domain_object_id=None, version='v1', with_token=False):
        url = SampleClient.url_join(self.base_url, version, list_path)
        if domain_object_id is not None:
            url = SampleClient.url_join(url, domain_object_id)

        query_params = {} if not with_token else {'token': self.token}
        if params is not None:
            query_params.update(params)
        if len(query_params) > 0:
            query_string_sep = '&' if '?' in url else '?'
            url += query_string_sep + urllib.urlencode(query_params)

        return url

    def get_search_result(self, list_path, params=None, version='v1'):
        url = self.get_url(list_path, params=params, version=version)
        response = self.rest_client.get(url)
        return json.loads(response.text)

    def get_domain_object_by_custom_id(self, list_path, custom_id, raise_error_when_custom_id_is_unused=False,
                                       version='v1'):
        search_result = self.get_search_result(list_path, params={'customId': custom_id}, version=version)
        for domain_object in search_result['domainObjects']:
            if custom_id == domain_object['customId']:
                return domain_object
        if raise_error_when_custom_id_is_unused:
            raise RuntimeError('domain object {0} with customId {1} is not exist.'.format(list_path, custom_id))
        return None

    def get_domain_object_by_id(self, list_path, domain_object_id, params=None, version='v1'):
        url = self.get_url(list_path, params=params, domain_object_id=domain_object_id, version=version)
        return self.rest_client.get(url)

    def post_domain_object(self, list_path, domain_object, params=None, version='v1'):
        url = self.get_url(list_path, params=params, version=version)
        response = self.rest_client.post(url, data=domain_object)
        domain_object = json.loads(response.text)
        return domain_object

    def put_domain_object(self, list_path, domain_object, params=None, version='v1'):
        domain_object_id = domain_object.get('id', None)
        if domain_object_id is None:
            domain_object['id'] = str(uuid.uuid4())

        url = self.get_url(list_path, params=params, domain_object_id=domain_object_id, version=version)
        response = self.rest_client.put(url, data=domain_object)
        domain_object = json.loads(response.text)
        return domain_object

    def delete(self, list_path, params=None, version='v1'):
        url = self.get_url(list_path, params=params, version=version)
        self.rest_client.delete(url)

    def delete_domain_object_by_id(self, list_path, domain_object_id, params=None, version='v1'):
        url = self.get_url(list_path, params=params, domain_object_id=domain_object_id, version=version)
        self.rest_client.delete(url)

    def import_file(self, path, filename, file_content=None, params=None, domain_object=None):
        url = self.get_url(path, params=params, with_token=True)
        multipart_data = {}
        if file_content is None:
            multipart_data['file'] = (ntpath.basename(filename), open(filename, mode='rb'))
        else:
            multipart_data['file'] = (filename, file_content)
        if domain_object is not None:
            multipart_data['domainObject'] = domain_object
        return self.rest_client.post_file(url, files=multipart_data)

    @staticmethod
    def make_sampling_location(custom_id, override={}):
        sampling_location = {
            'customId': custom_id,
            'name': custom_id,
            'type': 'RIVER',
            'latitude': '49.2061028',
            'longitude': '-123.1504412',
            'horizontalDatum': 'NAD83',
            'verticalDatum': 'NAVD88',
            'horizontalCollectionMethod': 'GPS-Unspecified',
            'verticalCollectionMethod': 'Precise Leveling-Bench mark'
        }
        sampling_location.update(override)
        return sampling_location

    @staticmethod
    def make_field_visit(sampling_location, custom_id, override={}):
        field_visit = {
            'samplingLocation': sampling_location,
            'customId': custom_id,
            'startTime': '2014-10-29T09:00:00.000-07:00',
            'endTime': '2014-10-29T17:00:00.000-07:00',
            'planningStatus': 'DONE'
        }
        field_visit.update(override)
        return field_visit

    @staticmethod
    def make_activity(field_visit, activity_type, custom_id, override={}):
        activity = {
            'samplingLocation': field_visit['samplingLocation'],
            'fieldVisit': field_visit,
            'customId': custom_id,
            'type': activity_type,
            'medium': 'WATER',
            'startTime': field_visit['startTime'],
            'endTime': field_visit['endTime']
        }
        activity.update(override)
        return activity
