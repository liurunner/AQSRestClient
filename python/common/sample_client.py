import json
import ntpath
import urllib
import uuid

from common_logging import CommonLogging
from rest_client import RestClient

__CREATED_BY__ = 'Created by AQSRestClient'


class SampleClient(object):
    def __init__(self, token, base_url):
        assert token is not None, 'token is required to instantiate SampleClient'
        assert base_url is not None, 'base_url is required to instantiate SampleClient'

        self.logger = CommonLogging.get_logger("SampleClient")
        self.token = token
        self.base_url = base_url

        self.logger.info('token: %s', self.token)
        self.logger.info('baseUrl: %s', self.base_url)

        self.rest_client = RestClient()
        self.rest_client.set_default_headers({
            'Content-Type': 'application/json',
            'Authorization': 'token ' + self.token
        })
        if 'debug.gaiaserve.net' in self.base_url:
            self.rest_client.set_verify('../resources/gaia-sm-ca-chain.cert.pem')
        elif '.gaiaserve.net' in self.base_url:
            self.rest_client.set_verify(True)
            self.rest_client.set_cert(('../resources/gaiaserve-net.cert.pem', '../resources/gaiaserve-net.key.pem'))
        elif '.aqsamples.com' in self.base_url:
            self.rest_client.set_verify(True)

    def check_availability(self):
        response = self.rest_client.get(self.get_url('status'))
        status_object = json.loads(response.text)
        if 'releaseName' not in status_object:
            raise RuntimeError('Target sample tenant {0} is not available.'.format(self.base_url))
        return status_object

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

    '''Generic domain object methods'''
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

    '''Specific domain object methods'''

    def get_or_create_sampling_location(self, sampling_location_overrides):
        assert 'customId' in sampling_location_overrides, 'customId is required from sampling_location_overrides'

        sampling_location = self.get_domain_object_by_custom_id(
            'samplinglocations', sampling_location_overrides['customId'])
        if sampling_location is None:
            sampling_location_to_post = SampleClient.make_sampling_location(sampling_location_overrides)
            sampling_location = self.post_domain_object('samplinglocations', sampling_location_to_post)
            self.logger.debug('Posted SamplingLocation %s', sampling_location)
        return sampling_location

    def get_or_create_field_visit(self, field_visit_overrides):
        assert 'customId' in field_visit_overrides, 'customId is required from field_visit_override'
        assert 'samplingLocation' in field_visit_overrides, 'samplingLocation is required from field_visit_override'

        field_visit = self.get_domain_object_by_custom_id('fieldvisits', field_visit_overrides['customId'])
        if field_visit is None:
            field_visit_to_post = SampleClient.make_field_visit(field_visit_overrides)
            field_visit = self.post_domain_object('fieldvisits', field_visit_to_post)
            self.logger.debug('Posted FieldVisit %s', field_visit)
        return field_visit

    def get_or_create_activity(self, activity_overrides):
        assert 'customId' in activity_overrides, 'customId is required from activity_override'

        activity = self.get_domain_object_by_custom_id('activities', activity_overrides['customId'])
        if activity is None:
            activity_to_post = SampleClient.make_activity(activity_overrides)
            activity = self.post_domain_object('activities', activity_to_post)
            self.logger.debug('Posted Activity %s', activity)
        return activity

    def delete_observations(self, params):
        url = self.get_url('observations', params=params)
        return self.rest_client.delete(url)

    def delete_field_visits_by_sampling_location_id(self, sampling_location_id):
        search_result = self.get_search_result('fieldvisits', {
            'samplingLocationIds': sampling_location_id
        })
        for field_visit in search_result['domainObjects']:
            self.delete_activities_by_field_visit_id(field_visit['id'])
            self.delete_domain_object_by_id('fieldvisits', field_visit['id'])

    def delete_activities_by_field_visit_id(self, field_visit_id):
        search_result = self.get_search_result('activities', {'fieldVisitId': field_visit_id})
        for activity in search_result['domainObjects']:
            self.delete_domain_object_by_id('activities', activity['id'])

    '''Static methods'''

    @staticmethod
    def url_join(base_url, *paths):
        url = base_url
        for path in paths:
            url = url + path if url.endswith('/') else url + '/' + path
        return url

    @staticmethod
    def get_overrides_value(overrides, key, default_value):
        return default_value if key not in overrides else overrides[key]

    @staticmethod
    def make_sampling_location(overrides={}):
        custom_id = SampleClient.get_overrides_value(overrides, 'customId', str(uuid.uuid4()))

        sampling_location = {
            'customId': custom_id,
            'name': custom_id,
            'type': 'RIVER',
            'latitude': '49.2061028',
            'longitude': '-123.1504412',
            'horizontalDatum': 'NAD83',
            'verticalDatum': 'NAVD88',
            'horizontalCollectionMethod': 'GPS-Unspecified',
            'verticalCollectionMethod': 'Precise Leveling-Bench mark',
            'description': __CREATED_BY__
        }
        sampling_location.update(overrides)

        return sampling_location

    @staticmethod
    def make_field_visit(overrides={}):
        sampling_location = SampleClient.get_overrides_value(overrides, 'samplingLocation', SampleClient.make_sampling_location())
        custom_id = SampleClient.get_overrides_value(overrides, 'customId', str(uuid.uuid4()))

        field_visit = {
            'samplingLocation': sampling_location,
            'customId': custom_id,
            'startTime': '2014-10-29T09:00:00.000-07:00',
            'endTime': '2014-10-29T17:00:00.000-07:00',
            'planningStatus': 'DONE',
            'notes': __CREATED_BY__
        }
        field_visit.update(overrides)

        return field_visit

    @staticmethod
    def make_activity(overrides={}):
        field_visit = SampleClient.get_overrides_value(overrides, 'fieldVisit', SampleClient.make_field_visit())
        custom_id = SampleClient.get_overrides_value(overrides, 'customId', str(uuid.uuid4()))
        activity_type = SampleClient.get_overrides_value(overrides, 'type', 'SAMPLE_ROUTINE')

        activity = {
            'samplingLocation': field_visit['samplingLocation'],
            'fieldVisit': field_visit,
            'customId': custom_id,
            'type': activity_type,
            'medium': 'WATER',
            'startTime': field_visit['startTime'],
            'endTime': field_visit['endTime'],
            'comment': __CREATED_BY__
        }
        activity.update(overrides)

        return activity
