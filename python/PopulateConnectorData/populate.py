#!/usr/bin/python
# coding:utf-8

import getopt
import sys

from python import CommonLogging
from python import SampleClient

logger = CommonLogging.get_logger("main")


class AppConfig(object):
    def __init__(self):
        self.token = None
        self.base_url = None
        self.certs = '../resources/gaia-sm-ca-bundle.cert.pem'
        self.logFile = './client.log'

        self.load_command_line_options()

    def load_command_line_options(self):
        try:
            opts, args = getopt.getopt(sys.argv[1:], '', ['token=', 'host=', 'certs=', 'log'])
        except getopt.GetoptError as err:
            print str(err)
            sys.exit(2)

        if opts:
            for opt, arg in opts:
                if opt == '--token':
                    self.token = arg
                elif opt == '--host':
                    self.base_url = 'https://{0}/api/'.format(arg)
                elif opt == '--log':
                    self.logFile = arg
                elif opt == '--certs':
                    self.certs = arg
                else:
                    pass


class ConnectorPropagator(object):
    def __init__(self, sample_client):
        self.logger = CommonLogging.get_logger("ConnectorPropagator")
        self.sampling_locations = []
        self.exchange_configuration = None
        self.sample_client = sample_client

    def populate_locations(self, locations):
        for location in locations:
            sampling_location = self.sample_client.get_domain_object_by_custom_id('samplinglocations', location)
            if sampling_location is None:
                location_to_post = SampleClient.make_sampling_location(location)
                sampling_location = self.sample_client.post_domain_object('samplinglocations', location_to_post)

            self.sampling_locations.append(sampling_location)
        self.logger.info('location %s are populated.', locations)

    def __reset_exchange_configuration_setting_to_empty(self):
        self.exchange_configuration['settings'][:] = []
        self.exchange_configuration['samplingLocationMappings'][:] = []
        self.exchange_configuration['observationMappings'][:] = []
        self.exchange_configuration = self.sample_client.put_domain_object(
            'exchangeconfigurations', self.exchange_configuration)

    def __set_exchange_configuration_setting(self):
        self.exchange_configuration['settings'].append({'key': 'DEPTH_PARAMETER', 'value': 'DEPTH'})
        self.exchange_configuration['settings'].append({'key': 'DEPTH_PARAMETER_UNIT', 'value': 'ft'})
        self.exchange_configuration['settings'].append({'key': 'NON_DETECT_ALGORITHM', 'value': 'HALF_MDL'})
        self.exchange_configuration['settings'].append({'key': 'DEFAULT_LOCATION_PATH', 'value': 'Locations.AQS'})
        self.exchange_configuration['settings'].append({'key': 'DEFAULT_TIME_ZONE_OFFSET_HOURS', 'value': '-7'})

    def __set_exchange_configuration_location_mappings(self):
        for sampling_location in self.sampling_locations:
            self.exchange_configuration['samplingLocationMappings'].append({
                'samplingLocation': {
                    'id': sampling_location['id']
                },
                'externalLocation': sampling_location['customId']
            })

    def __add_observation_mapping(self, observed_property_custom_id, aqts_parameter_type, aqts_parameter_unit):
        observed_property = self.sample_client.get_domain_object_by_custom_id(
            'observedproperties', observed_property_custom_id, raise_error_when_custom_id_is_unused=True)
        self.exchange_configuration['observationMappings'].append({
            'observedProperty': {
                'id': observed_property['id']
            },
            'externalObservedProperty': aqts_parameter_type,
            'externalUnit': aqts_parameter_unit
        })

    def __set_exchange_configuration_observation_mappings(self):
        self.__add_observation_mapping('Ammonia', 'NH4NH3_dis', 'mg/l')
        self.__add_observation_mapping('Total Dissolved Solids', 'TDS', 'mg/l')
        self.__add_observation_mapping('Total suspended solids', 'TSS', 'mg/l')

        self.__add_observation_mapping('Battery Voltage', 'VB', 'V')
        self.__add_observation_mapping('Chlorophyll a', 'WY', 'μg/l')
        self.__add_observation_mapping('DO (Concentration)', 'WO', 'mg/l')
        self.__add_observation_mapping('DO (Saturation)', 'WX', '%')
        self.__add_observation_mapping('ORP', 'ORP', 'mV')
        self.__add_observation_mapping('pH', 'PH', 'pH Units')
        self.__add_observation_mapping('Specific conductance', 'SpCond', 'μS/cm')
        self.__add_observation_mapping('Temperature', 'TW', '°F')
        self.__add_observation_mapping('Turbidity', 'WTNTU', '_NTU')

    def populate_exchange_configuration(self):
        exchange_configurations = self.sample_client.get_search_result(
            'exchangeconfigurations', {'type': 'AQUARIUS_TIMESERIES'})
        self.exchange_configuration = exchange_configurations['domainObjects'][0]

        self.__reset_exchange_configuration_setting_to_empty()

        self.__set_exchange_configuration_setting()
        self.__set_exchange_configuration_location_mappings()
        self.__set_exchange_configuration_observation_mappings()
        self.exchange_configuration = self.sample_client.put_domain_object(
            'exchangeconfigurations', self.exchange_configuration)

        self.logger.info('exchange_configuration is configured.')

    def get_or_create_field_visit(self, sampling_location, field_visit_custom_id):
        field_visit = self.sample_client.get_domain_object_by_custom_id('fieldvisits', field_visit_custom_id)
        if field_visit is None:
            field_visit_to_post = SampleClient.make_field_visit(sampling_location, field_visit_custom_id)
            field_visit = self.sample_client.post_domain_object('fieldvisits', field_visit_to_post)
        return field_visit

    def get_or_create_activity(self, field_visit, activity_custom_id, activity_type):
        activity = self.sample_client.get_domain_object_by_custom_id('activities', activity_custom_id)
        if activity is None:
            activity_to_post = SampleClient.make_activity(field_visit, activity_type, activity_custom_id)
            activity = self.sample_client.post_domain_object('activities', activity_to_post)
        return activity

    def delete_field_visits_by_sampling_location_id(self, sampling_location_id):
        search_result = self.sample_client.get_search_result('fieldvisits', {
            'samplingLocationIds': sampling_location_id
        })
        for field_visit in search_result['domainObjects']:
            self.delete_activities_by_field_visit_id(field_visit['id'])
            self.sample_client.delete_domain_object_by_id('fieldvisits', field_visit['id'])

    def delete_activities_by_field_visit_id(self, field_visit_id):
        search_result = self.sample_client.get_search_result('activities', {'fieldVisitId': field_visit_id})
        for activity in search_result['domainObjects']:
            self.sample_client.delete_domain_object_by_id('activities', activity['id'])

    def populate_vertical_profile_observations(self, sampling_location):
        self.delete_field_visits_by_sampling_location_id(sampling_location['id'])

        field_visit_custom_id = sampling_location['customId'] + '_FV_20141029'
        field_visit = self.get_or_create_field_visit(sampling_location, field_visit_custom_id)

        activity_custom_id = sampling_location['customId'] + '_VPA_20141029';
        activity = self.get_or_create_activity(field_visit, activity_custom_id, 'SAMPLE_INTEGRATED_VERTICAL_PROFILE')

        self.sample_client.import_file('services/import/verticalprofiledata', './DefaultVerticalProfileData.csv', params={
            'activityId': activity['id'],
            'samplingLocationIds': sampling_location['id']
        })

    def populate_lab_observations(self, sampling_location):
        self.delete_field_visits_by_sampling_location_id(sampling_location['id'])

        lab_data_csv = """Observation ID,Location ID,Observed Property ID,Observed DateTime,Analyzed DateTime,Depth,Depth Unit,Data Classification,Result Value,Result Unit,Result Status,Result Grade,Medium,Sample ID,Collection Method,Field: Device ID,Field: Device Type,Field: Comment,Lab: Specimen Name,Lab: Analysis Method,Lab: Detection Condition,Lab: Limit Type,Lab: MDL,Lab: MRL,Lab: Quality Flag,Lab: Received DateTime,Lab: Prepared DateTime,Lab: Sample Fraction,Lab: From Laboratory,Lab: Sample ID,Lab: Dilution Factor,Lab: Comment,QC: Type,QC: Source Sample ID
,{0},Ammonia,2014-10-29T09:00:00.000-07:00,2014-10-29T09:00:00.000-07:00,5,ft,LAB,8.6,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_1,GRAB,,,,bottle1,,,LOWER,0.1,0.5,,,,,,,,,,
,{0},Ammonia,2014-10-29T09:10:00.000-07:00,2014-10-29T09:10:00.000-07:00,5,ft,LAB,,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_2,GRAB,,,,bottle2-ND,,not detected,LOWER,0.1,0.5,,,,,,,,,,
,{0},Ammonia,2014-10-29T09:20:00.000-07:00,2014-10-29T09:20:00.000-07:00,5,ft,LAB,8.2,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_3,GRAB,,,,bottle3,,,LOWER,0.1,0.5,,,,,,,,,,
,{0},Ammonia,2014-10-29T09:30:00.000-07:00,2014-10-29T09:30:00.000-07:00,5,ft,LAB,8.8,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_4,GRAB,,,,bottle4,,,LOWER,0.1,0.5,,,,,,,,,,
""".format(sampling_location['customId'])
        self.sample_client.import_file('services/import/observations', 'lab_data.csv', lab_data_csv, params={
            'fileType': 'SIMPLE_CSV',
            'timeZoneOffset': '-08',
            'linkFieldVisitsForNewObservations': True
        })

    def populate(self):
        locations = ['AqsConnectorLoc1', 'AqsConnectorLoc2']
        self.populate_locations(locations)
        self.populate_exchange_configuration()

        self.populate_vertical_profile_observations(self.sampling_locations[0])
        self.populate_lab_observations(self.sampling_locations[1])

if __name__ == '__main__':
    app_config = AppConfig()
    CommonLogging.configure(app_config.logFile)

    sampleClient = SampleClient(app_config.token, app_config.base_url, app_config.certs)
    connectorPropagator = ConnectorPropagator(sampleClient)

    connectorPropagator.populate()
