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
        self.base_url_second = None
        self.logFile = './client.log'

        self.load_command_line_options()

    def load_command_line_options(self):
        try:
            opts, args = getopt.getopt(sys.argv[1:], '', ['token=', 'host=', 'host2=', 'log'])
        except getopt.GetoptError as err:
            print(str(err))
            sys.exit(2)

        if opts:
            for opt, arg in opts:
                if opt == '--token':
                    self.token = arg
                elif opt == '--host':
                    self.base_url = 'https://{0}/api/'.format(arg)
                elif opt == '--host2':
                    self.base_url_second = 'https://{0}/api/'.format(arg)
                elif opt == '--log':
                    self.logFile = arg
                else:
                    pass


class ConnectorPropagator(object):
    def __init__(self, token, base_url):
        self.logger = CommonLogging.get_logger("ConnectorPropagator")
        self.sample_client = SampleClient(token, base_url)
        self.sample_client.check_availability()

    def populate_locations(self, location_data_tuple):
        sampling_locations = []

        for sampling_location_custom_id, location_data in location_data_tuple.items():
            sampling_location_overrides = {'customId': sampling_location_custom_id, 'latitude': '49.2061028', 'longitude': '-123.1504412'}
            sampling_location = self.sample_client.get_or_create_sampling_location(sampling_location_overrides)
            sampling_locations.append(sampling_location)

            if location_data['isVerticalProfile']:
                self.populate_vertical_profile_observations(sampling_location)
            elif 'csv_data_pattern' in location_data:
                self.populate_csv_observations(sampling_location, location_data['csv_data_pattern'])

            self.logger.info('populate_locations %s done', sampling_location_custom_id)

        return sampling_locations

    def reset_exchange_configuration_setting_to_empty(self, exchange_configuration):
        exchange_configuration['settings'][:] = []
        exchange_configuration['samplingLocationMappings'][:] = []
        exchange_configuration['observationMappings'][:] = []
        return self.sample_client.put_domain_object('exchangeconfigurations', exchange_configuration)

    def populate_exchange_configuration(self, external_location_dict, observation_map_tuple):
        exchange_configurations = self.sample_client.get_search_result('exchangeconfigurations', {'type': 'AQUARIUS_TIMESERIES'})
        exchange_configuration = self.reset_exchange_configuration_setting_to_empty(exchange_configurations['domainObjects'][0])

        setting_key_values = {
            'DEFAULT_TIME_ZONE_OFFSET_HOURS': '-7',
            'DEFAULT_LOCATION_PATH': 'All Locations',
            #'DEFAULT_LOCATION_TYPE': 'Water Quality Site',
            # AQUARIUS Time Series 3X
            #'DEFAULT_LOCATION_EXTENDED_ATTRIBUTES': 'NumSensors=10,Device=Trimble GPS GeoExplorer XH 6000,PlannedDatetime=2015-12-12 12:00',
            #'DEFAULT_TIME_SERIES_EXTENDED_ATTRIBUTES': '',
            # AQUARIUS Time Series NG
            #'DEFAULT_LOCATION_EXTENDED_ATTRIBUTES': 'LOGGERTYPE@LOCATION_EXTENSION=loggerType,NUMBEROFSENSORS@LOCATION_EXTENSION=5,LASTMODIFIED@LOCATION_EXTENSION=2015-12-12T12:00:00',
            #'DEFAULT_TIME_SERIES_EXTENDED_ATTRIBUTES': 'AVERAGE@TIMESERIES_EXTENSION=12,RECORDDATE@TIMESERIES_EXTENSION=2016-10-01T12:00:00,DATADESC@TIMESERIES_EXTENSION=My description',
            'DEPTH_PARAMETER': 'DEPTH',
            'DEPTH_PARAMETER_UNIT': 'ft',
            'NON_DETECT_ALGORITHM': 'HALF_MDL'
        }
        for key, value in setting_key_values.items():
            exchange_configuration['settings'].append({'key': key, 'value': value})

        for sampling_location_id, aqts_location in external_location_dict.items():
            exchange_configuration['samplingLocationMappings'].append({
                'samplingLocation': {'id': sampling_location_id},
                'externalLocation': aqts_location
            })

        for observed_property_custom_id, aqts_parameter_type, aqts_parameter_unit in observation_map_tuple:
            observed_property = self.sample_client.get_domain_object_by_custom_id(
                'observedproperties', observed_property_custom_id, raise_error_when_custom_id_is_unused=True)
            exchange_configuration['observationMappings'].append({
                'observedProperty': {'id': observed_property['id']},
                'externalObservedProperty': aqts_parameter_type,
                'externalUnit': aqts_parameter_unit
            })

        self.sample_client.put_domain_object('exchangeconfigurations', exchange_configuration)
        self.logger.info('populate_exchange_configuration is done')

    def populate_vertical_profile_observations(self, sampling_location):
        self.sample_client.delete_observations({'samplingLocationIds': sampling_location['id']})
        self.sample_client.delete_field_visits_by_sampling_location_id(sampling_location['id'])

        field_visit_overrides = {
            'samplingLocation': sampling_location,
            'customId': sampling_location['customId'] + '_FV_20141029'
        }
        field_visit = self.sample_client.get_or_create_field_visit(field_visit_overrides)

        activity_overrides = {
            'fieldVisit': field_visit,
            'customId': sampling_location['customId'] + '_VPA_20141029',
            'type': 'SAMPLE_INTEGRATED_VERTICAL_PROFILE'
        }
        activity = self.sample_client.get_or_create_activity(activity_overrides)

        self.sample_client.import_file('services/import/verticalprofiledata', './DefaultVerticalProfileData.csv', params={
            'activityId': activity['id'],
            'samplingLocationIds': sampling_location['id']
        })

    def populate_csv_observations(self, sampling_location, csv_data_pattern_on_location):
        self.sample_client.delete_observations({'samplingLocationIds': sampling_location['id']})
        self.sample_client.delete_field_visits_by_sampling_location_id(sampling_location['id'])

        observations_csv = csv_data_pattern_on_location.format(sampling_location['customId'])
        self.logger.debug('observations_csv: %s', observations_csv)
        self.sample_client.import_file('services/import/observations', 'observations_data.csv', observations_csv, params={
            'fileType': 'SIMPLE_CSV',
            'timeZoneOffset': '-08',
            'linkFieldVisitsForNewObservations': True
        })

    def get_location_data_tuple(self):
        return {
            'AqsConnectorLoc1': {
                'external_location': '',
                'isVerticalProfile': True
            },
            'AqsConnectorLoc2': {
                'external_location': '',
                'isVerticalProfile': False,
                'csv_data_pattern': """Observation ID,Location ID,Observed Property ID,Observed DateTime,Analyzed DateTime,Depth,Depth Unit,Data Classification,Result Value,Result Unit,Result Status,Result Grade,Medium,Sample ID,Collection Method,Field: Device ID,Field: Device Type,Field: Comment,Lab: Specimen Name,Lab: Analysis Method,Lab: Detection Condition,Lab: Limit Type,Lab: MDL,Lab: MRL,Lab: Quality Flag,Lab: Received DateTime,Lab: Prepared DateTime,Lab: Sample Fraction,Lab: From Laboratory,Lab: Sample ID,Lab: Dilution Factor,Lab: Comment,QC: Type,QC: Source Sample ID
,{0},Ammonia,2014-10-29T09:00:00.000-07:00,2014-10-29T09:00:00.000-07:00,5,ft,LAB,8.6,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_1,GRAB,,,,bottle1,,,LOWER,,,,,,,,,,,,
,{0},Ammonia,2014-10-29T09:10:00.000-07:00,2014-10-29T09:10:00.000-07:00,5,ft,LAB,,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_2,GRAB,,,,bottle2-ND,,not detected,LOWER,50,0.1,,,,,,,,,,
,{0},Ammonia,2014-10-29T09:20:00.000-07:00,2014-10-29T09:20:00.000-07:00,5,ft,LAB,8.2,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_3_1,GRAB,,,,bottle3,,,LOWER,,,,,,,,,,,,
,{0},Ammonia,2014-10-29T09:20:00.000-07:00,2014-10-29T09:20:00.000-07:00,10,ft,LAB,8.4,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_3_2,GRAB,,,,bottle3_1,,,LOWER,,,,,,,,,,,,
,{0},Ammonia,2014-10-29T09:20:00.000-07:00,2014-10-29T09:20:00.000-07:00,20,ft,LAB,8.4,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_3_3,GRAB,,,,bottle3_2,,,LOWER,,,,,,,,,,,,
,{0},Ammonia,2014-10-29T09:30:00.000-07:00,2014-10-29T09:30:00.000-07:00,5,ft,LAB,8.8,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_4,GRAB,,,,bottle4,,,LOWER,,,,,,,,,,,,
,{0},Total Dissolved Solids,2014-10-29T09:00:00.000-07:00,2014-10-29T09:00:00.000-07:00,5,ft,LAB,8.6,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_1,GRAB,,,,bottle1,,,LOWER,,,,,,,,,,,,
,{0},Total Dissolved Solids,2014-10-29T09:10:00.000-07:00,2014-10-29T09:10:00.000-07:00,5,ft,LAB,,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_2,GRAB,,,,bottle2-ND,,not detected,LOWER,50,0.1,,,,,,,,,,
,{0},Total Dissolved Solids,2014-10-29T09:20:00.000-07:00,2014-10-29T09:20:00.000-07:00,5,ft,LAB,8.2,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_3_1,GRAB,,,,bottle3,,,LOWER,,,,,,,,,,,,
,{0},Total Dissolved Solids,2014-10-29T09:30:00.000-07:00,2014-10-29T09:30:00.000-07:00,5,ft,LAB,8.8,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_4,GRAB,,,,bottle4,,,LOWER,,,,,,,,,,,,
"""
            },
            'AqsConnectorLoc3': {
                'external_location': '',
                'isVerticalProfile': False,
                'csv_data_pattern': """Observation ID,Location ID,Observed Property ID,Observed DateTime,Analyzed DateTime,Depth,Depth Unit,Data Classification,Result Value,Result Unit,Result Status,Result Grade,Medium,Sample ID,Collection Method,Field: Device ID,Field: Device Type,Field: Comment,Lab: Specimen Name,Lab: Analysis Method,Lab: Detection Condition,Lab: Limit Type,Lab: MDL,Lab: MRL,Lab: Quality Flag,Lab: Received DateTime,Lab: Prepared DateTime,Lab: Sample Fraction,Lab: From Laboratory,Lab: Sample ID,Lab: Dilution Factor,Lab: Comment,QC: Type,QC: Source Sample ID,Standards Violations
,{0},Ammonia,1700-01-01T09:10:00.000-07:00,1700-01-01T09:10:00.000-07:00,,,LAB,8.2,mg/l,PRELIMINARY,OK,WATER,{0}_SA_17000101_bottle1,,,,,bottle1,,,,,,,,,,,,,,,,
,{0},Ammonia,3000-01-01T09:10:00.000-07:00,3000-01-01T09:10:00.000-07:00,,,LAB,8.6,mg/l,PRELIMINARY,OK,WATER,{0}_SA_30000101_bottle1,,,,,bottle1,,,,,,,,,,,,,,,,
"""
            }
        }

    def get_external_location_dict(self, sampling_locations, location_data_tuple):
        external_location_dict = {}
        for index in range(len(sampling_locations)):
            sampling_location_id = sampling_locations[index]['id']
            sampling_location_custom_id = sampling_locations[index]['customId']
            location_data = location_data_tuple[sampling_location_custom_id]
            external_location_dict[sampling_location_id] = location_data['external_location']
        return external_location_dict

    def get_observation_map_tuple(self):
        return {
                ('Ammonia', 'NH4NH3_dis', 'mg/l'),
                ('Battery Voltage', 'VB', 'V'),
                ('Chlorophyll a', 'WY', 'μg/l'),
                ('DO (Concentration)', 'WO', 'mg/l'),
                ('DO (Saturation)', 'WX', '%'),
                ('ORP', 'ORP', 'mV'),
                ('pH', 'PH', 'pH Units'),
                ('Specific conductance', 'SpCond', 'μS/cm'),
                ('Temperature', 'TW', '°F'),
                ('Total Dissolved Solids', 'TDS', 'mg/l'),
                ('Total suspended solids', 'TSS', 'mg/l'),
                ('Turbidity', 'WTNTU', '_NTU'),
            }

    def populate(self):
        location_data_tuple = self.get_location_data_tuple()
        sampling_locations = self.populate_locations(location_data_tuple)
        self.populate_exchange_configuration(
            external_location_dict=self.get_external_location_dict(sampling_locations, location_data_tuple),
            observation_map_tuple=self.get_observation_map_tuple()
        )


class ConnectorPropagatorOnSecondSync(ConnectorPropagator):
    def __init__(self, token, base_url):
        ConnectorPropagator.__init__(self, token, base_url)
        self.logger = CommonLogging.get_logger("ConnectorPropagatorOnSecondSync")

    def get_location_data_tuple(self):
        return {
            'AqtsConnectorLoc1': {
                'external_location': 'AqsConnectorLoc1',
                'isVerticalProfile': True
            },
            'AqtsConnectorLoc2': {
                'external_location': 'AqsConnectorLoc2',
                'isVerticalProfile': False,
                'csv_data_pattern': """Observation ID,Location ID,Observed Property ID,Observed DateTime,Analyzed DateTime,Depth,Depth Unit,Data Classification,Result Value,Result Unit,Result Status,Result Grade,Medium,Sample ID,Collection Method,Field: Device ID,Field: Device Type,Field: Comment,Lab: Specimen Name,Lab: Analysis Method,Lab: Detection Condition,Lab: Limit Type,Lab: MDL,Lab: MRL,Lab: Quality Flag,Lab: Received DateTime,Lab: Prepared DateTime,Lab: Sample Fraction,Lab: From Laboratory,Lab: Sample ID,Lab: Dilution Factor,Lab: Comment,QC: Type,QC: Source Sample ID
,{0},Ammonia,2014-10-29T09:00:00.000-07:00,2014-10-29T09:05:00.000-07:00,5,ft,LAB,9.6,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_1,GRAB,,,,bottle1,,,LOWER,,,,,,,,,,,,
,{0},Ammonia,2014-10-29T09:10:00.000-07:00,2014-10-29T09:10:00.000-07:00,5,ft,LAB,,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_2,GRAB,,,,bottle2-ND,,not detected,LOWER,50,0.1,,,,,,,,,,
,{0},Ammonia,2014-10-29T09:20:00.000-07:00,2014-10-29T09:15:00.000-07:00,5,ft,LAB,9.2,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_3_1,GRAB,,,,bottle3,,,LOWER,,,,,,,,,,,,
,{0},Ammonia,2014-10-29T09:30:00.000-07:00,2014-10-29T09:20:00.000-07:00,5,ft,LAB,9.8,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_4,GRAB,,,,bottle4,,,LOWER,,,,,,,,,,,,
,{0},Total suspended solids,2014-10-29T09:00:00.000-07:00,2014-10-29T09:05:00.000-07:00,5,ft,LAB,9.6,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_1,GRAB,,,,bottle1,,,LOWER,,,,,,,,,,,,
,{0},Total suspended solids,2014-10-29T09:10:00.000-07:00,2014-10-29T09:10:00.000-07:00,5,ft,LAB,,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_2,GRAB,,,,bottle2-ND,,not detected,LOWER,50,0.1,,,,,,,,,,
,{0},Total suspended solids,2014-10-29T09:20:00.000-07:00,2014-10-29T09:15:00.000-07:00,5,ft,LAB,9.2,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_3_1,GRAB,,,,bottle3,,,LOWER,,,,,,,,,,,,
,{0},Total suspended solids,2014-10-29T09:30:00.000-07:00,2014-10-29T09:20:00.000-07:00,5,ft,LAB,9.8,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_4,GRAB,,,,bottle4,,,LOWER,,,,,,,,,,,,
"""
            },
            'AqtsConnectorLoc3': {
                'external_location': 'AqsConnectorLoc3',
                'isVerticalProfile': False,
                'csv_data_pattern': """Observation ID,Location ID,Observed Property ID,Observed DateTime,Analyzed DateTime,Depth,Depth Unit,Data Classification,Result Value,Result Unit,Result Status,Result Grade,Medium,Sample ID,Collection Method,Field: Device ID,Field: Device Type,Field: Comment,Lab: Specimen Name,Lab: Analysis Method,Lab: Detection Condition,Lab: Limit Type,Lab: MDL,Lab: MRL,Lab: Quality Flag,Lab: Received DateTime,Lab: Prepared DateTime,Lab: Sample Fraction,Lab: From Laboratory,Lab: Sample ID,Lab: Dilution Factor,Lab: Comment,QC: Type,QC: Source Sample ID,Standards Violations
,{0},Ammonia,1700-01-01T09:10:00.000-07:00,1700-01-01T09:10:00.000-07:00,,,LAB,8.2,mg/l,PRELIMINARY,OK,WATER,{0}_SA_17000101_bottle1,,,,,bottle1,,,,,,,,,,,,,,,,
,{0},Ammonia,3000-01-01T09:10:00.000-07:00,3000-01-01T09:10:00.000-07:00,,,LAB,8.6,mg/l,PRELIMINARY,OK,WATER,{0}_SA_30000101_bottle1,,,,,bottle1,,,,,,,,,,,,,,,,
"""
            },
            'AqtsConnectorLoc4': {
                'external_location': 'NoneExistingLocation',
                'isVerticalProfile': False
            }
        }


if __name__ == '__main__':
    app_config = AppConfig()
    CommonLogging.configure(app_config.logFile)

    if app_config.base_url_second is None:
        connectorPropagator = ConnectorPropagatorOnSecondSync(app_config.token, app_config.base_url)
        connectorPropagator.populate()
    else:
        connectorFirstSyncPropagator = ConnectorPropagator(app_config.token, app_config.base_url)
        connectorFirstSyncPropagator.populate()

        connectorSecondSyncPropagator = ConnectorPropagatorOnSecondSync(app_config.token, app_config.base_url_second)
        connectorSecondSyncPropagator.populate()
