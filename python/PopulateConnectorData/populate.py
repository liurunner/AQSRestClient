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
        self.sample_client = sample_client

    def populate_locations(self, sampling_location_custom_ids):
        sampling_locations = []

        for sampling_location_custom_id in sampling_location_custom_ids:
            sampling_location_overrides = {'customId': sampling_location_custom_id}
            sampling_location = self.sample_client.get_or_create_sampling_location(sampling_location_overrides)
            sampling_locations.append(sampling_location)

        self.logger.info('populate_locations %s done', sampling_location_custom_ids)
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
            'DEPTH_PARAMETER': 'DEPTH',
            'DEPTH_PARAMETER_UNIT': 'ft',
            'NON_DETECT_ALGORITHM': 'HALF_MDL',
            'DEFAULT_LOCATION_PATH': 'Locations.AQS',
            'DEFAULT_TIME_ZONE_OFFSET_HOURS': '-7'
        }
        for key, value in setting_key_values.iteritems():
            exchange_configuration['settings'].append({'key': key, 'value': value})

        for sampling_location_id, aqts_location in external_location_dict.iteritems():
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

    def populate_lab_observations(self, sampling_location):
        self.sample_client.delete_observations({'samplingLocationIds': sampling_location['id']})
        self.sample_client.delete_field_visits_by_sampling_location_id(sampling_location['id'])

        lab_data_csv = """Observation ID,Location ID,Observed Property ID,Observed DateTime,Analyzed DateTime,Depth,Depth Unit,Data Classification,Result Value,Result Unit,Result Status,Result Grade,Medium,Sample ID,Collection Method,Field: Device ID,Field: Device Type,Field: Comment,Lab: Specimen Name,Lab: Analysis Method,Lab: Detection Condition,Lab: Limit Type,Lab: MDL,Lab: MRL,Lab: Quality Flag,Lab: Received DateTime,Lab: Prepared DateTime,Lab: Sample Fraction,Lab: From Laboratory,Lab: Sample ID,Lab: Dilution Factor,Lab: Comment,QC: Type,QC: Source Sample ID
,{0},Ammonia,2014-10-29T09:00:00.000-07:00,2014-10-29T09:00:00.000-07:00,5,ft,LAB,8.6,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_1,GRAB,,,,bottle1,,,LOWER,0.1,0.5,,,,,,,,,,
,{0},Ammonia,2014-10-29T09:10:00.000-07:00,2014-10-29T09:10:00.000-07:00,5,ft,LAB,,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_2,GRAB,,,,bottle2-ND,,not detected,LOWER,0.1,0.5,,,,,,,,,,
,{0},Ammonia,2014-10-29T09:20:00.000-07:00,2014-10-29T09:20:00.000-07:00,5,ft,LAB,8.2,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_3_1,GRAB,,,,bottle3_1,,,LOWER,0.1,0.5,,,,,,,,,,
,{0},Ammonia,2014-10-29T09:20:00.000-07:00,2014-10-29T09:20:00.000-07:00,6,ft,LAB,8.4,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_3_2,GRAB,,,,bottle3_2,,,LOWER,0.1,0.5,,,,,,,,,,
,{0},Ammonia,2014-10-29T09:20:00.000-07:00,2014-10-29T09:20:00.000-07:00,8,ft,LAB,8.4,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_3_3,GRAB,,,,bottle3_3,,,LOWER,0.1,0.5,,,,,,,,,,
,{0},Ammonia,2014-10-29T09:30:00.000-07:00,2014-10-29T09:30:00.000-07:00,5,ft,LAB,8.8,mg/l,Preliminary,Ok,Water,{0}_SA_20141029_4,GRAB,,,,bottle4,,,LOWER,0.1,0.5,,,,,,,,,,
""".format(sampling_location['customId'])
        self.sample_client.import_file('services/import/observations', 'lab_data.csv', lab_data_csv, params={
            'fileType': 'SIMPLE_CSV',
            'timeZoneOffset': '-08',
            'linkFieldVisitsForNewObservations': True
        })

    def populate(self):
        sampling_location_custom_ids = ['AqsConnectorLoc1', 'AqsConnectorLoc2', 'AqsConnectorLoc3']
        sampling_locations = self.populate_locations(sampling_location_custom_ids)
        self.populate_exchange_configuration(
            external_location_dict={
                sampling_locations[0]['id']: sampling_location_custom_ids[0],
                sampling_locations[1]['id']: '',
                sampling_locations[2]['id']: 'NonexistentAQTSLocation'
            },
            observation_map_tuple={
                ('Ammonia', 'NH4NH3_dis', 'mg/l'),
                ('Total Dissolved Solids', 'TDS', 'mg/l'),
                ('Total suspended solids', 'TSS', 'mg/l'),
                ('Battery Voltage', 'VB', 'V'),
                ('Chlorophyll a', 'WY', 'μg/l'),
                ('DO (Concentration)', 'WO', 'mg/l'),
                ('DO (Saturation)', 'WX', '%'),
                ('ORP', 'ORP', 'mV'),
                ('pH', 'PH', 'pH Units'),
                ('Specific conductance', 'SpCond', 'μS/cm'),
                ('Temperature', 'TW', '°F'),
                ('Turbidity', 'WTNTU', '_NTU'),
            }
        )

        self.populate_vertical_profile_observations(sampling_locations[0])
        self.populate_lab_observations(sampling_locations[1])

if __name__ == '__main__':
    app_config = AppConfig()
    CommonLogging.configure(app_config.logFile)

    sampleClient = SampleClient(app_config.token, app_config.base_url, app_config.certs)
    connectorPropagator = ConnectorPropagator(sampleClient)

    connectorPropagator.populate()
