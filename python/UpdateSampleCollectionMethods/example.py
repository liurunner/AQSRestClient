#!/usr/bin/python
# coding:utf-8

import getopt, json, sys, requests;


class SampleClient(object):
    def __init__(self):
        try:
            opts, args = getopt.getopt(sys.argv[1:], "", ["token=", "host="])
        except getopt.GetoptError as err:
            print str(err)  # will print something like "option -a not recognized"
            sys.exit(2)

        if opts:
            for opt, arg in opts:
                if opt == "--token":
                    self.token = arg
                if opt == "--host":
                    self.baseUrl = "https://{0}/api/v1".format(arg)
                else:
                    pass
        print 'token:', self.token
        print 'baseUrl:', self.baseUrl

    def get_headers(self):
        return {'Content-Type': 'application/json', 'Authorization': 'token ' + self.token}

    def get_collection_method_id(self, __custom_id__):
        response = requests.get('{0}/collectionmethods'.format(self.baseUrl), headers=self.get_headers())
        collectionMethods = json.loads(response.text)
        for collectionMethod in collectionMethods:
            if __custom_id__ == collectionMethod['customId']:
                return collectionMethod['id']
        return None


if __name__ == "__main__":
    client = SampleClient()
    collectionMethodId = client.get_collection_method_id(u'Water Quality')
    print 'collection method id is ', collectionMethodId


