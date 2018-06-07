#
# Copyright European Organization for Nuclear Research (CERN),
#           National Research Centre "Kurchatov Institute" (NRC KI)
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mikhail Titov, <mikhail.titov@cern.ch>, 2018
#

import json

try:
    import requests
except ImportError:
    pass

API_VERSION = 'v1'


class P2PAClient(object):

    API_BASE_PATH = '/api/{0}'.format(API_VERSION)

    def __init__(self, token, base_url, verify_ssl_cert=False):
        self.api_url = '{0}{1}'.format(base_url, self.API_BASE_PATH)
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Token {0}'.format(token)}
        self.verify_ssl_cert = verify_ssl_cert

    def _create_request(self, action_cls, action, body):
        response = requests.post(url='{0}/{1}/{2}'.
                                     format(self.api_url, action_cls, action),
                                 headers=self.headers,
                                 data=json.dumps({
                                     'body': '{0}'.format(json.dumps(body))}),
                                 verify=self.verify_ssl_cert)

        if response.status_code == requests.codes.created:
            api_request_object = json.loads(response.content)
            return api_request_object['message']
        elif response.status_code == requests.codes.unauthorized:
            raise Exception('Access denied')
        else:
            raise Exception('Invalid HTTP response code: {0}'.
                            format(response.status_code))

    def set_td_predictions(self, data):
        return self._create_request(action_cls='prediction',
                                    action='set_block_td',
                                    body=data)
