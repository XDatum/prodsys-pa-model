#
# Author:
# - Dmitry Golubkov, <dmitry.v.golubkov@cern.ch>, 2016-2018
#
# Updates by:
# - Mikhail Titov, <mikhail.titov@cern.ch>, 2018
#

import json

try:
    import requests
    import urllib
except ImportError:
    pass

API_VERSION = 'v1'


class DEFTClient(object):

    API_BASE_PATH = '/api/{0}'.format(API_VERSION)

    def __init__(self, auth_user, auth_key, base_url, verify_ssl_cert=False):
        self.api_url = '{0}{1}'.format(base_url, self.API_BASE_PATH)
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': 'ApiKey {0}:{1}'.format(auth_user, auth_key)}
        self.verify_ssl_cert = verify_ssl_cert

    def _get_action_list(self):
        action_cls = 'actions'
        response = requests.get(url='{0}/{1}/'.format(
                                    self.api_url,
                                    action_cls),
                                headers=self.headers,
                                verify=self.verify_ssl_cert)

        if response.status_code == requests.codes.ok:
            return json.loads(response.content)['result']
        else:
            raise Exception('Invalid HTTP response code: {0}'.
                            format(response.status_code))

    def _create_request(self, action, owner, body):
        action_list = self._get_action_list()
        if action not in action_list:
            raise Exception('Invalid action: {0} ({1})'.
                            format(action, str(action_list)))

        action_cls = 'request'
        response = requests.post(url='{0}/{1}/'.format(
                                     self.api_url,
                                     action_cls),
                                 headers=self.headers,
                                 data=json.dumps({
                                     'action': action,
                                     'owner': owner,
                                     'body': '{0}'.format(json.dumps(body))}),
                                 verify=self.verify_ssl_cert)

        if response.status_code == requests.codes.created:
            api_request_object = json.loads(response.content)
            return api_request_object['id']
        elif response.status_code == requests.codes.unauthorized:
            raise Exception('Access denied')
        else:
            raise Exception('Invalid HTTP response code: {0}'.
                            format(response.status_code))

    def _create_task_search(self, filter_dict):
        if filter_dict:
            filter_dict.update({'limit': 0})
        filter_string = urllib.urlencode(filter_dict)

        action_cls = 'task'
        response = requests.get(url='{0}/{1}/?{2}'.format(
                                    self.api_url,
                                    action_cls,
                                    filter_string),
                                headers=self.headers,
                                verify=self.verify_ssl_cert)

        if response.status_code == requests.codes.ok:
            return json.loads(response.content)
        else:
            raise Exception('Invalid HTTP response code: {0}'.
                            format(response.status_code))

    def get_status(self, request_id):
        action_cls = 'request'
        response = requests.get(url='{0}/{1}/{2}/'.format(
                                    self.api_url,
                                    action_cls,
                                    request_id),
                                headers=self.headers,
                                verify=self.verify_ssl_cert)

        if response.status_code == requests.codes.ok:
            status_string = json.loads(response.content)['status']
            if status_string:
                return json.loads(status_string)
        elif response.status_code == requests.codes.unauthorized:
            raise Exception('Access denied')
        else:
            raise Exception('Invalid HTTP response code: {0}'.
                            format(response.status_code))

    def set_ttcr(self, owner, ttcr_dict):
        return self._create_request(action='set_ttcr',
                                    owner=owner,
                                    body={'ttcr_dict': ttcr_dict})

    def set_ttcj(self, owner, ttcj_dict):
        return self._create_request(action='set_ttcj',
                                    owner=owner,
                                    body={'ttcj_dict': ttcj_dict})

    def get_task(self, task_id):
        if task_id:
            response = self._create_task_search(filter_dict={'id': task_id})
            if response.get('objects'):
                return response['objects'][0]
