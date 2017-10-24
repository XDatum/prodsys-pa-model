__author__ = 'Dmitry Golubkov'
__email__ = 'dmitry.v.golubkov@cern.ch'

import json

try:
    import requests
except ImportError:
    pass


class DEFTClient(object):
    BASE_URL = 'https://aipanda015.cern.ch'

    def __init__(self, auth_user, auth_key, verify_ssl_cert=False, base_url=None):
        self.verify_ssl_cert = verify_ssl_cert
        self.base_url = self.BASE_URL if not base_url else base_url
        self.api_url = '/api/v1/request/'
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': 'ApiKey {0}:{1}'.format(auth_user, auth_key)}

    def _get_action_list(self):
        response = requests.get(
            '{0}{1}actions/'.format(self.base_url, self.api_url),
            headers=self.headers,
            verify=self.verify_ssl_cert)

        if response.status_code == requests.codes.ok:
            return json.loads(response.content)['result']
        else:
            raise Exception(
                'Invalid HTTP response code: {0}'.format(response.status_code))

    def _create_request(self, action, owner, body):
        action_list = self._get_action_list()
        if action not in action_list:
            raise Exception(
                'Invalid action: {0} ({1})'.format(action, str(action_list)))

        response = requests.post(
            '{0}{1}'.format(self.base_url, self.api_url),
            headers=self.headers,
            data=json.dumps({'action': action,
                             'owner': owner,
                             'body': '{0}'.format(json.dumps(body))}),
            verify=self.verify_ssl_cert)

        if response.status_code == requests.codes.created:
            api_request_object = json.loads(response.content)
            return api_request_object['id']
        elif response.status_code == requests.codes.unauthorized:
            raise Exception('Access denied')
        else:
            raise Exception(
                'Invalid HTTP response code: {0}'.format(response.status_code))

    def get_status(self, request_id):
        response = requests.get(
            '{0}{1}{2}/'.format(self.base_url, self.api_url, request_id),
            headers=self.headers,
            verify=self.verify_ssl_cert)

        if response.status_code == requests.codes.ok:
            status_string = json.loads(response.content)['status']
            if status_string:
                return json.loads(status_string)
        elif response.status_code == requests.codes.unauthorized:
            raise Exception('Access denied')
        else:
            raise Exception(
                'Invalid HTTP response code: {0}'.format(response.status_code))

    def set_ttcr(self, owner, ttcr_dict):
        body = {'ttcr_dict': ttcr_dict}
        return self._create_request('set_ttcr', owner, body)

    def set_ttcj(self, owner, ttcj_dict):
        body = {'ttcj_dict': ttcj_dict}
        return self._create_request('set_ttcj', owner, body)
