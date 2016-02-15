__author__ = 'alessio.rocchi'

import base64
import requests


class VCS(object):
    def __init__(self, host, username, password, version='5.5', verify=False, org='System'):
        if not (host.startswith('https://') or host.startswith('http://')):
            host = 'https://{host}'.format(host=host)
        self.url = host + '/api/sessions'
        self.username = username
        self.password = password
        self.version = version
        self.verify = verify
        self.token = None
        self.org = org
        self.org_list_url = None
        self.orgList = []

    def login(self):
        encode = "Basic " + base64.standard_b64encode(self.username + "@" + self.org + ":" + self.password)
        headers = {}
        headers["Authorization"] = encode.rstrip()
        headers["Accept"] = "application/*+xml;version=" + self.version
        response = requests.post(self.url, headers=headers, verify=self.verify)
        if response.status_code == requests.codes.ok:
            self.token = response.headers["x-vcloud-authorization"]
            return True
        else:
            return False

    def _get_vcloud_headers(self):
        headers = {}
        headers["x-vcloud-authorization"] = self.token
        headers["Accept"] = "application/*+xml;version=" + self.version
        return headers

    def execute_request(self, url):
        headers = self._get_vcloud_headers()
        response = requests.get(url, headers=headers, verify=self.verify)
        if response.status_code == requests.codes.ok:
            return response
        else:
            return None

