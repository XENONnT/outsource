import os
import requests
import json
from bson import json_util
import datetime
import logging

from pprint import pprint
from outsource.Config import Config

# TODO understand logging better
#logging.basicConfig(level=logging.DEBUG)

PREFIX = "http://xenon-runsdb-dev.grid.uchicago.edu:5000"
BASE_HEADERS = {'Content-Type': "application/json", 'Cache-Control': "no-cache"}


class Token:
    """
    Object handling tokens for runDB API access.
    
    """
    def __init__(self, path=".dbtoken"):
        # get a new token
        if os.path.exists(path):
            with open(path) as f:
                json_in = json.load(f)
                self.string = json_in['string']
                self.creation_time = json_in['creation_time']
        else:
            self.string = self.new_token()
            self.creation_time = datetime.datetime.now().timestamp()
        self.path = path

        # for writing to disk
        self.json = dict(string=self.string, creation_time=self.creation_time)

        self.write()

    def __call__(self):
        return self.string

    def new_token(self):
        path = PREFIX + "/login"
        # TODO put the username and password in config file or something
        data=json.dumps({"username": "admin", "password": "test_admin"})
        response = requests.post(path, data=data, headers=BASE_HEADERS)
        return json.loads(response.text)['access_token']

    def is_valid(self):
        # TODO do an API call for this instead?
        return datetime.datetime.now().timestamp() - self.creation_time < 24*60*60

    def refresh(self):
        # if valid, don't do anything
        if self.is_valid():
            logging.debug("Token is valid")
            return
        # update the token string
        url = PREFIX + "/refresh"
        headers = BASE_HEADERS.copy()
        headers['Authorization'] = "Bearer {string}".format(string=self.string)
        response = requests.get(url, headers=headers)
        self.string = json.loads(response.text)['access_token']
        # write out again
        self.write()
        logging.debug("Token refreshed")

    # TODO what if reach 30 day expiration?

    def write(self):
        with open(self.path, "w") as f:
            json.dump(self.json, f)


class DB:
    """Wrapper around the RunDB API"""

    def __init__(self, token_path=".dbtoken"):
        config = Config()
        self.get_params = {
            'username': config.get('Common', 'rundb_username'),
            'api_key': config.get('Common', 'rundb_api_key'),
        }
        
        # Takes a path to pickled token object. If path exists, load it; else make a new one
        token = Token(token_path)

        self.headers = BASE_HEADERS.copy()
        self.headers['Authorization'] = "Bearer {token}".format(token=token())

    def get(self, url):
        return requests.get(PREFIX + url, headers=self.headers)

    def get_name(self, number, detector='tpc'):
        # TODO check against the detector, if necessary
        url = "/runs/number/{number}/filter/detector".format(number=number)
        response = json.loads(self.get(url).text)
        return response['results']['name']

    def get_run(self, name, detector='tpc'):
        query = {'detector': detector,
                 'name': name}
        query = {'query': json.dumps(query)}
        
        # Prepare query parameters
        params = self.get_params
        for key in query.keys():
            params[key] = query[key]
        params['limit'] = 1
        params['offset'] = 0

        #pprint(params)

        data = requests.get('https://xenon1t-daq.lngs.infn.it/runs_api/runs/runs/', headers=self.headers, params = params).text
        #data = requests.get(PREFIX + '/runs_api/', headers=self.headers, params = params).text
        
        data = json_util.loads(data)
        #pprint(data)
        
        if data['meta']['total_count'] != 1:
            raise RuntimeError('Unable to find run in the run database')
        
        return data['objects'][0]['doc']


if __name__ == "__main__":
    db = DB()
    x = db.get_name(10000)
    print(x)
