import os
import requests
import json
from bson import json_util
import datetime
import logging

from pprint import pprint
from outsource.Config import Config

logger = logging.getLogger("outsource")

config = Config()

PREFIX = config.get('Common', 'rundb_api_url')
BASE_HEADERS = {'Content-Type': "application/json", 'Cache-Control': "no-cache"}


class Token:
    """
    Object handling tokens for runDB API access.
    
    """
    def __init__(self, path=os.path.join(os.environ['HOME'], ".dbtoken")):
        # if token path exists, read it in. Otherwise make a new one
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
        # save the token json to disk
        self.write()
        # refresh if needed
        self.refresh()

    def __call__(self):
        return self.string

    def new_token(self):
        path = PREFIX + "/login"
        # TODO put the username and password in config file or something
        data=json.dumps({"username": config.get('Common', 'rundb_api_user'),
                         "password": config.get('Common', 'rundb_api_password')})
        response = requests.post(path, data=data, headers=BASE_HEADERS)
        return json.loads(response.text)['access_token']

    @property
    def is_valid(self):
        # TODO do an API call for this instead?
        return datetime.datetime.now().timestamp() - self.creation_time < 24*60*60

    def refresh(self):
        # if valid, don't do anything
        if self.is_valid:
            logger.debug("Token is valid")
            return
        # update the token string
        url = PREFIX + "/refresh"
        headers = BASE_HEADERS.copy()
        headers['Authorization'] = "Bearer {string}".format(string=self.string)
        response = requests.get(url, headers=headers)
        self.string = json.loads(response.text)['access_token']
        # write out again
        self.write()
        logger.debug("Token refreshed")

    # TODO what if reach 30 day expiration?

    def write(self):
        with open(self.path, "w") as f:
            json.dump(self.json, f)


class DB:
    """Wrapper around the RunDB API"""

    def __init__(self, token_path=os.path.join(os.environ['HOME'], ".dbtoken")):
        # Takes a path to serialized token object
        token = Token(token_path)

        self.headers = BASE_HEADERS.copy()
        self.headers['Authorization'] = "Bearer {token}".format(token=token())

    def get(self, url):
        return requests.get(PREFIX + url, headers=self.headers)

    def put(self, url, data):
        return requests.put(PREFIX + url, data, headers=self.headers)

    def post(self, url, data):
        return requests.post(PREFIX + url, data, headers=self.headers)

    def get_name(self, number, detector='tpc'):
        # TODO check against the detector, if necessary
        url = "/runs/number/{number}/filter/detector".format(number=number)
        response = json.loads(self.get(url).text)
        return response['results']['name']

    def get_number(self, name, detector='tpc'):
        url = "/runs/name/{name}/filter/detector".format(name=name)
        response = json.loads(self.get(url).text)
        return response['results']['number']

    def get_doc(self, number):
        # return the whole run doc for this run number
        url = '/runs/number/{num}'.format(num=number)
        return json.loads(self.get(url).text)['results']

    def update_datum(self, run, datum):
        datum = json.dumps(datum)
        url = '/run/number/{num}/data/'.format(num=run)
        return self.post(url, data=datum)


# for testing
if __name__ == "__main__":
    db = DB()
    #x = db.get_name(10000)
    # get data doc for run 2023
    #x = db.get_doc(2023)
    url = '/run/number/2023/data'
    data = json.loads(db.get(url).text)['results']['data']

    hosts = [d['host'] for d in data]
    test_datum = {'checksum': "None",
                  'creation_time': "None",
                  'host': 'OSG',
                  'type': 'processed',
                  'status': 'transferring'}

    print(hosts)
    print(db.update_datum(2023, test_datum).text)

