#!/usr/bin/env python3
#
# Licensed to Elasticsearch under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""Push Pingboard to Elasticsearch"""

import argparse
import datetime
import json
import logging
import os
import re
import sys
import time
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from elasticsearch import Elasticsearch
import googlemaps
import yaml


def request(url, data=None, headers=None):
    """Generic request"""
    req = Request(url, data=data, headers=headers)
    return json.loads(urlopen(req).read())


class Location(object):  # pylint: disable=too-few-public-methods
    """Geographic location"""

    maps = None
    maps_reqs_per_sec = 50

    def __init__(self, data, verbose=False):
        self.address = ''
        for name in User.location_fields:
            if name in data:
                self.address += ', '.join(data[name])
        self.data = {}
        self.logger = logging.getLogger('maps')
        if verbose:
            self.logger.setLevel(logging.DEBUG)

    def __str__(self):
        return self.address

    def geocode(self):
        """Geocode user location"""
        if not self.maps or not self.address:
            return None, None
        try:
            results = self.maps.geocode(self.address)
            self.logger.info(results)
        except googlemaps.exceptions.HTTPError as http_error:
            self.logger.error(http_error)
        location = results[0]['geometry']['location']
        self.data['lat'], self.data['lon'] = location['lat'], location['lng']


class User(object):
    """Formatted user"""

    elasticsearch = None
    fields = {
        'bio': {
            'type': 'text'
        },
        #'birth_date': {
        #    'type': 'date'
        #},
        'email': {
            'type': 'keyword'
        },
        'first_name': {
            'type': 'keyword'
        },
        'job_title': {
            'type': 'text'
        },
        'last_name': {
            'type': 'keyword'
        },
        'locale': {
            'type': 'keyword',
        },
        'location': {
            'type': 'geo_point'
        },
        'nickname': {
            'type': 'keyword'
        },
        'time_zone': {
            'type': 'keyword'
        }
    }
    custom_fields = {}
    location_fields = []

    def __init__(self, data, verbose=False):
        self.doc_id = data['id']
        self.body = {}
        if data['start_date'] is not None:
            self.body['@timestamp'] = datetime.datetime.strptime(data['start_date'], '%Y-%m-%d')
        for key in self.fields:
            if self.fields[key]['type'] in ('keyword', 'text') and key in data:
                self.body[key] = data[key]
        custom_fields = data.setdefault('custom_fields', {})
        for key, name in self.custom_fields.items():
            custom_value = custom_fields.get(key)
            if custom_value:
                self.body[name] = custom_value
        self.location = None
        self.verbose = verbose

    def __str__(self):
        return str(self.body)

    def geocode(self):
        """Geocode the user's location"""
        if not Location.maps:
            return
        self.location = Location(self.body, verbose=self.verbose)
        self.location.geocode()
        if self.location.data:
            self.body['location'] = self.location.data

    def index(self):
        """Index into elasticsearch"""
        if self.elasticsearch:
            self.elasticsearch.index(index='users', doc_type='user', id=self.doc_id, body=self.body)


class Pingboard(object):
    """Pingboard APIs"""

    elasticsearch = None

    def __init__(self, config, verbose=False, email=None):
        """Get a token for this session"""
        values = urlencode({'client_id': config['client_id'],
                            'client_secret': config['client_secret']}).encode('ascii')
        if config['custom_fields']:
            for key, mapping in config['custom_fields'].items():
                User.fields.update({mapping['name']: {'type': mapping['type']}})
                User.custom_fields.update({key: mapping['name']})

        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        token = self.request('https://app.pingboard.com/oauth/token?grant_type=client_credentials',
                             data=values, headers=headers)
        self.headers_ = {
            'Authorization': 'Bearer ' + token['access_token']
        }
        self.users_ = []
        self.logger = logging.getLogger('pingboard')
        self.email = email
        self.verbose = verbose
        if self.verbose:
            self.logger.setLevel(logging.DEBUG)

    def request(self, url, data=None, headers=None):
        """Run a generic request"""
        headers = headers or self.headers_
        return request(url, data=data, headers=headers)

    def users(self):
        """Get all the users"""
        if not self.users_:
            url = 'https://app.pingboard.com/api/v2/users?page_size=10000'
            if self.email:
                url += '&email=' + self.email
            response = self.request(url)
            for data in response['users']:
                self.logger.info(data)
                user = User(data, verbose=self.verbose)
                self.users_.append(user)
        return self.users_

    def index(self):
        """Geocode users and index into elasticsearch"""
        quota = Location.maps_reqs_per_sec
        for user in self.users():
            if quota == 0:
                time.sleep(1)
                quota = Location.maps_reqs_per_sec
            user.geocode()
            quota -= 1
            user.index()

    def create_indeces(self, recreate=False):
        """Create the indeces"""
        body = {
            "mappings": {
                "user": {
                    "properties": User.fields
                }
            }
        }
        if self.elasticsearch:
            if recreate:
                self.elasticsearch.indices.delete(index="users")
            self.elasticsearch.indices.create(index="users", ignore=400, body=body)


def parse_config(config_file):
    """Parse the YAML config"""
    pattern = re.compile(r'^\<%= ENV\[\'(.*)\'\] %\>(.*)$')
    yaml.add_implicit_resolver("!env", pattern)

    def env_constructor(loader, node):
        """Constructor for environment variables"""
        value = loader.construct_scalar(node)
        env_var, remaining_path = pattern.match(value).groups()
        return os.environ[env_var] + remaining_path

    yaml.add_constructor('!env', env_constructor)
    with open(config_file) as config:
        return yaml.load(config)


def connect_elasticsearch_client(config, verbose=False):
    """Connect the elasticsearch client"""
    elasticsearch = None
    logging.basicConfig()
    if verbose:
        logging.getLogger('elasticsearch').setLevel(logging.DEBUG)
    if config['hosts']:
        http_auth = (config['user'], config['secret']) if config['user'] else None
        elasticsearch = Elasticsearch(config['hosts'], http_auth=http_auth)
    Pingboard.elasticsearch = elasticsearch
    User.elasticsearch = elasticsearch


def connect_maps_client(config):
    """Connect to the maps service"""
    if config['service'] != 'google':
        return
    Location.maps = googlemaps.Client(config['key'])
    User.location_fields = config['fields']


def main():
    """Main"""
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', default='elasticboard.yml')
    parser.add_argument('--email', help='Filter by email')
    parser.add_argument('--recreate-index', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    config = parse_config(args.config)
    pingboard = Pingboard(config['pingboard'], verbose=args.verbose, email=args.email)
    connect_maps_client(config['maps'])
    connect_elasticsearch_client(config['elasticsearch'], verbose=args.verbose)
    pingboard.create_indeces(recreate=args.recreate_index)
    pingboard.index()


if __name__ == '__main__':
    sys.exit(main())
