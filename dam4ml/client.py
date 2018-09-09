#!/usr/bin/env python
# encoding: utf-8
"""
client.py

Created by Pierre-Julien Grizel et al.
Copyright (c) 2016 NumeriCube. All rights reserved.

Client abstraction for the API endpoint
"""
from __future__ import unicode_literals

__author__ = ""
__copyright__ = "Copyright 2016, NumeriCube"
__credits__ = ["Pierre-Julien Grizel", ]
__license__ = "CLOSED SOURCE"
__version__ = "TBD"
__maintainer__ = "Pierre-Julien Grizel"
__email__ = "pjgrizel@numericube.com"
__status__ = "Production"

import os
import shutil
# import tempfile

import slumber
import requests
from requests.exceptions import ConnectionError
import tqdm
from tenacity import retry, retry_if_exception_type

class RequestsTokenAuth(requests.auth.AuthBase):
    """Implement token auth
    """
    _token = None

    def __init__(self, token):
        """Save user object for later"""
        self._token = token

    def __call__(self, r):
        # Implement my authentication
        r.headers["Authorization"] = "Token " + str(self._token)
        return r


class Dam4MLClient(object):
    """The client driver class for DAM4ML.
    """
    api = None
    project_slug = None
    _token = None
    _filter = {}

    def __init__(self, project_slug, token, api_url="https://dam4.ml/api/v1",
        tmpdir=None, persist=False):
        """Connect DAM4ML API with the given project and auth information.
        api_url: API endpoitn
        tmpdir: Temporary storage location. Default=~/.dam4ml/
        persist: If True, downloaded data will persist after the client object
            is deleted.
        """
        # Basic initialization
        self.project_slug = project_slug
        self._token = token
        self.api = slumber.API(
            api_url,
            auth=RequestsTokenAuth(token),
        )

        # Temp dir management
        if tmpdir is None:
            self.tmpdir = os.path.expanduser('~/.dam4ml')
        else:
            self.tmpdir = tmpdir
        # if persist is False:
        #       Warning: Python3 only
        #     tmpdir = tempfile.TemporaryDirectory

    def set_filter(self, **kwargs):
        """Set the filters applied to the assets listing.
        Eg. set_filter(tag_slug="test")
        """
        self._filter = kwargs

    def _iterate(self, filter_dict, offset=0, limit=10):
        """Iterate assets according to the given filter, and make sure
        pagination is handled correctly
        """
        filter_dict = filter_dict.copy()
        filter_dict['offset'] = offset
        filter_dict['limit'] = limit
        while True:
            res = self._retry_api(self.api.projects(self.project_slug).assets.get, **filter_dict)
            if not res['results']:
                return
            for item in res['results']:
                yield item
            filter_dict['offset'] += filter_dict['limit']

    @retry(retry=retry_if_exception_type(ConnectionError))
    def _retry_api(self, method, **kwargs):
        """Wrapper around tenacity to avoid Django pbs
        """
        return method(**kwargs)

    def preload(self, filter_dict=None, reset=False):
        """Preload dataset locally to accelerate things.
        This function can be very time-consuming if the dataset is huge.
        Set 'filter' to the filter dict if you want a specific filtering.
        If 'reset' is True, will re-import all files (does not use cache).
        """
        # Set filter_dict accordingly
        if filter_dict is None:
            filter_dict = self._filter
        filter_dict = filter_dict.copy()

        # Uh, what's the count, anyway?
        count = self._retry_api(
            self.api.projects(self.project_slug).assets.get,
            limit=1,
            **filter_dict
        )['count']

        # Looooop and save each file in our special structure
        for item in tqdm.tqdm(self._iterate(filter_dict), total=count):
            if not item.get('default_asset_file'):
                continue

            # Download file here, create path on-the-fly
            response = requests.get(item['default_asset_file']['url'], stream=True)
            response.raise_for_status()
            file_hash = response.headers["ETag"][1:-1]
            path_split = (
                self.tmpdir,
                file_hash[0:2],
                file_hash[2:4],
                file_hash[4:6],
                file_hash[6:],
            )

            # Make dirs, skip already existing paths
            os.makedirs(os.path.join(*path_split[:-1]), exist_ok=True)
            file_path = os.path.join(*path_split)
            if os.path.isfile(file_path) and not reset:
                continue

            # Actually get the file
            with open(file_path, 'wb') as f:
                response.raw.decode_content = True
                shutil.copyfileobj(response.raw, f)

def connect(project, auth, *args, **kw):
    """Connect the API with the given auth information
    """
    return Dam4MLClient(project, auth, *args, **kw)
