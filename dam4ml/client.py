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
__credits__ = ["Pierre-Julien Grizel"]
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
from tenacity import retry, retry_if_exception_type

# See http://lists.logilab.org/pipermail/python-projects/2012-September/003261.html
# pylint: disable=W0212
from .dataset import Dataset


class Client(object):
    """The client driver class for DAM4ML.
    """

    api = None
    project_slug = None
    _token = None
    _filter = {}

    def __init__(
        self,
        project_slug,
        username,
        token,
        api_url="https://dam4.ml/api/v1",
        tmpdir=None,
        persist=False,
    ):
        """Connect DAM4ML API with the given project and auth information.
        api_url: API endpoitn
        tmpdir: Temporary storage location. Default=~/.dam4ml/
        persist: If True, downloaded data will persist after the client object
            is deleted.
        preload: If True, will pre-load data at first call or if filtering changes.
        """
        # Basic initialization
        self.project_slug = project_slug
        self._token = token
        self.api = slumber.API(api_url, auth=(username, token))
        self._cached = False

        # Temp dir management
        if tmpdir is None:
            self.tmpdir = os.path.expanduser("~/.dam4ml")
        else:
            self.tmpdir = tmpdir

    def dataset(self, *args, **kwargs):
        """Shortcut method to retreive a Dataset() object.
        """
        return Dataset(client=self, *args, **kwargs)

    def get_cache_path(self, file_hash):
        """Convert a file hash to a local path
        """
        path_split = (
            self.tmpdir,
            file_hash[0:2],
            file_hash[2:4],
            file_hash[4:6],
            file_hash[6:],
        )
        return os.path.join(*path_split)

    def download_asset_file(self, asset_file, reset=False):
        """Load file for ONE asset, return its path (or None if irrelevant)
        AND update the asset dict accordingly.
        """
        # Check hash and path
        file_hash = asset_file["sha256"]
        file_path = self.get_cache_path(file_hash)

        # File already exists? Well, that's good.
        if os.path.isfile(file_path) and not reset:
            return file_path

        # Download it
        response = requests.get(asset_file["download_url"], stream=True)
        response.raise_for_status()

        # Create intermediate dirs, download file
        # XXX TODO: use atomic copy to be sure
        os.makedirs(os.path.split(file_path)[0], exist_ok=True)
        with open(file_path, "wb") as f:
            response.raw.decode_content = True
            shutil.copyfileobj(response.raw, f)
        return file_path

    @retry(retry=retry_if_exception_type(ConnectionError))
    def _retry_api(self, method, **kwargs):
        """Wrapper around tenacity to avoid Django pbs
        """
        return method(**kwargs)


# def connect(project, auth, *args, **kw):
#     """Connect the API with the given auth information
#     """
#     return Dam4MLClient(project, auth, *args, **kw)
