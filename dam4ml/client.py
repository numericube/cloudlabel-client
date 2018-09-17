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
import copy
import logging
import mimetypes

# import tempfile

import slumber
import tqdm
# from slumber.exceptions import HttpClientError
import requests
from requests.exceptions import ConnectionError
from tenacity import retry, retry_if_exception_type

# See http://lists.logilab.org/pipermail/python-projects/2012-September/003261.html
# pylint: disable=W0212
from .dataset import Dataset
from .formatters import JSONFormatter
from .tags import Tags


class Client(object):
    """The client driver class for DAM4ML.
    """

    api = None
    project_slug = None
    _token = None
    _filter = {}
    _tag_slugs_cache = {}

    def __init__(
        self,
        project_slug,
        username=None,
        token=None,
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
        if username is None:
            username = os.environ['DAM4ML_USERNAME']
        if token is None:
            token = os.environ['DAM4ML_TOKEN']
        self.project_slug = project_slug
        self._token = token
        self.api = slumber.API(api_url, auth=(username, token))
        self._cached = False

        # Check if connection is ok (will raise in case of a pb)
        self.api.projects(self.project_slug).get()

        # Temp dir management
        if tmpdir is None:
            self.tmpdir = os.path.expanduser("~/.dam4ml")
        else:
            self.tmpdir = tmpdir

    def dataset(self, *args, **kwargs):
        """Shortcut method to retreive a Dataset() object.
        """
        return Dataset(client=self, *args, **kwargs)

    def tags(self, *args, **kwargs):
        """Kind of the same as dataset() but for... tags ;)
        """
        return Tags(client=self, *args, **kwargs)

    # def update_tags(self, asset_id, tags=None):
    #     """set_tags(self, asset_id, tags=None)

    #     Set tags on a given asset, with the following rules.
    #     - tags can be a list of slugs or a more refined list of dicts.
    #     Dict format is {<tag_slug>: <content>}, where <content> can be:
    #         - None (asset without content)
    #         - A json-serializable object (asset content)
    #         - False, IN THIS CASE ALL THE ASSET_TAG WITH THIS SLUG WILL BE REMOVED.

    #     Ok, here are some examples:
    #     tags=("test", "1st-batch", )
    #         => Will set "test" and "1st-batch" within this asset.
    #     tags=({"test": None, "1st-batch": None, )
    #         => Same as above.
    #     tags=({"test": False, "1st-batch", )
    #         => Will remove *ALL* "test" tags and set "1st-batch" within this asset.
    #     tags=({"test": (1, 2, 3), )
    #         => Will enforce *ALL* "test" tags and leave "1st-batch" alone.
    #     """

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
        """Wrapper around tenacity to avoid Django pbs.
        Is also a little more verbose in case of exceptions
        """
        try:
            return method(**kwargs)
        except slumber.exceptions.HttpClientError as exc:
            logging.warning("HTTP Error %s:%s", str(exc), exc.content)
            raise


    def uc_upload(self, path, multipart_threshold=15*1024*1024):
        """Takes (good) care of UC upload for the given filename.
        Handles multipart, etc.
        Internal use only.
        Returns an upload_id string.
        """
        # Compute basic information
        file_size = os.path.getsize(path)
        filename = os.path.split(path)[1]
        f = open(path, 'rb')
        session = requests.session()

        # Recover upload information
        upload_info = self._retry_api(
            self.api.projects(self.project_slug).upload_info.get
        )
        data = upload_info['data']

        # Not multipart
        if file_size < multipart_threshold:
            response = session.request(
                upload_info["method"].lower(),
                url=upload_info["url"],
                data=data,
                files={'file': f},
            )
            response.raise_for_status()
            return response.json()[upload_info["upload_id_attribute"]]

        # Multipart
        PART_SIZE = 5242880
        data.update({
            "filename": filename,
            "size": file_size,
            "content_type": "application/octet-stream",
        })
        response = session.request(
            "post",
            url="https://upload.uploadcare.com/multipart/start/",
            data=data
        )
        response.raise_for_status()
        for part in tqdm.tqdm(response.json()['parts']):
            multipart_response = session.request(
                "put",
                url=part,
                headers={
                    "Content-Type": "application/octet-stream"
                },
                data=f.read(PART_SIZE),
            )
            multipart_response.raise_for_status()
        data["uuid"] = response.json()["uuid"]
        response = session.request(
            "post",
            url="https://upload.uploadcare.com/multipart/complete/",
            data=data
        )
        response.raise_for_status()
        return response.json()["uuid"]


# def connect(project, auth, *args, **kw):
#     """Connect the API with the given auth information
#     """
#     return Dam4MLClient(project, auth, *args, **kw)
