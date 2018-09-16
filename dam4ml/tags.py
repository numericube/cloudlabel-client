#!/usr/bin/env python
# encoding: utf-8
"""
tags.py

Created by Pierre-Julien Grizel et al.
Copyright (c) 2016 NumeriCube. All rights reserved.

Tags management
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

import pprint
import collections

# import tempfile

import tqdm

from .formatters import BaseFormatter, JSONFormatter, TupleFormatter

# See http://lists.logilab.org/pipermail/python-projects/2012-September/003261.html
# pylint: disable=W0212


class JSONTagWrapper(collections.MutableMapping):
    """Wrapper around dict() to add a few utility methods to the returned object.
    """

    def __init__(self, client, *args, **kwargs):
        self.store = dict()
        self.client = client
        self.update(dict(*args, **kwargs))  # use the free update to set keys

    def __getitem__(self, key):
        return self.store[self.__keytransform__(key)]

    def __setitem__(self, key, value):
        self.store[self.__keytransform__(key)] = value

    def __delitem__(self, key):
        del self.store[self.__keytransform__(key)]

    def __iter__(self):
        return iter(self.store)

    def __str__(self,):
        return pprint.pformat(str(self.store))

    def __len__(self):
        return len(self.store)

    def __keytransform__(self, key):
        return key

    def delete(self,):
        """Delete current asset
        """
        client = self.client
        client._retry_api(
            client.api.projects(client.project_slug).tags(self.store["id"]).delete
        )


class Tags(object):
    """Abstraction of the tags of a project.
    """

    def __init__(self, client, **kwargs):
        """Initial setup. kwargs is the initial filter.
        # Arguments
            client: The client to handle
        """
        # Save parameters
        self.client = client
        self.set_filter(**kwargs)

    def create(self, slug, **kwargs):
        """Tag creation
        """
        tag_data = {"slug": slug}
        tag_data.update(kwargs)
        return JSONTagWrapper(
            self.client._retry_api(
                self.client.api.projects(self.client.project_slug).tags.post,
                data=tag_data,
            )
        )

    def set_filter(self, **kwargs):
        """Set the filters applied to the assets listing.
        Eg. set_filter(tag_slug="test")
        """
        # XXX TODO: check filter coherence
        self._filter = kwargs.copy()

    def __iter__(self,):
        """Iteration management
        """
        return self._iterate_one()

    # def __len__(self,):
    #     """Evaluate the length of the dataset (with 1 API call)
    #     """
    #     return self.client._retry_api(
    #         self.client.api.projects(self.client.project_slug).assets.get,
    #         limit=1,
    #         **self._filter
    #     )["count"]

    def _iterate_one(self, offset=0, limit=100, raw=False):
        """Iterate assets according to the given filter, and make sure
        pagination is handled correctly
        """
        # Use the given filter to iterate
        filter_dict = self._filter.copy()
        filter_dict["offset"] = offset
        filter_dict["limit"] = limit
        while True:
            res = self.client._retry_api(
                self.client.api.projects(self.client.project_slug).tags.get,
                **filter_dict
            )
            if not res["results"]:
                return
            for tag in res["results"]:
                yield JSONTagWrapper(self.client, tag)
            filter_dict["offset"] += filter_dict["limit"]
