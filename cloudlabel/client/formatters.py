#!/usr/bin/env python
# encoding: utf-8
"""
formatter.py

Created by Pierre-Julien Grizel et al.
Copyright (c) 2016 NumeriCube. All rights reserved.

A way to express an asset / dataset into different formats
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

import copy
import pprint
import collections
from abc import abstractmethod


class JSONAssetWrapper(collections.MutableMapping):
    """Wrapper around dict() to add a few utility methods to the returned object.
    """

    def __init__(self, dataset, *args, **kwargs):
        self.store = dict()
        self.dataset = dataset
        self.update(dict(*args, **kwargs))  # use the free update to set keys

    def __getitem__(self, key):
        return self.store[self.__keytransform__(key)]

    def __setitem__(self, key, value):
        self.store[self.__keytransform__(key)] = value

    def __delitem__(self, key):
        del self.store[self.__keytransform__(key)]

    def __repr__(self,):
        return pprint.pformat(self.store)

    def __str__(self,):
        return pprint.pformat(self.store)

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __keytransform__(self, key):
        return key

    def delete(self,):
        """Delete current asset
        """
        client = self.dataset.client
        client._retry_api(
            client.api.projects(client.project_slug).assets(self.store["id"]).delete
        )


class BaseFormatter(object):
    """Basic inheritance for all formatters
    """

    @abstractmethod
    def asset_to_format(self, dataset, asset):
        """Convert the given asset to the proper format.

        # Arguments
            dataset: the Dataset object, sometimes useful to get back to the client.
            asset: the na(t)ive API asset object, with its cache_filename

        # Returns
            whatever object type this is meant to return.
        """
        raise NotImplementedError


class JSONFormatter(BaseFormatter):
    """Default formatter, ISO.
    Will return the asset and underlying data as its native JSON format.
    """

    def __init__(self,):
        """Default paramters for the formatter.
        This formatter doesn't take any parameter.
        """
        pass

    def asset_to_format(self, dataset, asset):
        """Doesn't convert anything.
        """
        return JSONAssetWrapper(dataset, asset)


class TupleFormatter(BaseFormatter):
    """Transform the asset into a tuple with the given columns.
    Example:
    f = TupleFormatter(
        column.Filename(),
        column.TagRegex(r"0-9"),
    )
    """

    def __init__(self, *columns):
        """
        *columns is the list of columns you want to extract information into.
        Can be either a *Column() instance, or a simple dotted string that will
        resolve into the asset's information.
        """
        self._columns = copy.deepcopy(columns)

    def asset_to_format(self, dataset, asset):
        """Convert each column to a tuple
        """
        # XXX Use a list expression instead
        ret = []
        for column in self._columns:
            ret.append(column.format(dataset, asset))
        return tuple(ret)


class NumpyFormatter(BaseFormatter):
    """Retreive the given asset information as a numpy array.
    """

    def __init__(self,):
        """Check if everything is loadable
        """
        try:
            import numpy as np
        except ImportError:
            raise ValueError("You must install numpy for this formatter to work")
