#!/usr/bin/env python
# encoding: utf-8
"""
dataset.py

Created by Pierre-Julien Grizel et al.
Copyright (c) 2016 NumeriCube. All rights reserved.

Abstraction of a dataset
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


import collections

# import tempfile

import tqdm

from .formatters import BaseFormatter, JSONFormatter, TupleFormatter

# See http://lists.logilab.org/pipermail/python-projects/2012-September/003261.html
# pylint: disable=W0212


class Dataset(object):
    """Abstraction of a dataset.
    A DS is a pre-filtered instance of the API.
    """

    def __init__(self, client, formatter=JSONFormatter(), batch_size=None, **kwargs):
        """Initial setup. kwargs is the initial filter.
        # Arguments
            client: The client to handle
            formatter: How to format client data
            batch_size: Group results by this side. If 'None', no batch is applied.
            kwargs: Additional filter arguments (used to filter dataset)
        """
        # Save parameters
        self.client = client
        self.batch_size = batch_size
        self.set_filter(**kwargs)

        # Smart way of finding the proper formatter implicitly
        # See http://lists.logilab.org/pipermail/python-projects/2012-September/003261.html
        # pylint: disable=R0204
        if isinstance(formatter, collections.Sequence):
            self.formatter = TupleFormatter(*formatter)
        elif isinstance(formatter, BaseFormatter):
            self.formatter = formatter
        elif isinstance(formatter, dict):
            raise NotImplementedError("Sorry guys, I only have 2 arms")
        else:
            raise ValueError(
                "Invalid type for 'formatter', must be sequence, dict or *Formatter"
            )

    def set_filter(self, **kwargs):
        """Set the filters applied to the assets listing.
        Eg. set_filter(tag_slug="test")
        """
        # XXX TODO: check filter coherence
        self._filter = kwargs.copy()

    def _to_format(self, asset):
        """Convert asset to the proper format
        """
        return self.formatter.asset_to_format(self, asset)

    def __iter__(self,):
        """Iteration management
        """
        if self.batch_size is not None:
            return self._iterate_batch()
        else:
            return self._iterate_one()

    def __getitem__(self, idx):
        """Return item at given position.
        """
        # Use the given filter to fetch
        filter_dict = self._filter.copy()
        filter_dict["offset"] = idx
        filter_dict["limit"] = 1
        res = self.client._retry_api(
            self.client.api.projects(self.client.project_slug).assets.get, **filter_dict
        )
        if not res["results"]:
            return IndexError()
        assert len(res["results"]) == 1
        asset = res["results"][0]
        return self._to_format(asset)
        # asset_file = asset.get('default_asset_file')
        # if asset_file:
        #     self.client.download_asset_file(asset_file)
        # return asset

    def __len__(self,):
        """Evaluate the length of the dataset (with 1 API call)
        """
        return self.client._retry_api(
            self.client.api.projects(self.client.project_slug).assets.get,
            limit=1,
            **self._filter
        )["count"]

    def _iterate_batch(self, raw=False):
        """Iterate assets according to the given filter, and make sure
        pagination is handled correctly.
        """
        # Use the given filter to iterate
        assert self.batch_size
        filter_dict = self._filter.copy()
        filter_dict["offset"] = 0
        filter_dict["limit"] = self.batch_size
        while True:
            res = self.client._retry_api(
                self.client.api.projects(self.client.project_slug).assets.get,
                **filter_dict
            )
            if not res["results"]:
                return
            batch = []
            for asset in res["results"]:
                if not raw:
                    batch.append(self._to_format(asset))
                else:
                    batch.append(asset)

            # Empty list management
            if not batch:
                return

            # Batch vs reversed batch
            # XXX THIS IS SUBOPTIMAL
            if isinstance(batch[0], collections.Sequence) and not isinstance(
                batch[0], str
            ):
                r_batch = []
                # import pytest;pytest.set_trace()
                for x_col in range(len(batch[0])):
                    r_batch.append([item[x_col] for item in batch])
                yield tuple(r_batch)
            else:
                yield tuple(batch)
            filter_dict["offset"] += filter_dict["limit"]

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
                self.client.api.projects(self.client.project_slug).assets.get,
                **filter_dict
            )
            if not res["results"]:
                return
            for asset in res["results"]:
                if not raw:
                    yield self._to_format(asset)
                else:
                    yield asset
            filter_dict["offset"] += filter_dict["limit"]

    def load(self):
        """Preload dataset locally to accelerate things.
        This function can be very time-consuming if the dataset is huge.
        """
        # Uh, what's the count, anyway?
        count = len(self)

        # Looooop and save each file in our special structure
        for asset in tqdm.tqdm(self._iterate_one(raw=True), total=count):
            self._to_format(asset)
