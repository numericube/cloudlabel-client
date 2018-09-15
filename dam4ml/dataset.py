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
import copy

# import tempfile
import requests
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


    def _tag_slug_to_id(self, slug):
        """Convert a tag slug into an id, using cache if necessary.
        Will raise if not found.
        """
        if slug in self.client._tag_slugs_cache:
            return self.client._tag_slugs_cache[slug]
        tags = self.client._retry_api(
            self.client.api.projects(self.client.project_slug).tags.get,
            slug=slug,
        )
        if tags["count"] == 0:
            raise ValueError("Unkown tag slug: '{}'".format(slug))
        elif tags["count"] > 1:
            raise RuntimeError("Several tags for this slug. Critical API error.")
        self.client._tag_slugs_cache[slug] = tags["results"][0]["id"]
        return tags["results"][0]["id"]

    def _convert_tags(self, tags):
        """Convert given tags to the proper format.
        tags can be either a list of strings, a dict, or a list of dicts.
        """
        tags = copy.deepcopy(tags)
        ret = []
        for tag in tags:
            if not isinstance(tags, dict) and isinstance(tag, str):
                ret.append({
                    "tag_id": self._tag_slug_to_id(tag),
                })
            elif isinstance(tags, dict) and isinstance(tag, str):
                tags[tag]["tag_id"] = self._tag_slug_to_id(tag)
                ret.append(tags[tag])
            elif isinstance(tag, dict):
                slug = tag.pop("slug")
                tag["tag_id"] = self._tag_slug_to_id(slug)
                ret.append(tag)
        return ret

    #                                                                       #
    #                       NOW, THE WRITABLE PART                          #
    #                                                                       #

    def upload(self, file_or_filename, name=None, overwrite=True, tags=None,
        formatter=JSONFormatter()):
        """Upload either from a stream or a filename.
        If we can compute sha256 (basically, if we can seek(0) the file), then
        we'll check against duplicates, to avoid unnecessary uploads.

        Basically:
        if 'overwrite' is False, we'll hang if an asset with the same name exists.
        If it's True (default), if we find *exactly* one match either by filename or by
        asset name, we'll replace it. If 0 match, we create a new one.
        If several match, we return a 400 error.

        Keeping 'overwrite' always True ensures your dataset only contains
        only one of each default_asset_file.

        # Returns
            asset (the JSON versino)

        # Arguments
            file_or_filename: Either a filename or a stream object.
                Stream must be in binary mode!
            name: Set the asset name. If "None", will be deduced from filename,
                with possible addition of numbers at the end to avoid name conflicts.
            tags: Exactly the same format as set_tags(tags). If None,
                it's just ignored and tags are neither created nor changed.
                If given, will replace *ALL* tags from the given asset.
            strict: If True (default), new tags won't be automatically created.
            formatter: Will use this formatter to return the uploaded asset.
        """
        # Oour main structure
        data = {
            "overwrite": overwrite,
        }

        # Open file if necessary
        if isinstance(file_or_filename, (str,)):
            f = open(file_or_filename, "rb")

        # Get relevant tags
        if tags is not None:
            data["asset_tags"] = self._convert_tags(tags)

        # Put file in UC, by recovering important information
        upload_info = self.client._retry_api(
            self.client.api.projects(self.client.project_slug).upload_info.get,
        )
        response = getattr(requests, upload_info['method'].lower())(
            url=upload_info['url'],
            data=upload_info['data'],
            files={
                'file': f,
            },
        )
        upload_id = response.json()[upload_info["upload_id_attribute"]]

        # Create the asset, handle name conflicts if necessary
        data["upload_id"] = upload_id
        if name:
            data["name"] = name
        response = self.client._retry_api(
            self.client.api.projects(self.client.project_slug).assets.post,
            data=data,
        )

        # Return it
        return formatter.asset_to_format(self, response)
