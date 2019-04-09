#!/usr/bin/env python
# encoding: utf-8
"""
attribute.py

Created by Pierre-Julien Grizel et al.
Copyright (c) 2016 NumeriCube. All rights reserved.

Different column types used to retreive information from the assets
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

import re


class BaseAttribute(object):
    """An abstract attribute type
    """

    def format(self, dataset, asset):
        """Format the given asset attribute
        """
        raise NotImplementedError()


class LocalFilename(BaseAttribute):
    """Convert a downloadable URL into a (local) filename, whatever happens.
    Return None if no file in there
    """

    def __init__(self, asset_file_attr="default_asset_file"):
        """Used to select file attribute
        """
        self.asset_file_attr = asset_file_attr

    def format(self, dataset, asset):
        """Return local path
        """
        asset_file = asset.get(self.asset_file_attr)
        if not asset_file:
            return None
        return dataset.client.download_asset_file(asset_file)


class File(LocalFilename):
    """Return an open file on the given object
    """

    def __init__(self, mode="rb", *args, **kwargs):
        """mode is used to set the mode of the opened file
        """
        super(File, self).__init__(*args, **kwargs)
        self.mode = mode

    def format(self, dataset, asset):
        """Open the file and return it
        """
        return open(super(File, self).format(dataset, asset), self.mode)


class ImageIO(LocalFilename):
    """Return a numpy array with the given image
    """

    def __init__(self, asset_file_attr="default_asset_file", **kwargs):
        """Pass along additional kwargs to imread()
        """
        try:
            # See http://lists.logilab.org/pipermail/python-projects/2012-September/003261.html
            # pylint: disable=W0612
            import imageio
        except ImportError:
            raise ImportError("Please install imageio with: 'pip install imageio'")
        self.imread_kwargs = kwargs
        super(ImageIO, self).__init__(asset_file_attr)

    def format(self, dataset, asset):
        """Read file (locally if necessary) as a numpy array
        """
        import imageio

        filename = super(ImageIO, self).format(dataset, asset)
        return imageio.imread(filename, **self.imread_kwargs)


class TagRegex(BaseAttribute):
    """Filter tags matching the given regex into a single attribute.
    What will be retreive in the attribute itself is the tag slug.
    # Arguments
        include_regex: regex to filter tags with.
    """

    include_regex = None

    def __init__(self, include_regex, flatten=True, separator=","):
        """
        # Arguments
            include_regex: regex to filter tags with
            flatten: if True (default), will return a flattened string instead of a list
        """
        self.include_regex = include_regex
        self.flatten = flatten
        self.separator = separator

    def format(self, dataset, asset):
        """Go, go, go
        """
        ret = []
        tag_slugs = [tag["slug"] for tag in asset["tags"]]
        for tag_slug in tag_slugs:
            match = re.match(self.include_regex, tag_slug)
            if not match:
                continue
            ret.append(match.group())

        # Flatten or not before returning
        if self.flatten:
            return self.separator.join(ret)
        return ret
