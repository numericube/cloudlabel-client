#!/usr/bin/env python
# encoding: utf-8
"""
zipper.py

Created by Pierre-Julien Grizel et al.
Copyright (c) 2016 NumeriCube. All rights reserved.

ZIP upload management
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
import zipfile
import tempfile
import requests

class ZIPUploadMixin(object):
    """We separate code for lisibility reasons.
    """

    # See http://lists.logilab.org/pipermail/python-projects/2012-September/003261.html
    #pylint: disable=W0212
    def _upload_directory(self, path, **kwargs):
        """Actual directory upload class.
        This will:
        - prepare a ZIP in a temp directory
        - upload it to dam4ml

        Ignores files starting with a dot (.)

        Additional kwargs will be passed along to the ZIP API method.
        """
        # Now we loop through all files and lay them down in /tmp
        # in a breautiful directory that we'll later ZIP
        root = os.path.abspath(path)
        with tempfile.NamedTemporaryFile(suffix=".zip") as tmpzipf:
            zipf = zipfile.ZipFile(tmpzipf.name, "w", zipfile.ZIP_DEFLATED)
            for (dirpath, _, filenames) in os.walk(root):
                for filename in filenames:
                    if filename.startswith("."):
                        continue
                    fullpath = os.path.join(dirpath, filename)
                    arcname = fullpath[len(root) + 1:]
                    zipf.write(
                        os.path.join(dirpath, filename),
                        arcname=arcname,
                    )

            # Close ZIP so that it's ready for upload
            zipf.close()

            # Put file into the cloud, by recovering upload information
            upload_info = self.client._retry_api(
                self.client.api.projects(self.client.project_slug).upload_info.get
            )
            response = getattr(requests, upload_info["method"].lower())(
                url=upload_info["url"], data=upload_info["data"],
                files={"file": open(tmpzipf.name, "rb")}
            )
            upload_id = response.json()[upload_info["upload_id_attribute"]]

            # Upload the big ZIP
            data = {}
            data["upload_id"] = upload_id
            response = self.client._retry_api(
                self.client.api.projects(self.client.project_slug).assets.zip.post, data=data, **kwargs
            )

            # Return it
            return response
