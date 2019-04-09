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
import copy

import requests

import tqdm

from .formatters import JSONFormatter


class UploadMixin(object):
    """We separate code for lisibility reasons.
    """

    #                                                                       #
    #                       NOW, THE WRITABLE PART                          #
    #                                                                       #

    def upload(
        self,
        file_or_filename,
        name=None,
        overwrite=True,
        tags=None,
        formatter=JSONFormatter(),
    ):
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
        data = {"overwrite": overwrite}

        # Open file if necessary
        if isinstance(file_or_filename, (str,)):
            f = open(file_or_filename, "rb")

        # Get relevant tags
        if tags is not None:
            data["asset_tags"] = self._convert_tags(tags)

        # Put file into the cloud, by recovering upload information
        upload_info = self._retry_api(
            self.api.projects(self.project_slug).upload_info.get
        )
        response = getattr(requests, upload_info["method"].lower())(
            url=upload_info["url"], data=upload_info["data"], files={"file": f}
        )
        upload_id = response.json()[upload_info["upload_id_attribute"]]

        # Create the asset, handle name conflicts if necessary
        data["upload_id"] = upload_id
        if name:
            data["name"] = name
        response = self._retry_api(
            self.api.projects(self.project_slug).assets.post, data=data
        )

        # Return it
        return formatter.asset_to_format(self, response)

    def uc_upload(self, path, multipart_threshold=15 * 1024 * 1024):
        """Takes (good) care of UC upload for the given filename.
        Handles multipart, etc.
        Internal use only.
        Returns an upload_id string.
        """
        # Compute basic information
        file_size = os.path.getsize(path)
        filename = os.path.split(path)[1]
        f = open(path, "rb")
        session = requests.session()

        # Recover upload information
        upload_info = self._retry_api(
            self.api.projects(self.project_slug).upload_info.get
        )
        data = upload_info["data"]

        # Not multipart
        if file_size < multipart_threshold:
            response = session.request(
                upload_info["method"].lower(),
                url=upload_info["url"],
                data=data,
                files={"file": f},
            )
            response.raise_for_status()
            return response.json()[upload_info["upload_id_attribute"]]

        # Multipart
        PART_SIZE = 5242880
        data.update(
            {
                "filename": filename,
                "size": file_size,
                "content_type": "application/octet-stream",
            }
        )
        response = session.request(
            "post", url="https://upload.uploadcare.com/multipart/start/", data=data
        )
        response.raise_for_status()
        for part in tqdm.tqdm(response.json()["parts"]):
            multipart_response = session.request(
                "put",
                url=part,
                headers={"Content-Type": "application/octet-stream"},
                data=f.read(PART_SIZE),
            )
            multipart_response.raise_for_status()
        data["uuid"] = response.json()["uuid"]
        response = session.request(
            "post", url="https://upload.uploadcare.com/multipart/complete/", data=data
        )
        response.raise_for_status()
        return response.json()["uuid"]

    def _tag_slug_to_id(self, slug):
        """Convert a tag slug into an id, using cache if necessary.
        Will raise if not found.
        """
        if slug in self._tag_slugs_cache:
            return self._tag_slugs_cache[slug]
        tags = self._retry_api(self.api.projects(self.project_slug).tags.get, slug=slug)
        if tags["count"] == 0:
            raise ValueError("Unkown tag slug: '{}'".format(slug))
        elif tags["count"] > 1:
            raise RuntimeError("Several tags for this slug. Critical API error.")
        self._tag_slugs_cache[slug] = tags["results"][0]["id"]
        return tags["results"][0]["id"]

    def _convert_tags(self, tags):
        """Convert given tags to the proper format.
        tags can be either a list of strings, a dict, or a list of dicts.
        """
        tags = copy.deepcopy(tags)
        ret = []
        for tag in tags:
            if not isinstance(tags, dict) and isinstance(tag, str):
                ret.append({"tag_id": self._tag_slug_to_id(tag)})
            elif isinstance(tags, dict) and isinstance(tag, str):
                tags[tag]["tag_id"] = self._tag_slug_to_id(tag)
                ret.append(tags[tag])
            elif isinstance(tag, dict):
                slug = tag.pop("slug")
                tag["tag_id"] = self._tag_slug_to_id(slug)
                ret.append(tag)
        return ret

    #                                                                   #
    #                           ZIP management                          #
    #                                                                   #

    def test_zip(self, zip_path, mime_types="*/*", **kwargs):
        """Dry-run the test-zip
        """
        zipf = zipfile.ZipFile(zip_path, "r")
        paths = zipf.namelist()
        # root = os.path.abspath(path)
        # for (dirpath, _, filenames) in os.walk(root):
        #     for filename in filenames:
        #         if filename.startswith("."):
        #             continue
        #         fullpath = os.path.join(dirpath, filename)
        #         arcname = fullpath[len(root) + 1:]
        #         paths.append(arcname)

        # Test the files
        data = {"paths": paths}
        data.update(kwargs)
        return self._retry_api(
            self.api.projects(self.project_slug).assets.test_zip.post,
            data=data,
            **kwargs
        )

    # # See http://lists.logilab.org/pipermail/python-projects/2012-September/003261.html
    # #pylint: disable=W0212
    # def upload_dir(self, path, **kwargs):
    #     """Actual directory upload class.
    #     This will:
    #     [- prepare a ZIP in a temp directory]
    #     - upload it to dam4ml

    #     Ignores files starting with a dot (.)

    #     Additional kwargs will be passed along to the ZIP API method.
    #     """
    #     # Put file into the cloud, by recovering upload information
    #     # upload_info = self._retry_api(
    #     #     self.api.projects(self.project_slug).upload_info.get
    #     # )

    #     # Prepare assets to create
    #     to_create = self.test_zip(path, **kwargs)

    #     # Create tags
    #     # tags_mapping = {}
    #     for tag_dict in to_create["tags"]:
    #         data = dict(slug=tag_dict['slug'])
    #         response = self._retry_api(
    #             self.api.projects(self.project_slug).tags.post, data=data, **kwargs
    #         )
    #         # tags_mapping[tag_dict['slug']] = response['id']

    #     # Create assets
    #     response = []
    #     for asset_dict in tqdm.tqdm(to_create["assets"]):
    #         response.append(
    #             self.upload(
    #                 os.path.join(path, asset_dict["default_asset_file__source_filename"]),
    #                 tags=asset_dict["asset_tag_slugs"],
    #             )
    #         )

    #     # Return it
    #     return response

    def upload_zip(self, path, **kwargs):
        """Same as upload_dir but with a zipped file
        """
        upload_id = self.uc_upload(path)

        # Upload the big ZIP
        data = {"upload_id": upload_id}
        response = self._retry_api(
            self.api.projects(self.project_slug).assets.zip.post, data=data, **kwargs
        )

        # Return it
        return response

    # See http://lists.logilab.org/pipermail/python-projects/2012-September/003261.html
    # pylint: disable=W0212
    def upload_dir(self, path, **kwargs):
        """Actual directory upload class.
        This will:
        [- prepare a ZIP in a temp directory]
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
                    arcname = fullpath[len(root) + 1 :]
                    zipf.write(os.path.join(dirpath, filename), arcname=arcname)

            # Close ZIP so that it's ready for upload
            zipf.close()

            # Go for it.
            return self.upload_zip(tmpzipf.name)
