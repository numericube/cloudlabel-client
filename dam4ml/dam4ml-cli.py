#!/usr/bin/env python
# encoding: utf-8
"""
command.py

Created by Pierre-Julien Grizel et al.
Copyright (c) 2016 NumeriCube. All rights reserved.

A command-line script to upload/download datasets to DAM4ML
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
import argparse
import os

import dam4ml

def get_client_from_args(parsed_args):
    """Get dam4ml client
    """
    return dam4ml.Client(
        project_slug=parsed_args.project,
        username=parsed_args.username,
        token=parsed_args.token,
        api_url=parsed_args.api_url,
    )

def upload_dir(parsed_args):
    """Upload a whole directory to your dam4ml instance.
    """
    client = get_client_from_args(parsed_args)
    response = client.dataset().upload_dir(
        parsed_args.path,
        create_tags=parsed_args.create_tags,
    )
    pprint.pprint(response)

if __name__ == "__main__":
    # Main parser and CMD line features
    parser = argparse.ArgumentParser()
    parser.add_argument("project", help="Project slug to connect to")
    parser.add_argument("--username", default=os.environ.get('DAM4ML_USERNAME'), help="Your dam4ml username")
    parser.add_argument("--token", default=os.environ.get('DAM4ML_TOKEN'), help="Your dam4ml secret token")
    parser.add_argument("--api_url", help="API URL", default="http://localhost:8000/api/v1/")
    subparsers = parser.add_subparsers(
        help="execute this command", title="subcommands", description="valid subcommands"
    )

    # create the parser for the "upload_dir" command
    parser_upload_dir = subparsers.add_parser(
        "upload_dir", help=upload_dir.__doc__,
    )
    parser_upload_dir.add_argument("path", help="directory to upload")
    parser_upload_dir.add_argument("--create-tags", action="store_true", help="Create tags on-the-fly", default=False)
    parser_upload_dir.set_defaults(func=upload_dir)

    # create the parser for the "individual upload" command
    parser_upload = subparsers.add_parser(
        "upload", help=upload_dir.__doc__,
    )
    parser_upload.add_argument("path", help="file to upload")
    parser_upload.set_defaults(func=upload_dir)

    # Let's conclude this
    args = parser.parse_args()
    args.func(args)
