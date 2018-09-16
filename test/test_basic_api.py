#!/usr/bin/env python
# encoding: utf-8
"""
test_basic_api.py

Created by Pierre-Julien Grizel et al.
Copyright (c) 2016 NumeriCube. All rights reserved.

Test basic API features
"""
from __future__ import unicode_literals

__author__ = ""
__copyright__ = "Copyright 2016, NumeriCube"
__credits__ = ["Pierre-Julien Grizel", ]
__license__ = "CLOSED SOURCE"
__version__ = "TBD"
__maintainer__ = "Pierre-Julien Grizel"
__email__ = "pjgrizel@numericube.com"
__status__ = "Production"

import dam4ml

def _get_mnist_test_client():
    # Connect MNIST-Test
    return dam4ml.Client(
        project_slug="mnist-test",
        username="numericube",
        token="dK_fm2Ijg3pa09gSfnU8_QWXE81yLkOgHNLVxyiQvy8",
        api_url="http://localhost:8000/api/v1/",
    )

def _get_test_client():
    # Connect Test
    return dam4ml.Client(
        project_slug="test",
        username="numericube",
        token="dSh3WYSIw5VEy0FVn23uNImlLE0-Nn11hL9Aj5PXA1w",
        api_url="http://localhost:8000/api/v1/",
    )

def test_basic_mnist():
    """Test basic MNIST features
    """
    # Connect MNIST-Test
    client = _get_mnist_test_client()

    # Just a quick test to check if we're okay, and filter things.
    # print(dataset.api.projects("mnist-test").get())
    x_y_formatter = dam4ml.formatters.TupleFormatter(
        dam4ml.attributes.ImageIO(), dam4ml.attributes.TagRegex(r"[0-9]", flatten=True)
    )
    test_dataset = client.dataset(tag_slug="test", formatter=x_y_formatter)
    val_dataset = client.dataset(
        tag_slug="validation",
        formatter=(
            dam4ml.attributes.ImageIO(),
            dam4ml.attributes.TagRegex(r"[0-9]", flatten=True),
        ),
        batch_size=100000,
    )

    # [OPTIONAL] Preload dataset
    val_dataset.load()

    # [OPTIONAL] Check/Print dataset size
    print(len(test_dataset))
    print(test_dataset[4])

    # Open each file just to check if they're okay
    for asset in test_dataset:
        print(asset[0].shape, asset[1])

    # Another way to see the problem is as a batched result
    for x_val, y_val in val_dataset:
        assert len(x_val) == len(y_val)

def test_upload():
    """Test basic upload
    """
    # Connect client, patiently delete everything
    client = _get_test_client()
    for asset in client.dataset():
        asset.delete()
    for tag in client.tags():
        tag.delete()

    # Let's upload a sample image (by filename)
    try:
        asset1 = client.dataset().upload(
            "./requirements.txt",
            tags=("3", "9", "abc"),
        )
    except ValueError:
        pass # Ok, tag slug doesn't exist
    else:
        assert False

    # Haha it was a joke. I need to create the tags first
    client.tags().create("3")
    client.tags().create("9")
    client.tags().create("abc")
    asset1 = client.dataset().upload(
        "./requirements.txt",
        tags=("3", "9", "abc"),
    )
    assert asset1["default_asset_file"]["path"] == "requirements.txt"
    assert set([ tag["slug"] for tag in asset1["tags"] ]) == set(("3", "9", "abc"))

    # Either upload if sha256 doesn't exist, or update if it does.
    asset2 = client.dataset().upload(
        "./requirements.txt",
        tags=("3", "9", "abc"),
    )
    assert asset2["default_asset_file"]["path"] == "requirements.txt"
    assert asset1["id"] == asset2["id"]
