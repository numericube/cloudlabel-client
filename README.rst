=====================
DAM4ML CLIENT LIBRARY
=====================

This is the CloudLabel client library.

Installation
------------

pip install cloudlabel-client

(Ugh, pretty is, uh?)

Basic usage
-----------

.. code-block:: python

    from cloudlabel import CloudLabelClient, Dataset, formatter, attribute

    # Connect MNIST-Test
    client = CloudLabelClient(
        project_slug="mnist-test",
        username="numericube",
        token="dK_fm2Ijg3pa09gSfnU8_QWXE81yLkOgHNLVxyiQvy8",
        api_url="http://localhost:8000/api/v1/",
    )

    # Just a quick test to check if we're okay, and filter things.
    # print(dataset.api.projects("mnist-test").get())
    x_y_formatter = formatter.TupleFormatter(
        attribute.ImageIO(),
        attribute.TagRegex(r"[0-9]", flatten=True),
    )
    test_dataset = Dataset(client, tag_slug="test", formatter=x_y_formatter)
    val_dataset = Dataset(client, tag_slug="validation", formatter=x_y_formatter)

    # [OPTIONAL] Preload dataset
    # test_dataset.load()

    # [OPTIONAL] Check/Print dataset size
    print(len(test_dataset))
    print(test_dataset[4])

    # Open each file just to check if they're okay
    for asset in test_dataset:
        print(asset[0].shape, asset[1])

    # Ok, then... to simulate what Keras' load_dataset() method would do:
    (x_train, y_train) = pn_dataset[]
    (x_val, y_val) = pn_dataset[]


Upload data
-----------

ZIP upload
%%%%%%%%%%%

So you've got a ZIP file you want to upload? Easy.








