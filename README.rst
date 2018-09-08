=====================
DAM4ML CLIENT LIBRARY
=====================

This is the dam4ml client library.

Installation
------------

pip install dam4ml

(Ugh, pretty is, uh?)

Basic usage
-----------

.. code-block:: python

    from dam4ml import client
    from dam4ml import transforms

    # Login to DAM4ML
    dataset = client.connect("mnist", api_key="")

    # (optional) Pre-load the whole dataset for offline performance.
    # This will take a while but will improve further performance.
    dataset.load()

    # (optional) You can pre-filter your dataset. See DAM4ML website
    # for more information about how to build your filter
    filter = {
        "tag_slug": "test",
    }

    # Iterate through all dataset items
    for item in dataset.as_dict(**filter):
        # ...process each dataset item here.
        pass

    # Convert dataset to a pynum array
    dataset.as_pynum(**filter)

    # Even better, simulate what Keras' load_dataset() method would do:
    pn_dataset = dataset.as_pynum()
    (x_train, y_train) = pn_dataset[]
    (x_val, y_val) = pn_dataset[]


