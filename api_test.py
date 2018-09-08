"""Just little tests
"""

# Little API test
from dam4ml.client import connect


# Connect MNIST-Test
cli = connect(
    "mnist-test",
    "d5e5b353d5a68bfa9d52f29cbab82f3f9e66080c",
    api_url="http://localhost:8000/api/v1/"
)

# Just a quick test to check if we're okay, and filter things.
print(cli.api.projects("mnist-test").get())
cli.set_filter(
    # tag_slug="test",
)

# Pre-load files
cli.preload()


