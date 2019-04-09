from setuptools import setup, find_packages

with open("README.rst", "r") as fh:
    long_description = fh.read()

setup(
    name="cloudlabel-client",
    version="1.0.0",
    author=u"NumeriCube",
    author_email="support@numericube.com",
    description="CloudLabel client library",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    url="https://github.com/numericube/cloudlabel-client",
    packages=find_packages(),
    license="GPLv3",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    ],
    entry_points="""
      # -*- Entry points: -*-
      [console_scripts]
      cloudlabel-cli = cloudlabel.client.command_line_interface:main
      """,
)
