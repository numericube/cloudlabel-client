from setuptools import setup, find_packages

with open('README.rst', 'r') as fh:
    long_description = fh.read()

setup(
    name='dam4ml',
    version='1.0.0',
    author=u'NumeriCube',
    author_email='support@numericube.com',
    description='Dam4ML client library',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    url='https://github.com/numericube/dam4ml-client',
    packages=find_packages(),
    license='GPLv3',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)'
    ]
)
