"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""
import codecs
import os
import time
from setuptools import setup

# Folder containing the setup.py
root = os.path.dirname(os.path.abspath(__file__))

# Path to __version__ module
version_file = os.path.join(root, 'gremlin_python', '__version__.py')

# Check if this is a source distribution.
# If not create the __version__ module containing the version
if not os.path.exists(os.path.join(root, 'PKG-INFO')):
    timestamp = int(os.getenv('TIMESTAMP', time.time() * 1000)) / 1000
    fd = codecs.open(version_file, 'w', 'utf-8')
    fd.write("'''")
    fd.write(__doc__)
    fd.write("'''\n")
    fd.write('version   = %r\n' % os.getenv('VERSION', '?').replace('-SNAPSHOT', '.dev-%d' % timestamp))
    fd.write('timestamp = %d\n' % timestamp)
    fd.close()
# Load version
from gremlin_python import __version__

version = __version__.version

install_requires = [
    'nest_asyncio',
    'aiohttp>=3.8.0,<4.0.0',
    'aenum>=1.4.5,<4.0.0',
    'isodate>=0.6.0,<1.0.0',
    'boto3',
    'botocore',
    'async-timeout>=4.0.3,<5.0; python_version < "3.11"',
]

setup(
    name='gremlinpython',
    version=version,
    packages=['gremlin_python', 'gremlin_python.driver',
              'gremlin_python.driver.aiohttp', 'gremlin_python.process',
              'gremlin_python.structure', 'gremlin_python.structure.io'],
    license='Apache 2',
    url='https://tinkerpop.apache.org',
    description='Gremlin-Python for Apache TinkerPop',
    maintainer='Apache TinkerPop',
    maintainer_email='dev@tinkerpop.apache.org',
    long_description=codecs.open("README.rst", "r", "UTF-8").read(),
    long_description_content_type='text/x-rst',
    test_suite="tests",
    data_files=[("", ["LICENSE", "NOTICE"])],
    setup_requires=[
        'pytest-runner==6.0.0',
        'importlib-metadata<5.0.0'
    ],
    tests_require=[
        'pytest>=4.6.4,<7.2.0',
        'radish-bdd==0.13.4',
        'PyHamcrest>=1.9.0,<3.0.0',
        'PyYAML>=5.3'
    ],
    install_requires=install_requires,
    extras_require={
        'kerberos': 'kerberos>=1.3.0,<2.0.0; sys_platform != "win32"',  # Does not install in Microsoft Windows
        'ujson': 'ujson>=2.0.0'
    },
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3"
    ],
    python_requires='>=3.9'
)
