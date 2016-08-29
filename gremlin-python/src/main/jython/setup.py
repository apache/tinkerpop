'''
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
'''
import codecs
import os
import time
from setuptools import setup, Command

# Folder containing the setup.py
root = os.path.dirname(os.path.abspath(__file__))

# Path to __version__ module
version_file = os.path.join(root, '.', '__version__.py')

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
import __version__

version = __version__.version

class PyTest(Command):
    user_options = []
    def initialize_options(self):
        pass
    def finalize_options(self):
        pass
    def run(self):
        import sys,subprocess
        errno = subprocess.call([sys.executable, 'runtest.py'])
        raise SystemExit(errno)

setup(
    name='gremlinpython',
    version=version,
    packages=['gremlin_python', 'gremlin_python.driver', 'gremlin_python.process', 'gremlin_python.structure', 'gremlin_python.structure.io', 'tests'],
    license='Apache 2',
    url='http://tinkerpop.apache.org',
    description='Gremlin-Python for Apache TinkerPop',
    long_description=open("README").read(),
    test_suite="tests",
    cmdclass = {'test': PyTest},
    setup_requires=[
        'pytest-runner',
    ],
    tests_require=[
        'pytest'
    ],
    install_requires=[
        'aenum==1.4.5',
        'tornado==4.4.1'
    ]
)
