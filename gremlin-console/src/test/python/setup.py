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
import sys
import time
from setuptools import setup

setup(
    name='gremlinconsoletest',
    test_suite="tests",
    setup_requires=[
        'pytest-runner==6.0.0',
        'importlib-metadata<5.0.0'
    ],
    tests_require=[
        'pytest>=4.6.4,<7.2.0',
        'mock>=3.0.5,<5.1.0',
        'pexpect==4.9.0'
    ]
)
