#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import os
from aiohttp import BasicAuth as aiohttpBasicAuth
from gremlin_python.driver.auth import basic, sigv4


def create_mock_request():
    return {'headers':
            {'content-type': 'application/vnd.graphbinary-v4.0',
             'accept': 'application/vnd.graphbinary-v4.0'},
            'payload': b'',
            'url': 'https://test_url:8182/gremlin'}


class TestAuth(object):

    def test_basic_auth_request(self):
        mock_request = create_mock_request()
        assert 'authorization' not in mock_request['headers']
        basic('username', 'password')(mock_request)
        assert 'authorization' in mock_request['headers']
        assert aiohttpBasicAuth('username', 'password').encode() == mock_request['headers']['authorization']

    def test_sigv4_auth_request(self):
        mock_request = create_mock_request()
        assert 'Authorization' not in mock_request['headers']
        assert 'X-Amz-Date' not in mock_request['headers']
        os.environ['AWS_ACCESS_KEY_ID'] = 'MOCK_ID'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'MOCK_KEY'
        sigv4('gremlin-east-1', 'tinkerpop-sigv4')(mock_request)
        assert mock_request['headers']['X-Amz-Date'] is not None
        assert mock_request['headers']['Authorization'].startswith('AWS4-HMAC-SHA256 Credential=MOCK_ID')
        assert 'gremlin-east-1/tinkerpop-sigv4/aws4_request' in mock_request['headers']['Authorization']
        assert 'Signature=' in mock_request['headers']['Authorization']

    def test_sigv4_auth_request_session_token(self):
        mock_request = create_mock_request()
        assert 'Authorization' not in mock_request['headers']
        assert 'X-Amz-Date' not in mock_request['headers']
        assert 'X-Amz-Security-Token' not in mock_request['headers']
        os.environ['AWS_SESSION_TOKEN'] = 'MOCK_TOKEN'
        sigv4('gremlin-east-1', 'tinkerpop-sigv4')(mock_request)
        assert mock_request['headers']['X-Amz-Date'] is not None
        assert mock_request['headers']['Authorization'].startswith('AWS4-HMAC-SHA256 Credential=')
        assert mock_request['headers']['X-Amz-Security-Token'] == 'MOCK_TOKEN'
        assert 'gremlin-east-1/tinkerpop-sigv4/aws4_request' in mock_request['headers']['Authorization']
        assert 'Signature=' in mock_request['headers']['Authorization']


