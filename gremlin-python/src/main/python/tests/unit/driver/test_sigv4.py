#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""SigV4 SignedHeaders invariants.

Only host and the headers the AWS SDK adds itself are signed; transport-managed headers
(accept-encoding, content-type, ...) are never signed, or the signature would not match what
the server reconstructs. The session token is signed only when session credentials are used.

Unlike the other GLVs, botocore's plain SigV4Auth does not add an ``x-amz-content-sha256``
header, so Python's SignedHeaders is ``host;x-amz-date`` (the body hash is still bound into the
signature via the canonical request's mandatory payload-hash line). This is the SDK's natural
behavior and is intentionally left as-is.
"""
from unittest.mock import patch

from botocore.credentials import Credentials

from gremlin_python.driver.auth import sigv4
from gremlin_python.driver.http_request import HttpRequest
from gremlin_python.driver.request import RequestMessage

ACCESS_KEY = "foo"
SECRET_KEY = "bar"


def _signed_headers(request):
    lower = {k.lower(): v for k, v in request.headers.items()}
    authorization = lower["authorization"]
    marker = "SignedHeaders="
    start = authorization.index(marker) + len(marker)
    end = authorization.find(",", start)
    return authorization[start:] if end < 0 else authorization[start:end]


def _make_request():
    # A default-port (443) https URL; seed transport-managed / content headers that must NOT be
    # signed.
    return HttpRequest(
        method="POST",
        url="https://example.com:443/gremlin",
        headers={
            "accept": "application/vnd.graphbinary-v4.0",
            "content-type": "application/json",
            "accept-encoding": "deflate",
            "user-agent": "gremlin-python-test",
        },
        body=b'{"gremlin":"g.V().count()"}',
    )


class TestSigV4SignedHeaders:

    def test_basic_credentials_sign_only_host_and_date(self):
        creds = Credentials(access_key=ACCESS_KEY, secret_key=SECRET_KEY, token=None)
        request = _make_request()

        sigv4("region-1", "example-service", credentials=creds)(request)

        assert _signed_headers(request) == "host;x-amz-date"

    def test_session_credentials_also_sign_the_security_token(self):
        creds = Credentials(access_key=ACCESS_KEY, secret_key=SECRET_KEY, token="MOCK_TOKEN")
        request = _make_request()

        sigv4("region-1", "example-service", credentials=creds)(request)

        assert _signed_headers(request) == "host;x-amz-date;x-amz-security-token"
        lower = {k.lower(): v for k, v in request.headers.items()}
        assert lower["x-amz-security-token"] == "MOCK_TOKEN"


def _make_basic_request():
    msg = RequestMessage(fields={"g": "g"}, gremlin="g.V()")
    return HttpRequest(method="POST", url="http://localhost:8182/gremlin",
                       headers={}, body=msg)


class TestSigV4Auth:

    def test_writes_signed_headers_onto_request(self):
        # Asserts the interceptor copies the signer's headers onto the request;
        # credential resolution is covered by TestSigV4CredentialsProvider.
        def provider():
            return object()

        def fake_add_auth(aws_request):
            aws_request.headers['Authorization'] = 'AWS4-HMAC-SHA256 Credential=AKID/...'
            aws_request.headers['X-Amz-Date'] = '20260715T000000Z'

        with patch('botocore.auth.SigV4Auth') as MockAuth:
            MockAuth.return_value.add_auth.side_effect = fake_add_auth
            interceptor = sigv4('us-east-1', 'neptune-db', credentials=provider)
            request = _make_basic_request()
            interceptor(request)

        # Both signer-produced headers were written back onto the request.
        assert request.headers['Authorization'].startswith('AWS4-HMAC-SHA256')
        assert request.headers['X-Amz-Date'] == '20260715T000000Z'
