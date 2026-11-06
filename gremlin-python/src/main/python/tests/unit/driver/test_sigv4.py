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
from botocore.credentials import Credentials

from gremlin_python.driver.auth import sigv4
from gremlin_python.driver.http_request import HttpRequest

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
