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
import base64


def basic(username, password):
    """Returns an interceptor that adds Basic auth to the request."""
    def interceptor(request):
        credentials = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("utf-8")
        request.headers['authorization'] = f"Basic {credentials}"

    return interceptor


def sigv4(region, service, credentials=None):
    """Returns an interceptor that signs the request with AWS SigV4.

    By default credentials are sourced from the standard AWS environment
    variables (``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY`` and the optional
    ``AWS_SESSION_TOKEN``). A custom credentials provider may be supplied via
    ``credentials``; it accepts either:

    * a callable returning a botocore ``Credentials`` object (or any object with
      ``access_key``/``secret_key``/``token`` attributes), evaluated per request, or
    * a botocore ``Credentials`` object (or ``Session``) used directly.

    When no provider is given, signing falls back to the environment.
    """
    import os
    from boto3 import Session
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest

    def _resolve_credentials():
        if credentials is None:
            access_key = os.environ.get('AWS_ACCESS_KEY_ID', '')
            secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
            session_token = os.environ.get('AWS_SESSION_TOKEN', '')
            session = Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=session_token,
                region_name=region
            )
            return session.get_credentials()

        provider = credentials() if callable(credentials) else credentials
        # A botocore Session exposes get_credentials(); a Credentials object is
        # already usable as-is.
        if hasattr(provider, 'get_credentials'):
            return provider.get_credentials()
        return provider

    def interceptor(request):
        # Ensure body is serialized so we can sign it
        body_bytes = request.serialize_body()

        resolved = _resolve_credentials()

        sigv4_request = AWSRequest(method=request.method, url=request.url, data=body_bytes)
        SigV4Auth(resolved, service, region).add_auth(sigv4_request)
        request.headers.update(dict(sigv4_request.headers))

    return interceptor
