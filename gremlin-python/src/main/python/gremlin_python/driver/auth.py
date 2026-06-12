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


def sigv4(region, service):
    """Returns an interceptor that signs the request with AWS SigV4."""
    import os
    from boto3 import Session
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest

    def interceptor(request):
        # Ensure body is serialized so we can sign it
        body_bytes = request.serialize_body()

        access_key = os.environ.get('AWS_ACCESS_KEY_ID', '')
        secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
        session_token = os.environ.get('AWS_SESSION_TOKEN', '')

        session = Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            region_name=region
        )

        sigv4_request = AWSRequest(method=request.method, url=request.url, data=body_bytes)
        SigV4Auth(session.get_credentials(), service, region).add_auth(sigv4_request)
        request.headers.update(dict(sigv4_request.headers))

    return interceptor
