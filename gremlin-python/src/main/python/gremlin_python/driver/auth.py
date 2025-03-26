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


def basic(username, password):
    from aiohttp import BasicAuth as aiohttpBasicAuth

    def apply(request):
        return request['headers'].update({'authorization': aiohttpBasicAuth(username, password).encode()})

    return apply


def sigv4(region, service):
    import os
    from boto3 import Session
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest

    def apply(request):
        access_key = os.environ.get('AWS_ACCESS_KEY_ID', '')
        secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
        session_token = os.environ.get('AWS_SESSION_TOKEN', '')

        session = Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            region_name=region
        )

        sigv4_request = AWSRequest(method="POST", url=request['url'], data=request['payload'])
        SigV4Auth(session.get_credentials(), service, region).add_auth(sigv4_request)
        request['headers'].update(sigv4_request.headers)
        request['payload'] = sigv4_request.data
        return request

    return apply

