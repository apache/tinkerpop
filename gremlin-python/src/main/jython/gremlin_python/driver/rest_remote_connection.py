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
import json
import requests

from gremlin_python.process.traversal import Traverser
from remote_connection import RemoteConnection
from remote_connection import RemoteResponse

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'


class RESTRemoteConnection(RemoteConnection):
    def __init__(self, url, traversal_source):
        RemoteConnection.__init__(self, url, traversal_source)

    def __repr__(self):
        return "RESTRemoteConnection[" + self.url + "]"

    def submit(self, target_language, bytecode):
        response = requests.post(self.url, data=json.dumps(
            {"gremlin": bytecode, "source": self.traversal_source, "language": target_language, "bindings": None}))
        if response.status_code != requests.codes.ok:
            raise BaseException(response.text)
        traversers = []
        for x in response.json()['result']['data']:
            traversers.append(Traverser(x, 1))
        return RemoteResponse(iter(traversers), {})
