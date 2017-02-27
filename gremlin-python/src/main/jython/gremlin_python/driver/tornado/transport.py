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
from tornado import ioloop, websocket

from gremlin_python.driver.transport import AbstractBaseTransport

__author__ = 'David M. Brown (davebshow@gmail.com)'


class TornadoTransport(AbstractBaseTransport):

    def __init__(self):
        self._loop = ioloop.IOLoop(make_current=False)

    def connect(self, url):
        self._ws = self._loop.run_sync(
            lambda: websocket.websocket_connect(url))

    def write(self, message):
        self._loop.run_sync(
            lambda: self._ws.write_message(message, binary=True))

    def read(self):
        return self._loop.run_sync(lambda: self._ws.read_message())

    def close(self):
        self._ws.close()
        self._loop.close()

    def closed(self):
        return not self._ws.protocol
