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
from tornado import ioloop, websocket
from tornado import httpclient

from gremlin_python.driver.transport import AbstractBaseTransport

__author__ = 'David M. Brown (davebshow@gmail.com)'


class TornadoTransport(AbstractBaseTransport):

    def __init__(self, read_timeout=None, write_timeout=None,
                 compression_options={'compression_level': 5, 'mem_level': 5}):
        self._loop = ioloop.IOLoop(make_current=False)
        self._ws = None
        self._read_timeout = read_timeout
        self._write_timeout = write_timeout
        self._compression_options = compression_options

    def connect(self, url, headers=None):
        if headers:
            url = httpclient.HTTPRequest(url, headers=headers)
        self._ws = self._loop.run_sync(
            lambda: websocket.websocket_connect(url, compression_options=self._compression_options))

    def write(self, message):
        self._loop.run_sync(
            lambda: self._ws.write_message(message, binary=True), timeout=self._write_timeout)

    def read(self):
        return self._loop.run_sync(lambda: self._ws.read_message(), timeout=self._read_timeout)

    def close(self):
        self._ws.close()
        self._loop.close()

    def closed(self):
        return not self._ws.protocol
