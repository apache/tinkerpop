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
import abc
import base64

import six
try:
    import ujson as json
except ImportError:
    import json

from gremlin_python.driver import serializer, request

__author__ = 'David M. Brown (davebshow@gmail.com)'


class GremlinServerError(Exception):
    pass


@six.add_metaclass(abc.ABCMeta)
class AbstractBaseProtocol:

    @abc.abstractmethod
    def connection_made(self, transport):
        self._transport = transport

    @abc.abstractmethod
    def data_received(self, message):
        pass

    @abc.abstractmethod
    def write(self, request_id, request_message):
        pass


class GremlinServerWSProtocol(AbstractBaseProtocol):

    def __init__(self, message_serializer, username='', password=''):
        self._message_serializer = message_serializer
        self._username = username
        self._password = password

    def connection_made(self, transport):
        super(GremlinServerWSProtocol, self).connection_made(transport)

    def write(self, request_id, request_message):
        message = self._message_serializer.serialize_message(
            request_id, request_message)
        self._transport.write(message)

    def data_received(self, data, results_dict):
        data = json.loads(data.decode('utf-8'))
        request_id = data['requestId']
        result_set = results_dict[request_id]
        status_code = data['status']['code']
        aggregate_to = data['result']['meta'].get('aggregateTo', 'list')
        result_set.aggregate_to = aggregate_to
        if status_code == 407:
            auth = b''.join([b'\x00', self._username.encode('utf-8'),
                             b'\x00', self._password.encode('utf-8')])
            request_message = request.RequestMessage(
                'traversal', 'authentication',
                {'sasl': base64.b64encode(auth).decode()})
            self.write(request_id, request_message)
            data = self._transport.read()
            self.data_received(data, results_dict)
        elif status_code == 204:
            result_set.stream.put_nowait([])
            del results_dict[request_id]
        elif status_code in [200, 206]:
            results = []
            for msg in data["result"]["data"]:
                results.append(
                    self._message_serializer.deserialize_message(msg))
            result_set.stream.put_nowait(results)
            if status_code == 206:
                data = self._transport.read()
                self.data_received(data, results_dict)
            else:
                del results_dict[request_id]
        else:
            del results_dict[request_id]
            raise GremlinServerError(
                "{0}: {1}".format(status_code, data["status"]["message"]))
