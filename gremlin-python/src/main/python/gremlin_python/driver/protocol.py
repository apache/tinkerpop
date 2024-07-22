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
import json
import logging
import abc

log = logging.getLogger("gremlinpython")

__author__ = 'David M. Brown (davebshow@gmail.com)'


class GremlinServerError(Exception):
    def __init__(self, status):
        super(GremlinServerError, self).__init__('{0}: {1}'.format(status['code'], status['message']))
        self.status_code = status['code']
        self.status_message = status['message']
        self.status_exception = status['exception']


class ConfigurationError(Exception):
    pass


class AbstractBaseProtocol(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def connection_made(self, transport):
        self._transport = transport

    @abc.abstractmethod
    def data_received(self, message, result_set):
        pass

    @abc.abstractmethod
    def write(self, request_message):
        pass


class GremlinServerHTTPProtocol(AbstractBaseProtocol):

    def __init__(self,
                 message_serializer,
                 username='', password=''):
        self._message_serializer = message_serializer
        self._username = username
        self._password = password
        self._response_msg = {'status': {'code': 0,
                                         'message': '',
                                         'exception': ''},
                              'result': {'meta': {},
                                         'data': []}}
        self._is_first_chunk = True

    def connection_made(self, transport):
        super(GremlinServerHTTPProtocol, self).connection_made(transport)

    def write(self, request_message):

        basic_auth = {}
        if self._username and self._password:
            basic_auth['username'] = self._username
            basic_auth['password'] = self._password

        content_type = str(self._message_serializer.version, encoding='utf-8')
        message = {
            'headers': {'CONTENT-TYPE': content_type,
                        'ACCEPT': content_type},
            'payload': self._message_serializer.serialize_message(request_message),
            'auth': basic_auth
        }

        self._transport.write(message)

    # data is received in chunks
    def data_received(self, response_chunk, result_set, read_completed=None, http_req_resp=None):
        # we shouldn't need to use the http_req_resp code as status is sent in response message, but leaving it for now
        if read_completed:
            status_code = self._response_msg['status']['code']
            aggregate_to = self._response_msg['result']['meta'].get('aggregateTo', 'list')
            data = self._response_msg['result']['data']
            result_set.aggregate_to = aggregate_to

            # reset response message
            self._response_msg = {'status': {'code': 0,
                                             'message': '',
                                             'exception': ''},
                                  'result': {'meta': {},
                                             'data': []}}
            self._is_first_chunk = True

            if status_code == 204 and len(data) == 0:
                result_set.stream.put_nowait([])
            elif status_code in [200, 204, 206]:
                result_set.stream.put_nowait(data)
            else:
                log.error("\r\nReceived error message '%s'\r\n\r\nWith result set '%s'",
                          str(self._response_msg), str(result_set))
                raise GremlinServerError({'code': status_code,
                                          'message': self._response_msg['status']['message'],
                                          'exception': self._response_msg['status']['exception']})
        else:
            self._response_msg = self._decode_chunk(self._response_msg, response_chunk, self._is_first_chunk)
            self._is_first_chunk = False

    def _decode_chunk(self, message, data_buffer, is_first_chunk):
        chunk_msg = self._message_serializer.deserialize_message(data_buffer, is_first_chunk)

        if 'result' in chunk_msg:
            msg_data = message['result']['data']
            chunk_data = chunk_msg['result']['data']
            message['result']['data'] = [*msg_data, *chunk_data]
        if 'status' in chunk_msg:
            status_code = chunk_msg['status']['code']
            if status_code in [200, 204, 206]:
                message.update({'status': chunk_msg['status']})
            else:
                raise GremlinServerError({'code': status_code,
                                          'message': chunk_msg['status']['message'],
                                          'exception': chunk_msg['status']['exception']})
        return message
