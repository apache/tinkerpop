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

    def __init__(self, request_serializer, response_serializer,
                 interceptors=None, auth=None):
        if callable(interceptors):
            interceptors = [interceptors]
        elif not (isinstance(interceptors, tuple)
                  or isinstance(interceptors, list)
                  or interceptors is None):
            raise TypeError("interceptors must be a callable, tuple, list or None")

        self._auth = auth
        self._interceptors = interceptors
        self._request_serializer = request_serializer
        self._response_serializer = response_serializer
        self._response_msg = {'status': {'code': 0,
                                         'message': '',
                                         'exception': ''},
                              'result': {'meta': {},
                                         'data': []}}
        self._is_first_chunk = True

    def connection_made(self, transport):
        super(GremlinServerHTTPProtocol, self).connection_made(transport)

    def write(self, request_message):
        accept = str(self._response_serializer.version, encoding='utf-8')
        message = {
            'headers': {'accept': accept},
            'payload': self._request_serializer.serialize_message(request_message)
                if self._request_serializer is not None else request_message,
            'auth': self._auth
        }

        # The user may not want the payload to be serialized if they are using an interceptor.
        if self._request_serializer is not None:
            content_type = str(self._request_serializer.version, encoding='utf-8')
            message['headers']['content-type'] = content_type

        for interceptor in self._interceptors or []:
            message = interceptor(message)

        self._transport.write(message)

    '''
    GraphSON does not support streaming deserialization, we are aggregating data and bypassing streamed
     deserialization while GraphSON is enabled for testing. Remove after GraphSON is removed.
    '''
    def data_received_aggregate(self, response, result_set):
        response_msg = {'status': {'code': 0,
                                         'message': '',
                                         'exception': ''},
                              'result': {'meta': {},
                                         'data': []}}

        response_msg = self._decode_chunk(response_msg, response, self._is_first_chunk)

        self._is_first_chunk = False
        status_code = response_msg['status']['code']
        aggregate_to = response_msg['result']['meta'].get('aggregateTo', 'list')
        data = response_msg['result']['data']
        result_set.aggregate_to = aggregate_to
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
        chunk_msg = self._response_serializer.deserialize_message(data_buffer, is_first_chunk)

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
