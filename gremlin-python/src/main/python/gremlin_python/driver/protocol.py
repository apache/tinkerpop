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
import base64
import struct

# import kerberos    Optional dependency imported in relevant codeblock

from gremlin_python.driver import request
from gremlin_python.driver.resultset import ResultSet
from gremlin_python.process.translator import Translator
from gremlin_python.process.traversal import Bytecode

log = logging.getLogger("gremlinpython")

__author__ = 'David M. Brown (davebshow@gmail.com)'


class GremlinServerError(Exception):
    def __init__(self, status):
        super(GremlinServerError, self).__init__('{0}: {1}'.format(status['code'], status['message']))
        self._status_attributes = status['attributes']
        self.status_code = status['code']
        self.status_message = status['message']

    @property
    def status_attributes(self):
        return self._status_attributes


class ConfigurationError(Exception):
    pass


class AbstractBaseProtocol(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def connection_made(self, transport):
        self._transport = transport

    @abc.abstractmethod
    def data_received(self, message, results_dict):
        pass

    @abc.abstractmethod
    def write(self, request_id, request_message):
        pass


class GremlinServerWSProtocol(AbstractBaseProtocol):
    QOP_AUTH_BIT = 1
    _kerberos_context = None
    _max_content_length = 10 * 1024 * 1024

    def __init__(self,
                 message_serializer,
                 username='', password='',
                 kerberized_service='',
                 max_content_length=10 * 1024 * 1024):
        self._message_serializer = message_serializer
        self._username = username
        self._password = password
        self._kerberized_service = kerberized_service
        self._max_content_length = max_content_length

    def connection_made(self, transport):
        super(GremlinServerWSProtocol, self).connection_made(transport)

    def write(self, request_id, request_message):
        message = self._message_serializer.serialize_message(request_id, request_message)
        self._transport.write(message)

    def data_received(self, message, results_dict):
        # if Gremlin Server cuts off then we get a None for the message
        if message is None:
            log.error("Received empty message from server.")
            raise GremlinServerError({'code': 500,
                                      'message': 'Server disconnected - please try to reconnect', 'attributes': {}})

        message = self._message_serializer.deserialize_message(message)
        request_id = message['requestId']
        result_set = results_dict[request_id] if request_id in results_dict else ResultSet(None, None)
        status_code = message['status']['code']
        aggregate_to = message['result']['meta'].get('aggregateTo', 'list')
        data = message['result']['data']
        result_set.aggregate_to = aggregate_to
        if status_code == 407:
            if self._username and self._password:
                auth_bytes = b''.join([b'\x00', self._username.encode('utf-8'),
                                       b'\x00', self._password.encode('utf-8')])
                auth = base64.b64encode(auth_bytes)
                request_message = request.RequestMessage(
                    'traversal', 'authentication', {'sasl': auth.decode()})
            elif self._kerberized_service:
                request_message = self._kerberos_received(message)
            else:
                error_message = 'Gremlin server requires authentication credentials in DriverRemoteConnection. ' \
                                'For basic authentication provide username and password. ' \
                                'For kerberos authentication provide the kerberized_service parameter.'
                log.error(error_message)
                raise ConfigurationError(error_message)
            self.write(request_id, request_message)
            data = self._transport.read()
            # Allow for auth handshake with multiple steps
            return self.data_received(data, results_dict)
        elif status_code == 204:
            result_set.stream.put_nowait([])
            del results_dict[request_id]
            return status_code
        elif status_code in [200, 206]:
            result_set.stream.put_nowait(data)
            if status_code == 200:
                result_set.status_attributes = message['status']['attributes']
                del results_dict[request_id]
            return status_code
        else:
            # This message is going to be huge and kind of hard to read, but in the event of an error,
            # it can provide invaluable info, so space it out appropriately.
            log.error("\r\nReceived error message '%s'\r\n\r\nWith results dictionary '%s'",
                      str(message), str(results_dict))
            del results_dict[request_id]
            raise GremlinServerError(message['status'])

    def _kerberos_received(self, message):
        # Inspired by: https://github.com/thobbs/pure-sasl/blob/0.6.2/puresasl/mechanisms.py
        #              https://github.com/thobbs/pure-sasl/blob/0.6.2/LICENSE
        try:
            import kerberos
        except ImportError:
            raise ImportError('Please install gremlinpython[kerberos].')

        # First pass: get service granting ticket and return it to gremlin-server
        if not self._kerberos_context:
            try:
                _, kerberos_context = kerberos.authGSSClientInit(
                    self._kerberized_service, gssflags=kerberos.GSS_C_MUTUAL_FLAG)
                kerberos.authGSSClientStep(kerberos_context, '')
                auth = kerberos.authGSSClientResponse(kerberos_context)
                self._kerberos_context = kerberos_context
            except kerberos.KrbError as e:
                raise ConfigurationError(
                    'Kerberos authentication requires a valid service name in DriverRemoteConnection, '
                    'as well as a valid tgt (export KRB5CCNAME) or keytab (export KRB5_KTNAME): ' + str(e))
            return request.RequestMessage('', 'authentication', {'sasl': auth})

        # Second pass: completion of authentication
        sasl_response = message['status']['attributes']['sasl']
        if not self._username:
            result_code = kerberos.authGSSClientStep(self._kerberos_context, sasl_response)
            if result_code == kerberos.AUTH_GSS_COMPLETE:
                self._username = kerberos.authGSSClientUserName(self._kerberos_context)
            return request.RequestMessage('', 'authentication', {'sasl': ''})

        # Third pass: sasl quality of protection (qop) handshake

        # Gremlin-server Krb5Authenticator only supports qop=QOP_AUTH; use ssl for confidentiality.
        # Handshake content format:
        # byte 0: the selected qop. 1==auth, 2==auth-int, 4==auth-conf
        # byte 1-3: the max length for any buffer sent back and forth on this connection. (big endian)
        # the rest of the buffer: the authorization user name in UTF-8 - not null terminated.
        kerberos.authGSSClientUnwrap(self._kerberos_context, sasl_response)
        data = kerberos.authGSSClientResponse(self._kerberos_context)
        plaintext_data = base64.b64decode(data)
        assert len(plaintext_data) == 4, "Unexpected response from gremlin server sasl handshake"
        word, = struct.unpack('!I', plaintext_data)
        qop_bits = word >> 24
        assert self.QOP_AUTH_BIT & qop_bits, "Unexpected sasl qop level received from gremlin server"

        name_length = len(self._username)
        fmt = '!I' + str(name_length) + 's'
        word = self.QOP_AUTH_BIT << 24 | self._max_content_length
        out = struct.pack(fmt, word, self._username.encode("utf-8"), )
        encoded = base64.b64encode(out).decode('ascii')
        kerberos.authGSSClientWrap(self._kerberos_context, encoded)
        auth = kerberos.authGSSClientResponse(self._kerberos_context)
        return request.RequestMessage('', 'authentication', {'sasl': auth})


class GremlinServerHTTPProtocol(AbstractBaseProtocol):

    def __init__(self,
                 message_serializer,
                 username='', password=''):
        self._message_serializer = message_serializer
        self._username = username
        self._password = password

    def connection_made(self, transport):
        super(GremlinServerHTTPProtocol, self).connection_made(transport)

    def write(self, request_id, request_message):

        basic_auth = {}
        if self._username and self._password:
            basic_auth['username'] = self._username
            basic_auth['password'] = self._password

        content_type = str(self._message_serializer.version, encoding='utf-8')
        message = {
            'headers': {'CONTENT-TYPE': content_type,
                        'ACCEPT': content_type},
            'payload': self._message_serializer.serialize_message(request_id, request_message),
            'auth': basic_auth
        }

        self._transport.write(message)

    def data_received(self, response, results_dict):
        # if Gremlin Server cuts off then we get a None for the message
        if response is None:
            log.error("Received empty message from server.")
            raise GremlinServerError({'code': 500,
                                      'message': 'Server disconnected - please try to reconnect', 'attributes': {}})

        if response['ok']:
            message = self._message_serializer.deserialize_message(response['content'])
            request_id = message['requestId']
            result_set = results_dict[request_id] if request_id in results_dict else ResultSet(None, None)
            status_code = message['status']['code']
            aggregate_to = message['result']['meta'].get('aggregateTo', 'list')
            data = message['result']['data']
            result_set.aggregate_to = aggregate_to

            if status_code == 204:
                result_set.stream.put_nowait([])
                del results_dict[request_id]
                return status_code
            elif status_code in [200, 206]:
                result_set.stream.put_nowait(data)
                if status_code == 200:
                    result_set.status_attributes = message['status']['attributes']
                    del results_dict[request_id]
                return status_code
        else:
            # This message is going to be huge and kind of hard to read, but in the event of an error,
            # it can provide invaluable info, so space it out appropriately.
            log.error("\r\nReceived error message '%s'\r\n\r\nWith results dictionary '%s'",
                      str(response['content']), str(results_dict))
            body = json.loads(response['content'])
            del results_dict[body['requestId']]
            raise GremlinServerError({'code': response['status'], 'message': body['message'], 'attributes': {}})
