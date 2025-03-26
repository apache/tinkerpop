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
from gremlin_python.driver.protocol import GremlinServerHTTPProtocol
from gremlin_python.driver.serializer import GraphBinarySerializersV4
from gremlin_python.driver.transport import AbstractBaseTransport
from gremlin_python.driver.request import RequestMessage

class MockHTTPTransport(AbstractBaseTransport):
    def connect(self, url, headers=None):
        pass

    def write(self, message):
        self._message = message

    def get_write(self):
        return self._message

    def read(self):
        pass

    def close(self):
        pass

    def closed(self):
        pass

def test_none_request_serializer_valid():
    protocol = GremlinServerHTTPProtocol(None, GraphBinarySerializersV4(), interceptors=None)
    mock_transport = MockHTTPTransport()
    protocol.connection_made(mock_transport)
    
    message = RequestMessage(fields={}, gremlin="g.V()")
    protocol.write(message)
    written = mock_transport.get_write()

    assert written["payload"] == message
    assert 'content-type' not in written["headers"]

def test_graphbinary_request_serializer_serializes_payload():
    gb_ser = GraphBinarySerializersV4()
    protocol = GremlinServerHTTPProtocol(gb_ser, gb_ser)
    mock_transport = MockHTTPTransport()
    protocol.connection_made(mock_transport)
    
    message = RequestMessage(fields={}, gremlin="g.V()")
    protocol.write(message)
    written = mock_transport.get_write()

    assert written["payload"] == gb_ser.serialize_message(message)
    assert written["headers"]['content-type'] == str(gb_ser.version, encoding='utf-8')

def test_interceptor_allows_tuple_and_list():
    try:
        tuple = GremlinServerHTTPProtocol(None, None, interceptors=(lambda req: req))
        list = GremlinServerHTTPProtocol(None, None, interceptors=[lambda req: req])
        assert True
    except:
        assert False

def test_interceptor_doesnt_allow_any_type():
    try:
        protocol = GremlinServerHTTPProtocol(None, None, interceptors=1)
        assert False
    except TypeError:
        assert True

def test_single_interceptor_runs():
    changed_req = RequestMessage(fields={}, gremlin="changed")
    def interceptor(request):
        request['payload'] = changed_req
        return request

    protocol = GremlinServerHTTPProtocol(None, GraphBinarySerializersV4(),
                                         interceptors=interceptor)
    mock_transport = MockHTTPTransport()
    protocol.connection_made(mock_transport)
    
    message = RequestMessage(fields={}, gremlin="g.V()")
    protocol.write(message)
    written = mock_transport.get_write()

    assert written['payload'] == changed_req

def test_interceptor_works_with_request_serializer():
    gb_ser = GraphBinarySerializersV4()
    message = RequestMessage(fields={}, gremlin="g.E()")

    def assert_inteceptor(request):
        assert request['payload'] == gb_ser.serialize_message(message)
        request['payload'] = "changed"
        return request
    
    protocol = GremlinServerHTTPProtocol(gb_ser, gb_ser, interceptors=assert_inteceptor)
    mock_transport = MockHTTPTransport()
    protocol.connection_made(mock_transport)
    
    protocol.write(message)
    written = mock_transport.get_write()

    assert written["payload"] == "changed"

def test_interceptors_run_sequentially():
    def three(request): request['payload'].gremlin.append(3); return request
    def two(request): request['payload'].gremlin.append(2); return request
    def one(request): request['payload'].gremlin.append(1); return request
    protocol = GremlinServerHTTPProtocol(None, GraphBinarySerializersV4(),
                                         interceptors=[one, two, three])
    mock_transport = MockHTTPTransport()
    protocol.connection_made(mock_transport)
    
    message = RequestMessage(fields={}, gremlin=[])
    protocol.write(message)
    written = mock_transport.get_write()

    assert written["payload"].gremlin == [1, 2, 3]
