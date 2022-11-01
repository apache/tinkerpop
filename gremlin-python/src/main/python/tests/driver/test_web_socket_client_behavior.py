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

__author__ = 'Cole Greer (cole@colegreer.ca)'

# Note: This test demonstrates different behavior in response to a server sending a close frame than the other GLV's.
# Other GLV's will respond to this by trying to reconnect. This test is also demonstrating incorrect behavior of
# client.is_closed() as it appears unaware that the event loop is dead.
# These differences from other GLV's are being tracked in [TINKERPOP-2846]. If this behavior is changed to resemble
# other GLV's, this test should be updated to show a vertex is being received by the second request.
def test_does_not_create_new_connection_if_closed_by_server(socket_server_client, socket_server_settings):
    try:
        socket_server_client.submit(
            "1", request_options={'requestId': socket_server_settings["CLOSE_CONNECTION_REQUEST_ID"]}).all().result()
    except RuntimeError as err:
        assert str(err) == "Connection was closed by server."

    assert not socket_server_client.is_closed()

    try:
        response = socket_server_client.submit(
            "1", request_options={'requestId': socket_server_settings["SINGLE_VERTEX_REQUEST_ID"]}).all().result()
    except RuntimeError as err:
        assert str(err) == "Event loop is closed"

    assert not socket_server_client.is_closed()
