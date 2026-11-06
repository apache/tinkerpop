#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import json

from gremlin_python.driver.http_request import HttpRequest
from gremlin_python.driver.request import RequestMessage


def make_request(gremlin="g.V()", fields=None):
    msg = RequestMessage(fields=fields or {"g": "g"}, gremlin=gremlin)
    return HttpRequest(method="POST", url="http://localhost:8182/gremlin",
                       headers={"accept": "application/vnd.graphbinary-v4.0"}, body=msg)


class TestBasicInterceptorExecution:

    def test_interceptor_receives_request_message_in_body(self):
        request = make_request()
        captured = []

        def interceptor(req):
            captured.append(req.body)

        interceptor(request)
        assert len(captured) == 1
        assert isinstance(captured[0], RequestMessage)

    def test_interceptor_can_read_and_modify_headers(self):
        request = make_request()
        request.headers["x-existing"] = "original"

        def interceptor(req):
            assert req.headers["x-existing"] == "original"
            req.headers["x-existing"] = "modified"
            req.headers["x-new"] = "added"

        interceptor(request)
        assert request.headers["x-existing"] == "modified"
        assert request.headers["x-new"] == "added"

    def test_interceptor_can_modify_uri(self):
        request = make_request()

        def interceptor(req):
            req.url = "http://other-host:9999/gremlin"

        interceptor(request)
        assert request.url == "http://other-host:9999/gremlin"

    def test_interceptors_run_in_registration_order(self):
        request = make_request()
        order = []

        interceptors = [
            lambda req: order.append(1),
            lambda req: order.append(2),
            lambda req: order.append(3),
        ]

        for i in interceptors:
            i(request)

        assert order == [1, 2, 3]


class TestSerializeBody:

    def test_converts_request_message_to_json_bytes(self):
        request = make_request(gremlin="g.V().count()")
        result = request.serialize_body()

        parsed = json.loads(result)
        assert parsed["gremlin"] == "g.V().count()"
        assert parsed["g"] == "g"

    def test_sets_content_type_header(self):
        request = make_request()
        request.serialize_body()
        assert request.headers["content-type"] == "application/json"

    def test_sets_content_length_header(self):
        request = make_request()
        result = request.serialize_body()
        assert request.headers["content-length"] == str(len(result))

    def test_is_idempotent_with_pre_serialized_bytes(self):
        existing = b'{"gremlin":"g.V()"}'
        request = HttpRequest(method="POST", url="http://localhost:8182/gremlin",
                              headers={}, body=existing)
        first = request.serialize_body()
        second = request.serialize_body()
        assert first is existing
        assert first is second

    def test_is_idempotent_with_request_message(self):
        request = make_request()
        first = request.serialize_body()
        second = request.serialize_body()
        assert first is second

    def test_includes_all_fields_in_json(self):
        msg = RequestMessage(
            fields={"g": "g", "language": "gremlin-lang", "timeoutMillis": 5000},
            gremlin="g.V()"
        )
        request = HttpRequest(method="POST", url="http://localhost:8182/gremlin",
                              headers={}, body=msg)
        result = request.serialize_body()
        parsed = json.loads(result)

        assert parsed["gremlin"] == "g.V()"
        assert parsed["g"] == "g"
        assert parsed["language"] == "gremlin-lang"
        assert parsed["timeoutMillis"] == 5000

    def test_raises_on_unsupported_body_type(self):
        request = HttpRequest(method="POST", url="http://localhost:8182/gremlin",
                              headers={}, body=42)
        try:
            request.serialize_body()
            assert False, "expected TypeError for unsupported body type"
        except TypeError as e:
            assert "int" in str(e)

    def test_raises_on_none_body(self):
        request = HttpRequest(method="POST", url="http://localhost:8182/gremlin",
                              headers={}, body=None)
        try:
            request.serialize_body()
            assert False, "expected TypeError for None body"
        except TypeError as e:
            assert "NoneType" in str(e)


class TestFieldMutation:

    def test_interceptor_can_replace_body_before_serialization(self):
        request = make_request()

        def interceptor(req):
            req.body = RequestMessage(fields={"g": "gmodern"}, gremlin="g.E()")

        interceptor(request)
        request.serialize_body()

        parsed = json.loads(request.body)
        assert parsed["gremlin"] == "g.E()"
        assert parsed["g"] == "gmodern"




class TestAuthInterceptorOrdering:

    def test_auth_interceptor_is_always_last(self):
        """Auth interceptor should always be appended to the end of the interceptor list."""
        from unittest.mock import MagicMock
        from gremlin_python.driver.connection import Connection

        order = []

        def interceptor1(req):
            order.append(1)

        def interceptor2(req):
            order.append(2)

        def auth_interceptor(req):
            order.append(3)

        conn = Connection(
            url="http://localhost:8182/gremlin",
            traversal_source="g",
            executor=MagicMock(),
            pool=MagicMock(),
            auth=auth_interceptor,
            interceptors=[interceptor1, interceptor2],
            enable_user_agent_on_connect=False
        )

        # Verify the internal interceptor list has auth at the end
        assert len(conn._interceptors) == 3
        assert conn._interceptors[0] is interceptor1
        assert conn._interceptors[1] is interceptor2
        assert conn._interceptors[2] is auth_interceptor

    def test_auth_interceptor_is_last_even_without_other_interceptors(self):
        """Auth interceptor works when no other interceptors are provided."""
        from unittest.mock import MagicMock
        from gremlin_python.driver.connection import Connection

        def auth_interceptor(req):
            pass

        conn = Connection(
            url="http://localhost:8182/gremlin",
            traversal_source="g",
            executor=MagicMock(),
            pool=MagicMock(),
            auth=auth_interceptor,
            enable_user_agent_on_connect=False
        )

        assert len(conn._interceptors) == 1
        assert conn._interceptors[0] is auth_interceptor
