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

from gremlin_python.driver.request import RequestMessage


class HttpRequest:
    """Represents the HTTP request passed through the interceptor chain.

    The body starts as a RequestMessage and can be serialized to JSON bytes
    via serialize_body(). Interceptors mutate this object in place.
    """

    def __init__(self, method, url, headers, body):
        self.method = method
        self.url = url
        self.headers = headers
        self.body = body

    def serialize_body(self):
        """Serialize the body to JSON bytes if it is still a RequestMessage.

        If the body is already bytes, returns them as-is (idempotent).
        Sets the Content-Type header to application/json and Content-Length
        to the byte length of the serialized body.

        Returns:
            bytes: the serialized body

        Raises:
            TypeError: if the body is neither a RequestMessage nor bytes
        """
        if isinstance(self.body, bytes):
            return self.body

        if not isinstance(self.body, RequestMessage):
            raise TypeError(
                f"Cannot serialize body of type {type(self.body).__name__}. "
                "Expected RequestMessage or bytes."
            )

        payload = {"gremlin": self.body.gremlin}
        payload.update(self.body.fields)
        data = json.dumps(payload).encode("utf-8")
        self.body = data
        self.headers["content-type"] = "application/json"
        self.headers["content-length"] = str(len(data))
        return data
