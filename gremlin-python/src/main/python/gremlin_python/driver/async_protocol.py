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
import asyncio
import base64
import logging

from gremlin_python.driver import request
from gremlin_python.driver.protocol import (
    ConfigurationError,
    GremlinServerError,
    GremlinServerWSProtocol,
)

log = logging.getLogger("gremlinpython")

__author__ = 'Apache TinkerPop (dev@tinkerpop.apache.org)'


class AsyncGremlinServerWSProtocol(GremlinServerWSProtocol):
    """Async version of :class:`~gremlin_python.driver.protocol.GremlinServerWSProtocol`.

    Both :meth:`write` and :meth:`data_received` are coroutines that must be
    awaited.  The transport assigned via :meth:`connection_made` must be an
    :class:`~gremlin_python.driver.aiohttp.async_transport.AsyncAiohttpWSTransport`
    (i.e. its ``write`` and ``read`` methods must be coroutines).
    """

    async def write(self, request_id, request_message):
        """Serialize *request_message* and send it over the WebSocket."""
        message = self._message_serializer.serialize_message(
            request_id, request_message
        )
        await self._transport.write(message)

    async def data_received(self, message, results_dict):
        """Deserialize a raw WebSocket frame and dispatch results.

        Deserialization is offloaded to the default executor so that the event
        loop is not blocked by CPU-bound work.

        Returns the Gremlin Server response status code (200, 204, or 206).
        Raises :class:`~gremlin_python.driver.protocol.GremlinServerError` for
        error status codes.
        """
        if message is None:
            log.error("Received empty message from server.")
            raise GremlinServerError(
                {
                    "code": 500,
                    "message": "Server disconnected - please try to reconnect",
                    "attributes": {},
                }
            )

        loop = asyncio.get_running_loop()
        message = await loop.run_in_executor(
            None, self._message_serializer.deserialize_message, message
        )

        request_id = message["requestId"]
        result_set = results_dict.get(request_id)
        status_code = message["status"]["code"]
        aggregate_to = message["result"]["meta"].get("aggregateTo", "list")
        data = message["result"]["data"]

        if result_set is not None:
            result_set.aggregate_to = aggregate_to

        if status_code == 407:
            if self._username and self._password:
                auth_bytes = b"".join(
                    [
                        b"\x00",
                        self._username.encode("utf-8"),
                        b"\x00",
                        self._password.encode("utf-8"),
                    ]
                )
                auth = base64.b64encode(auth_bytes)
                request_message = request.RequestMessage(
                    "traversal", "authentication", {"sasl": auth.decode()}
                )
            elif self._kerberized_service:
                request_message = self._kerberos_received(message)
            else:
                error_message = (
                    "Gremlin server requires authentication credentials in "
                    "DriverRemoteConnection. For basic authentication provide "
                    "username and password. For kerberos authentication provide "
                    "the kerberized_service parameter."
                )
                log.error(error_message)
                raise ConfigurationError(error_message)
            await self.write(request_id, request_message)
            raw = await self._transport.read()
            return await self.data_received(raw, results_dict)

        if status_code == 204:
            if result_set is not None:
                result_set.stream.put_nowait([])
                del results_dict[request_id]
            return status_code

        if status_code in (200, 206):
            if result_set is not None:
                result_set.stream.put_nowait(data)
                if status_code == 200:
                    result_set.status_attributes = message["status"]["attributes"]
                    del results_dict[request_id]
            return status_code

        log.error(
            "\r\nReceived error message '%s'\r\n\r\nWith results dictionary '%s'",
            str(message),
            str(results_dict),
        )
        if request_id in results_dict:
            del results_dict[request_id]
        raise GremlinServerError(message["status"])
