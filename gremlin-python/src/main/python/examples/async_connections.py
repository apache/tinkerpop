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
import os
import ssl
import sys
import uuid

sys.path.append("..")

from gremlin_python.driver.async_driver_remote_connection import AsyncDriverRemoteConnection
from gremlin_python.driver.async_graph_traversal import async_traversal

SERVER_URL = os.getenv("GREMLIN_SERVER_URL", "ws://localhost:8182/gremlin").format(45940)
VERTEX_LABEL = os.getenv("VERTEX_LABEL", "connection")


async def main():
    await with_context_manager()
    await with_explicit_close()
    await with_auth()
    await with_custom_pool()
    await with_session()


async def with_context_manager():
    # The recommended way: async context manager guarantees close() is called
    # even if an exception is raised.
    async with AsyncDriverRemoteConnection(SERVER_URL, "g") as rc:
        g = async_traversal().with_remote(rc)

        # Terminal steps (to_list, next, iterate) are coroutines — await them.
        await g.add_v(VERTEX_LABEL).iterate()

        count = await g.V().has_label(VERTEX_LABEL).count().next()
        print("with_context_manager — vertex count:", count)


async def with_explicit_close():
    # When a context manager is not practical, close() manually in a finally block.
    rc = AsyncDriverRemoteConnection(SERVER_URL, "g")
    try:
        g = async_traversal().with_remote(rc)
        count = await g.V().count().next()
        print("with_explicit_close — vertex count:", count)
    finally:
        await rc.close()


async def with_auth():
    # Plain-text (SASL/basic) authentication.
    auth_url = os.getenv("GREMLIN_SERVER_BASIC_AUTH_URL",
                         "ws://localhost:8182/gremlin").format(45941)

    ssl_ctx = None
    if ":45941" in auth_url:
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

    async with AsyncDriverRemoteConnection(
        auth_url, "g",
        username="stephen", password="password",
        ssl=ssl_ctx,
    ) as rc:
        g = async_traversal().with_remote(rc)
        count = await g.V().count().next()
        print("with_auth — vertex count:", count)


async def with_custom_pool():
    # pool_size controls concurrent WebSocket connections.
    async with AsyncDriverRemoteConnection(SERVER_URL, "g", pool_size=4) as rc:
        g = async_traversal().with_remote(rc)
        count = await g.V().count().next()
        print("with_custom_pool — vertex count:", count)


async def with_session():
    # Server-side sessions preserve state across multiple requests.
    session_id = str(uuid.uuid4())
    async with AsyncDriverRemoteConnection(SERVER_URL, "g", session=session_id) as rc:
        g = async_traversal().with_remote(rc)

        await g.add_v(VERTEX_LABEL).property("session", session_id).iterate()
        count = await g.V().has("session", session_id).count().next()
        print("with_session — vertex count:", count)


if __name__ == "__main__":
    asyncio.run(main())
