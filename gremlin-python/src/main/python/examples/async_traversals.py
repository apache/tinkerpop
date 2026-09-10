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

# This example requires the Modern toy graph to be preloaded when Gremlin Server
# starts.  See https://tinkerpop.apache.org/docs/current/reference/#gremlin-server-docker-image
# and use conf/gremlin-server-modern.yaml.

import asyncio
import os
import sys

sys.path.append("..")

from gremlin_python.driver.async_driver_remote_connection import AsyncDriverRemoteConnection
from gremlin_python.driver.async_graph_traversal import async_traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import P, T

SERVER_URL = os.getenv("GREMLIN_SERVER_URL", "ws://localhost:8182/gremlin").format(45940)

# CI uses port 45940 with gmodern binding; local uses 8182 with g binding.
GRAPH_BINDING = "gmodern" if ":45940" in SERVER_URL else "g"


async def main():
    async with AsyncDriverRemoteConnection(SERVER_URL, GRAPH_BINDING) as rc:
        g = async_traversal().with_remote(rc)

        await basic_queries(g)
        await write_operations(g)
        await concurrent_queries(g)
        await traversal_steps(g)


async def basic_queries(g):
    # to_list() collects all results into a list — identical name to the sync
    # version, but here it is a coroutine that must be awaited.
    names = await g.V().values("name").to_list()
    print("basic_queries — all names:", names)

    # next() returns the first result (raises StopAsyncIteration if empty).
    count = await g.V().count().next()
    print("basic_queries — vertex count:", count)

    # to_set() deduplicates results.
    labels = await g.V().label().to_set()
    print("basic_queries — unique labels:", labels)

    # has_next() returns True/False without consuming the traversal.
    exists = await g.V().has("name", "marko").has_next()
    print("basic_queries — marko exists:", exists)

    # Filtering and projection with snake_case step names.
    person_maps = await g.V().has_label("person").value_map("name", "age").to_list()
    print("basic_queries — person maps:", person_maps)


async def write_operations(g):
    # iterate() discards results and is the right choice for writes.
    # It adds a "discard" step so the server knows no data need be returned.
    await g.add_v("planet").property("name", "saturn").iterate()

    saturn = await g.V().has("name", "saturn").next()
    print("write_operations — created:", saturn)

    # Clean up.
    await g.V().has("name", "saturn").drop().iterate()


async def concurrent_queries(g):
    # asyncio.gather() runs multiple traversals concurrently over the shared
    # connection pool — no extra connections are opened.
    names_coro = g.V().has_label("person").values("name").to_list()
    edges_coro = g.E().label().to_set()
    software_coro = g.V().has_label("software").values("name").to_list()

    names, edges, software = await asyncio.gather(names_coro, edges_coro, software_coro)
    print("concurrent_queries — people:   ", names)
    print("concurrent_queries — edge types:", edges)
    print("concurrent_queries — software:  ", software)


async def traversal_steps(g):
    # The full Gremlin DSL is available — all intermediate steps work
    # exactly as in the sync version.
    e1 = await g.V(1).both_e().to_list()
    print("traversal_steps (1) — all edges of v1:", e1)

    e2 = await g.V(1).both_e().where(__.other_v().has_id(2)).to_list()
    print("traversal_steps (2) — edges between v1 and v2:", e2)

    e3 = await g.V(1).out_e().where(__.in_v().has(T.id, P.within(2, 3))).to_list()
    print("traversal_steps (3) — out-edges of v1 to v2 or v3:", e3)

    # next(n) returns the first n results as a list.
    first_two = await g.V().has_label("person").values("name").next(2)
    print("traversal_steps — first two person names:", first_two)


if __name__ == "__main__":
    asyncio.run(main())
