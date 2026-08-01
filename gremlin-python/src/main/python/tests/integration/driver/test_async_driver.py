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
"""Integration tests for the native-async Gremlin driver.

Requires a running Gremlin Server (see CLAUDE.md / Docker Compose).
All test classes are skipped automatically when the server is unreachable.

Graphs used:
  gmodern — read-only Modern toy graph (6 vertices, 6 edges)
  gtx     — transactional graph used for AsyncTransaction tests
"""
import asyncio
import os
import socket
import unittest
import urllib.parse

from gremlin_python.driver.async_client import AsyncClient
from gremlin_python.driver.async_driver_remote_connection import AsyncDriverRemoteConnection
from gremlin_python.driver.async_graph_traversal import async_traversal
from gremlin_python.driver.async_resultset import AsyncResultSet
from gremlin_python.process.traversal import Bytecode
from gremlin_python.structure.graph import Vertex

gremlin_server_url = os.environ.get("GREMLIN_SERVER_URL", "ws://localhost:{}/gremlin")
anonymous_url = gremlin_server_url.format(45940)


def _server_reachable(url):
    parsed = urllib.parse.urlparse(url)
    try:
        s = socket.create_connection((parsed.hostname, parsed.port or 80), timeout=2)
        s.close()
        return True
    except OSError:
        return False


_skip_if_no_server = unittest.skipUnless(
    _server_reachable(anonymous_url),
    "Gremlin Server is not running",
)


# ---------------------------------------------------------------------------
# AsyncClient
# ---------------------------------------------------------------------------

@_skip_if_no_server
class TestAsyncClientIntegration(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.client = AsyncClient(anonymous_url, "gmodern")

    async def asyncTearDown(self):
        await self.client.close()

    async def test_submit_string_returns_results(self):
        rs = await self.client.submit("g.V().count()")
        results = await rs.all()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], 6)

    async def test_submit_bytecode_returns_all_vertices(self):
        bc = Bytecode()
        bc.add_step("V")
        rs = await self.client.submit(bc)
        results = await rs.all()
        self.assertEqual(len(results), 6)

    async def test_submit_returns_async_result_set(self):
        rs = await self.client.submit("g.V().count()")
        self.assertIsInstance(rs, AsyncResultSet)

    async def test_submit_async_single_vertex(self):
        rs = await self.client.submit_async("g.V(1)")
        results = await rs.all()
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], Vertex)

    async def test_context_manager(self):
        async with AsyncClient(anonymous_url, "gmodern") as client:
            rs = await client.submit("g.V().count()")
            results = await rs.all()
            self.assertEqual(results[0], 6)

    async def test_concurrent_submits(self):
        rs1, rs2 = await asyncio.gather(
            self.client.submit("g.V().count()"),
            self.client.submit("g.E().count()"),
        )
        vertex_count = (await rs1.all())[0]
        edge_count = (await rs2.all())[0]
        self.assertEqual(vertex_count, 6)
        self.assertEqual(edge_count, 6)

    async def test_is_closed_after_close(self):
        client = AsyncClient(anonymous_url, "gmodern")
        self.assertFalse(client.is_closed())
        await client.close()
        self.assertTrue(client.is_closed())


# ---------------------------------------------------------------------------
# AsyncDriverRemoteConnection — DSL terminal steps
# ---------------------------------------------------------------------------

@_skip_if_no_server
class TestAsyncRemoteConnectionDSL(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.rc = AsyncDriverRemoteConnection(anonymous_url, "gmodern")
        self.g = async_traversal().with_remote(self.rc)

    async def asyncTearDown(self):
        await self.rc.close()

    async def test_to_list_vertex_count(self):
        results = await self.g.V().count().to_list()
        self.assertEqual(results, [6])

    async def test_next_single_value(self):
        count = await self.g.V().count().next()
        self.assertEqual(count, 6)

    async def test_next_amount(self):
        results = await self.g.V().next(3)
        self.assertEqual(len(results), 3)

    async def test_to_set_deduplicates(self):
        labels = await self.g.V().label().to_set()
        self.assertIsInstance(labels, set)
        self.assertIn("person", labels)
        self.assertIn("software", labels)

    async def test_has_next_true(self):
        self.assertTrue(await self.g.V().has_next())

    async def test_has_next_false(self):
        self.assertFalse(await self.g.V().has("name", "__nonexistent__").has_next())

    async def test_iterate_returns_traversal(self):
        result = await self.g.V().iterate()
        self.assertIsNotNone(result)

    async def test_async_for_loop(self):
        names = []
        async for name in self.g.V().has_label("person").values("name"):
            names.append(name)
        self.assertEqual(len(names), 4)
        self.assertIn("marko", names)

    async def test_person_names_correct(self):
        names = await self.g.V().has_label("person").values("name").to_list()
        self.assertEqual(len(names), 4)
        for name in ["marko", "vadas", "josh", "peter"]:
            self.assertIn(name, names)

    async def test_vertex_by_id(self):
        vertex = await self.g.V(1).next()
        self.assertIsInstance(vertex, Vertex)
        self.assertEqual(vertex.id, 1)

    async def test_out_edges(self):
        names = await self.g.V(1).out("knows").values("name").to_list()
        self.assertEqual(len(names), 2)
        self.assertIn("vadas", names)
        self.assertIn("josh", names)


# ---------------------------------------------------------------------------
# AsyncDriverRemoteConnection — submit_async / submit_stream
# ---------------------------------------------------------------------------

@_skip_if_no_server
class TestAsyncRemoteConnectionSubmit(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.rc = AsyncDriverRemoteConnection(anonymous_url, "gmodern")

    async def asyncTearDown(self):
        await self.rc.close()

    async def test_submit_async_returns_list(self):
        bc = Bytecode()
        bc.add_step("V")
        bc.add_step("count")
        results = await self.rc.submit_async(bc)
        self.assertIsInstance(results, list)
        self.assertEqual(results, [6])

    async def test_submit_async_accepts_traversal(self):
        g = async_traversal().with_remote(self.rc)
        t = g.V().has_label("person").values("name")
        results = await self.rc.submit_async(t)
        self.assertEqual(len(results), 4)

    async def test_submit_stream_returns_result_set(self):
        bc = Bytecode()
        bc.add_step("V")
        result = await self.rc.submit_stream(bc)
        self.assertIsInstance(result, AsyncResultSet)

    async def test_submit_stream_async_for(self):
        bc = Bytecode()
        bc.add_step("V")
        collected = []
        async for item in await self.rc.submit_stream(bc):
            collected.append(item)
        self.assertEqual(len(collected), 6)


# ---------------------------------------------------------------------------
# Concurrent queries
# ---------------------------------------------------------------------------

@_skip_if_no_server
class TestAsyncConcurrency(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.rc = AsyncDriverRemoteConnection(anonymous_url, "gmodern")
        self.g = async_traversal().with_remote(self.rc)

    async def asyncTearDown(self):
        await self.rc.close()

    async def test_gather_multiple_traversals(self):
        vertex_count, edge_count, names = await asyncio.gather(
            self.g.V().count().next(),
            self.g.E().count().next(),
            self.g.V().has_label("person").values("name").to_list(),
        )
        self.assertEqual(vertex_count, 6)
        self.assertEqual(edge_count, 6)
        self.assertEqual(len(names), 4)

    async def test_many_concurrent_queries(self):
        results = await asyncio.gather(*[self.g.V().count().next() for _ in range(10)])
        self.assertTrue(all(r == 6 for r in results))


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------

@_skip_if_no_server
class TestAsyncContextManager(unittest.IsolatedAsyncioTestCase):

    async def test_closes_on_normal_exit(self):
        async with AsyncDriverRemoteConnection(anonymous_url, "gmodern") as rc:
            g = async_traversal().with_remote(rc)
            count = await g.V().count().next()
            self.assertEqual(count, 6)
        self.assertTrue(rc.is_closed())

    async def test_closes_on_exception(self):
        rc = AsyncDriverRemoteConnection(anonymous_url, "gmodern")
        with self.assertRaises(ValueError):
            async with rc:
                raise ValueError("intentional")
        self.assertTrue(rc.is_closed())


# ---------------------------------------------------------------------------
# AsyncTransaction
# ---------------------------------------------------------------------------

@_skip_if_no_server
class TestAsyncTransaction(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.rc = AsyncDriverRemoteConnection(anonymous_url, "gtx")
        self.g = async_traversal().with_remote(self.rc)

    async def asyncTearDown(self):
        await self.rc.close()

    async def test_commit_persists_vertex(self):
        name = "async_tx_commit_test"
        async with await self.g.tx() as gtx:
            await gtx.add_v("person").property("name", name).iterate()
        count = await self.g.V().has("name", name).count().next()
        self.assertGreaterEqual(count, 1)
        await self.g.V().has("name", name).drop().iterate()

    async def test_rollback_discards_vertex(self):
        name = "async_tx_rollback_test"
        tx = await self.g.tx()
        gtx = await tx.begin()
        await gtx.add_v("person").property("name", name).iterate()
        await tx.rollback()
        count = await self.g.V().has("name", name).count().next()
        self.assertEqual(count, 0)

    async def test_context_manager_commits_on_success(self):
        name = "async_tx_ctx_commit"
        async with await self.g.tx() as gtx:
            await gtx.add_v("person").property("name", name).iterate()
        count = await self.g.V().has("name", name).count().next()
        self.assertGreaterEqual(count, 1)
        await self.g.V().has("name", name).drop().iterate()

    async def test_context_manager_rolls_back_on_exception(self):
        name = "async_tx_ctx_rollback"
        try:
            async with await self.g.tx() as gtx:
                await gtx.add_v("person").property("name", name).iterate()
                raise ValueError("force rollback")
        except ValueError:
            pass
        count = await self.g.V().has("name", name).count().next()
        self.assertEqual(count, 0)


if __name__ == "__main__":
    unittest.main()
