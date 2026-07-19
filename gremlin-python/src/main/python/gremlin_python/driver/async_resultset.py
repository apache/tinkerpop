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
import queue

from gremlin_python.driver.resultset import ResultSet

__author__ = 'Apache TinkerPop (dev@tinkerpop.apache.org)'

# Internal sentinel used to signal that the stream is exhausted.
_EXHAUSTED = object()


class AsyncResultSet(ResultSet):
    """Result set backed by an :class:`asyncio.Queue` and an
    :class:`asyncio.Future`.

    Consumption methods (:meth:`one`, :meth:`all`) are coroutines.  The class
    also supports ``async for`` iteration.

    The ``future`` attribute **must** be set to a resolved
    :class:`asyncio.Future` or a running :class:`asyncio.Task` by the owning
    :class:`~gremlin_python.driver.async_connection.AsyncConnection` before
    any consumption method is called.
    """

    def __init__(self, stream, request_id):
        # stream must be an asyncio.Queue
        super().__init__(stream, request_id)
        self._future = None
        # Cache individual items already unpacked from a multi-item chunk so
        # that successive one() calls each return exactly one result.
        self._item_cache = queue.Queue()

    # ------------------------------------------------------------------
    # future property (asyncio.Future / asyncio.Task)
    # ------------------------------------------------------------------

    @property
    def future(self):
        return self._future

    @future.setter
    def future(self, value):
        self._future = value

    # ------------------------------------------------------------------
    # Disable the synchronous interface
    # ------------------------------------------------------------------

    def __iter__(self):
        raise NotImplementedError(
            "AsyncResultSet supports only async iteration; use 'async for'"
        )

    def __next__(self):
        raise NotImplementedError(
            "AsyncResultSet supports only async iteration; use 'async for'"
        )

    # ------------------------------------------------------------------
    # Async iteration protocol
    # ------------------------------------------------------------------

    def __aiter__(self):
        return self

    async def __anext__(self):
        result = await self.one()
        if result is _EXHAUSTED:
            raise StopAsyncIteration
        return result

    # ------------------------------------------------------------------
    # Consumption
    # ------------------------------------------------------------------

    async def one(self):
        """Return the next individual result or the ``_EXHAUSTED`` sentinel.

        Each server response chunk contains a list of items.  ``one()``
        unpacks those lists so that each call returns exactly one item,
        caching any extras for subsequent calls.
        """
        if not self._item_cache.empty():
            return self._item_cache.get_nowait()

        chunk = await self._one_chunk()
        if chunk is _EXHAUSTED:
            return _EXHAUSTED

        if isinstance(chunk, list):
            if not chunk:
                # Empty list from a 204 No Content response.
                return _EXHAUSTED
            for item in chunk[1:]:
                self._item_cache.put_nowait(item)
            return chunk[0]

        return chunk

    async def _one_chunk(self):
        """Return the next raw chunk (list) from the stream or ``_EXHAUSTED``.

        Polls the stream queue while the background receive task is still
        running, yielding control for up to 100 ms between checks so that
        other coroutines can make progress.
        """
        while not self._future.done():
            if not self.stream.empty():
                return self.stream.get_nowait()
            # Brief yield so other coroutines can run while we wait.
            await asyncio.wait([self._future], timeout=0.1)

        # Future is done; drain whatever landed in the queue.
        if not self.stream.empty():
            return await self.stream.get()

        # Re-raise if the background task failed; return sentinel on success.
        self._future.result()
        return _EXHAUSTED

    async def all(self):
        """Collect every result and return them as a flat list."""
        results = []
        while True:
            chunk = await self._one_chunk()
            if chunk is _EXHAUSTED:
                break
            if isinstance(chunk, list):
                results.extend(chunk)
        return results
