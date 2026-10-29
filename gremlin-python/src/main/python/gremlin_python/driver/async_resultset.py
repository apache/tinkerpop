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

# Internal sentinel used to signal that the stream is exhausted.
_EXHAUSTED = object()


class AsyncResultSet:
    """Result set backed by an asyncio.Queue and an asyncio.Future.

    Consumption methods (one, all) are coroutines.  The class also supports
    async for iteration.

    The future attribute must be set to a resolved asyncio.Future or a
    running asyncio.Task by the owning AsyncConnection before any
    consumption method is called.
    """

    def __init__(self, stream, request_id):
        # stream must be an asyncio.Queue
        self._stream = stream
        self._request_id = request_id
        self._aggregate_to = None
        self._status_attributes = {}
        self._future = None
        # Cache individual items already unpacked from a multi-item chunk so
        # that successive one() calls each return exactly one result.
        self._item_cache = queue.Queue()

    @property
    def aggregate_to(self):
        return self._aggregate_to

    @aggregate_to.setter
    def aggregate_to(self, val):
        self._aggregate_to = val

    @property
    def status_attributes(self):
        return self._status_attributes

    @status_attributes.setter
    def status_attributes(self, val):
        self._status_attributes = val

    @property
    def request_id(self):
        return self._request_id

    @property
    def stream(self):
        return self._stream

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
        """Return the next individual result or the _EXHAUSTED sentinel.

        Unlike the synchronous ResultSet.one() which returns a full batch
        list per server frame, AsyncResultSet.one() unpacks each server frame
        so that each call (and each iteration of ``async for``) yields a
        single item, caching any remaining items from the frame for
        subsequent calls.
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
        """Return the next raw chunk (list) from the stream or _EXHAUSTED.

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
        while not self._item_cache.empty():
            results.append(self._item_cache.get_nowait())
        while True:
            chunk = await self._one_chunk()
            if chunk is _EXHAUSTED:
                break
            if isinstance(chunk, list):
                results.extend(chunk)
        return results
