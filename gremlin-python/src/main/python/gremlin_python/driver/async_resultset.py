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
"""Experimental API. This API may have breaking changes or be removed in a subsequent minor release.

Asynchronous result set backed by an asyncio.Queue and an asyncio.Future.
"""
import asyncio
import queue

# Internal sentinel used to signal that the stream is exhausted.
_EXHAUSTED = object()
# Internal sentinel enqueued when a pending background receive task finishes.
_STREAM_END = object()


class AsyncResultSet:
    """Experimental API. This API may have breaking changes or be removed in a subsequent minor release.

    Result set backed by an asyncio.Queue and an asyncio.Future.

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
        """Experimental API. This API may have breaking changes or be removed in a subsequent minor release."""
        return self._aggregate_to

    @aggregate_to.setter
    def aggregate_to(self, val):
        self._aggregate_to = val

    @property
    def status_attributes(self):
        """Experimental API. This API may have breaking changes or be removed in a subsequent minor release."""
        return self._status_attributes

    @status_attributes.setter
    def status_attributes(self, val):
        self._status_attributes = val

    @property
    def request_id(self):
        """Experimental API. This API may have breaking changes or be removed in a subsequent minor release."""
        return self._request_id

    @property
    def stream(self):
        """Experimental API. This API may have breaking changes or be removed in a subsequent minor release."""
        return self._stream

    # ------------------------------------------------------------------
    # future property (asyncio.Future / asyncio.Task)
    # ------------------------------------------------------------------

    @property
    def future(self):
        """Experimental API. This API may have breaking changes or be removed in a subsequent minor release."""
        return self._future

    @future.setter
    def future(self, value):
        self._future = value
        if value is not None and not value.done():
            value.add_done_callback(lambda _: self._stream.put_nowait(_STREAM_END))

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
        """Experimental API. This API may have breaking changes or be removed in a subsequent minor release.

        Return the next individual result or the _EXHAUSTED sentinel.

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

        Awaits the stream queue directly instead of polling the receive task.
        When a pending receive task completes (normally, with an exception, or
        via cancellation), its done callback enqueues _STREAM_END so any
        waiting consumer wakes up immediately.
        """
        while not self.stream.empty():
            chunk = self.stream.get_nowait()
            if chunk is not _STREAM_END:
                return chunk

        if self._future.done():
            self._future.result()
            return _EXHAUSTED

        chunk = await self.stream.get()
        if chunk is _STREAM_END:
            self.stream.put_nowait(_STREAM_END)
            self._future.result()
            return _EXHAUSTED
        return chunk

    async def all(self):
        """Experimental API. This API may have breaking changes or be removed in a subsequent minor release.

        Collect every result and return them as a flat list.
        """
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
