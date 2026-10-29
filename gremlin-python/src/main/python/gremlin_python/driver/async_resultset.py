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

from gremlin_python.driver.resultset import _EXHAUSTED

# Internal sentinel pushed by AsyncConnection._receive when the stream ends.
_STREAM_END = object()


class AsyncResultSet:
    """Async counterpart to :class:`~gremlin_python.driver.resultset.ResultSet`
    for TinkerPop 4.0 HTTP streaming responses.

    Backed by an :class:`asyncio.Queue` where each entry is a single
    deserialized item (matching 4.0's ``ResultSet`` semantics). All consumption
    methods are native coroutines or async iterators and wake up immediately
    without polling loops.
    """

    def __init__(self, stream: asyncio.Queue, request_id=None):
        self._stream = stream
        self._request_id = request_id
        self._status_attributes = None
        self._done = None
        self._aggregate_to = None

    @property
    def aggregate_to(self):
        return self._aggregate_to

    @aggregate_to.setter
    def aggregate_to(self, val):
        self._aggregate_to = val

    @property
    def stream(self) -> asyncio.Queue:
        return self._stream

    @property
    def request_id(self):
        return self._request_id

    @property
    def done(self):
        return self._done

    @done.setter
    def done(self, future):
        self._done = future

    # Alias for compatibility with code referencing result_set.future
    @property
    def future(self):
        return self._done

    @future.setter
    def future(self, future):
        self._done = future

    @property
    def status_attributes(self):
        return self._status_attributes

    @status_attributes.setter
    def status_attributes(self, val):
        if self._status_attributes is None:
            self._status_attributes = val
        else:
            raise RuntimeError("Notification attributes cannot be modified")

    def __iter__(self):
        raise NotImplementedError(
            "AsyncResultSet does not support synchronous iteration. "
            "Use 'async for item in result_set' or 'await result_set.all()'."
        )

    def __next__(self):
        raise NotImplementedError(
            "AsyncResultSet does not support synchronous iteration. "
            "Use 'await result_set.__anext__()' or 'await result_set.one()'."
        )

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self.one()
        if item is _EXHAUSTED:
            raise StopAsyncIteration
        return item

    async def _await_done(self):
        if self._done is None:
            return
        if asyncio.isfuture(self._done) or asyncio.iscoroutine(self._done):
            await self._done
        else:
            self._done.result()

    async def one(self):
        """Return the next item from the result stream, or ``_EXHAUSTED`` once
        the stream is exhausted (matching 4.0's ``ResultSet.one()`` so literal
        ``None`` results are preserved)."""
        while True:
            if not self._stream.empty():
                item = self._stream.get_nowait()
                if item is _STREAM_END:
                    await self._await_done()
                    return _EXHAUSTED
                return item

            if self._done is None or self._done.done():
                await self._await_done()
                if not self._stream.empty():
                    item = self._stream.get_nowait()
                    if item is _STREAM_END:
                        return _EXHAUSTED
                    return item
                return _EXHAUSTED

            get_task = asyncio.ensure_future(self._stream.get())
            done_set, _ = await asyncio.wait(
                {get_task, self._done},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if get_task in done_set:
                item = get_task.result()
                if item is _STREAM_END:
                    await self._await_done()
                    return _EXHAUSTED
                return item
            else:
                get_task.cancel()
                try:
                    await get_task
                except asyncio.CancelledError:
                    pass
                await self._await_done()
                if not self._stream.empty():
                    item = self._stream.get_nowait()
                    if item is _STREAM_END:
                        return _EXHAUSTED
                    return item
                return _EXHAUSTED

    async def all(self):
        """Drain the stream and return a flat list of all deserialized items."""
        return [item async for item in self]
