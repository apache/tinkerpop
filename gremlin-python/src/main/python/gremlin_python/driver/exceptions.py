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

class ReadTimeoutError(TimeoutError):
    """Raised when the client-side read timeout elapses while waiting for response
    data from the server.

    Subclasses the builtin :class:`TimeoutError` so it can be caught with
    ``except TimeoutError`` without depending on the underlying transport.

    This driver-owned type exists only because the driver is currently synchronous and
    should not surface the transport's asyncio/aiohttp timeout types. Once gremlin-python
    is fully asynchronous this should be reverted to raising ``asyncio.TimeoutError``
    directly. See TINKERPOP-2774: https://issues.apache.org/jira/browse/TINKERPOP-2774
    """
