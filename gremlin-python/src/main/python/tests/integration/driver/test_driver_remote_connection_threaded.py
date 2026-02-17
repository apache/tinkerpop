#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import concurrent.futures
import os
import sys
import queue
from threading import Thread

from gremlin_python.driver.driver_remote_connection import (
    DriverRemoteConnection)
from gremlin_python.process.anonymous_traversal import traversal

__author__ = 'David M. Brown (davebshow@gmail.com)'

gremlin_server_url = os.environ.get('GREMLIN_SERVER_URL', 'http://localhost:{}/gremlin')
test_no_auth_url = gremlin_server_url.format(45940)


def test_conns_in_threads(remote_connection):
    q = queue.Queue()
    child = Thread(target=_executor, args=(q, None))
    child2 = Thread(target=_executor, args=(q, None))
    child.start()
    child2.start()
    for x in range(2):
        success = q.get()
        assert success == 'success!'
    child.join()
    child2.join()


def test_conn_in_threads(remote_connection):
    q = queue.Queue()
    child = Thread(target=_executor, args=(q, remote_connection))
    child2 = Thread(target=_executor, args=(q, remote_connection))
    child.start()
    child2.start()
    for x in range(2):
        success = q.get()
        assert success == 'success!'
    child.join()
    child2.join()


def _executor(q, conn):
    close = False
    if not conn:
        # This isn't a fixture so close manually
        close = True
        conn = DriverRemoteConnection(test_no_auth_url, 'gmodern', pool_size=4)
    try:
        g = traversal().with_(conn)
        future = g.V().promise()
        t = future.result()
        assert len(t.to_list()) == 6
    except:
        q.put(sys.exc_info()[0])
    else:
        q.put('success!')
        # Close conn
        if close:
            conn.close()


def handle_request():
    try:
        remote_connection = DriverRemoteConnection(test_no_auth_url, "gmodern")
        g = traversal().with_(remote_connection)
        g.V().limit(1).to_list()
        remote_connection.close()
        return True
    except RuntimeError:
        return False


def test_multithread(client):
    try:
        for i in range(10):
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(handle_request)
                assert future.result()
    except RuntimeError:
        assert False
