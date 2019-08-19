'''
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
'''

import datetime
import uuid

from gremlin_python.driver.serializer import GraphSONSerializersV2d0
from gremlin_python.structure.graph import Graph
from gremlin_python.statics import *


def test_timestamp(remote_connection):
    g = Graph().traversal().withRemote(remote_connection)
    ts = timestamp(1481750076295 / 1000)
    resp = g.addV('test_vertex').property('ts', ts)
    resp = resp.toList()
    vid = resp[0].id
    try:
        ts_prop = g.V(vid).properties('ts').toList()[0]
        assert isinstance(ts_prop.value, timestamp)
        assert ts_prop.value == ts
    finally:
        g.V(vid).drop().iterate()


def test_datetime(remote_connection):
    g = Graph().traversal().withRemote(remote_connection)
    dt = datetime.datetime.utcfromtimestamp(1481750076295 / 1000)
    resp = g.addV('test_vertex').property('dt', dt).toList()
    vid = resp[0].id
    try:
        dt_prop = g.V(vid).properties('dt').toList()[0]
        assert isinstance(dt_prop.value, datetime.datetime)
        assert dt_prop.value == dt
    finally:
        g.V(vid).drop().iterate()


def test_uuid(remote_connection):
    g = Graph().traversal().withRemote(remote_connection)
    uid = uuid.UUID("41d2e28a-20a4-4ab0-b379-d810dede3786")
    resp = g.addV('test_vertex').property('uuid', uid).toList()
    vid = resp[0].id
    try:
        uid_prop = g.V(vid).properties('uuid').toList()[0]
        assert isinstance(uid_prop.value, uuid.UUID)
        assert uid_prop.value == uid
    finally:
        g.V(vid).drop().iterate()


def test_odd_bits(remote_connection):
    if not isinstance(remote_connection._client._message_serializer, GraphSONSerializersV2d0):
        g = Graph().traversal().withRemote(remote_connection)
        char_lower = str.__new__(SingleChar, chr(78))
        resp = g.addV('test_vertex').property('char_lower', char_lower).toList()
        vid = resp[0].id
        try:
            v = g.V(vid).values('char_lower').toList()[0]
            assert v == char_lower
        finally:
            g.V(vid).drop().iterate()

        if six.PY3:
            char_upper = str.__new__(SingleChar, chr(57344))
            resp = g.addV('test_vertex').property('char_upper', char_upper).toList()
            vid = resp[0].id
            try:
                v = g.V(vid).values('char_upper').toList()[0]
                assert v == char_upper
            finally:
                g.V(vid).drop().iterate()
