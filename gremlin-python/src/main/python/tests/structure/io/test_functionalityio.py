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

import uuid
import pytest

from datetime import datetime, timedelta, timezone
from gremlin_python.driver.serializer import GraphBinarySerializersV4
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.statics import *


def test_vertex(remote_connection):
    g = traversal().with_(remote_connection)
    vertex = g.V(1).next()
    assert vertex.id == 1
    assert vertex.label == 'person'
    assert len(vertex.properties) == 2
    assert vertex.properties[0].key == 'name'
    assert vertex.properties[0].value == 'marko'
    assert vertex.properties[1].key == 'age'
    assert vertex.properties[1].value == 29


def test_vertex_without_properties(remote_connection):
    g = traversal().with_(remote_connection)
    vertex = g.with_('materializeProperties', 'tokens').V(1).next()
    assert vertex.id == 1
    assert vertex.label == 'person'
    # empty array for GraphBinary and missing field for GraphSON
    assert vertex.properties is None or len(vertex.properties) == 0


def test_edge(remote_connection):
    g = traversal().with_(remote_connection)
    edge = g.E(7).next()
    assert edge.id == 7
    assert edge.label == 'knows'
    assert len(edge.properties) == 1
    assert edge.properties[0].key == 'weight'
    assert edge.properties[0].value == 0.5


def test_edge_without_properties(remote_connection):
    g = traversal().with_(remote_connection)
    edge = g.with_('materializeProperties', 'tokens').E(7).next()
    assert edge.id == 7
    assert edge.label == 'knows'
    # empty array for GraphBinary and missing field for GraphSON
    assert edge.properties is None or len(edge.properties) == 0


def test_vertex_vertex_properties(remote_connection_crew):
    g = traversal().with_(remote_connection_crew)
    vertex = g.V(7).next()
    assert vertex.id == 7
    assert vertex.label == 'person'
    assert len(vertex.properties) == 4
    assert vertex.properties[1].key == 'location'
    assert vertex.properties[1].value == 'centreville'
    assert len(vertex.properties[1].properties) == 2
    assert vertex.properties[1].properties[0].key == 'startTime'
    assert vertex.properties[1].properties[0].value == 1990
    assert vertex.properties[1].properties[1].key == 'endTime'
    assert vertex.properties[1].properties[1].value == 2000


def test_datetime(remote_connection):
    g = traversal().with_(remote_connection)
    tz = timezone(timedelta(seconds=36000))
    ms = 12345678912
    dt = datetime(2022, 5, 20, tzinfo=tz) + timedelta(microseconds=ms)
    resp = g.add_v('test_vertex').property('dt', dt).to_list()
    vid = resp[0].id
    try:
        dt_prop = g.V(vid).properties('dt').to_list()[0]
        assert isinstance(dt_prop.value, datetime)
        assert dt_prop.value == dt
    finally:
        g.V(vid).drop().iterate()


def test_uuid(remote_connection):
    g = traversal().with_(remote_connection)
    uid = uuid.UUID("41d2e28a-20a4-4ab0-b379-d810dede3786")
    resp = g.add_v('test_vertex').property('uuid', uid).to_list()
    vid = resp[0].id
    try:
        uid_prop = g.V(vid).properties('uuid').to_list()[0]
        assert isinstance(uid_prop.value, uuid.UUID)
        assert uid_prop.value == uid
    finally:
        g.V(vid).drop().iterate()


def test_short(remote_connection):
    if not isinstance(remote_connection._client.response_serializer(), GraphBinarySerializersV4):
        return

    g = traversal().with_(remote_connection)
    num = short(1111)
    resp = g.with_('language', 'gremlin-lang').add_v('test_vertex').property('short', num).to_list()
    vid = resp[0].id
    try:
        short_prop = g.with_('language', 'gremlin-lang').V(vid).properties('short').to_list()[0]
        assert isinstance(short_prop.value, int)
        assert short_prop.value == num
    finally:
        g.V(vid).drop().iterate()


def test_bigint_positive(remote_connection):
    if not isinstance(remote_connection._client.response_serializer(), GraphBinarySerializersV4):
        return

    g = traversal().with_(remote_connection)
    big = bigint(0x1000_0000_0000_0000_0000)
    resp = g.with_('language', 'gremlin-lang').add_v('test_vertex').property('bigint', big).to_list()
    vid = resp[0].id
    try:
        bigint_prop = g.with_('language', 'gremlin-lang').V(vid).properties('bigint').to_list()[0]
        assert isinstance(bigint_prop.value, int)
        assert bigint_prop.value == big
    finally:
        g.V(vid).drop().iterate()


def test_bigint_negative(remote_connection):
    if not isinstance(remote_connection._client.response_serializer(), GraphBinarySerializersV4):
        return

    g = traversal().with_(remote_connection)
    big = bigint(-0x1000_0000_0000_0000_0000)
    resp = g.with_('language', 'gremlin-lang').add_v('test_vertex').property('bigint', big).to_list()
    vid = resp[0].id
    try:
        bigint_prop = g.with_('language', 'gremlin-lang').V(vid).properties('bigint').to_list()[0]
        assert isinstance(bigint_prop.value, int)
        assert bigint_prop.value == big
    finally:
        g.V(vid).drop().iterate()


def test_bigdecimal(remote_connection):
    if not isinstance(remote_connection._client.response_serializer(), GraphBinarySerializersV4):
        return

    g = traversal().with_(remote_connection)
    bigdecimal = BigDecimal(10, 235)
    resp = g.with_('language', 'gremlin-lang').add_v('test_vertex').property('bigdecimal', bigdecimal).to_list()
    vid = resp[0].id
    try:
        bigdecimal_prop = g.with_('language', 'gremlin-lang').V(vid).properties('bigdecimal').next()
        assert isinstance(bigdecimal_prop.value, BigDecimal)
        assert bigdecimal_prop.value.scale == bigdecimal.scale
        assert bigdecimal_prop.value.unscaled_value == bigdecimal.unscaled_value
    finally:
        g.V(vid).drop().iterate()


def test_odd_bits(remote_connection):
    g = traversal().with_(remote_connection)
    char_lower = str.__new__(SingleChar, chr(78))
    resp = g.add_v('test_vertex').property('char_lower', char_lower).to_list()
    vid = resp[0].id
    try:
        v = g.V(vid).values('char_lower').to_list()[0]
        assert v == char_lower
    finally:
        g.V(vid).drop().iterate()

    char_upper = str.__new__(SingleChar, chr(57344))
    resp = g.add_v('test_vertex').property('char_upper', char_upper).to_list()
    vid = resp[0].id
    try:
        v = g.V(vid).values('char_upper').to_list()[0]
        assert v == char_upper
    finally:
        g.V(vid).drop().iterate()

    dur = timedelta(seconds=1000, microseconds=1000)
    resp = g.add_v('test_vertex').property('dur', dur).to_list()
    vid = resp[0].id
    try:
        v = g.V(vid).values('dur').to_list()[0]
        assert v == dur
    finally:
        g.V(vid).drop().iterate()
