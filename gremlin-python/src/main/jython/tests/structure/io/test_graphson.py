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

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'

import json
import unittest
from unittest import TestCase

import six

from gremlin_python.statics import long
from gremlin_python.structure.graph import Vertex
from gremlin_python.structure.graph import Path
from gremlin_python.structure.io.graphson import GraphSONReader
from gremlin_python.structure.io.graphson import GraphSONWriter
from gremlin_python.process.traversal import P


class TestGraphSONReader(TestCase):
    def test_numbers(self):
        x = GraphSONReader.readObject(json.dumps({
            "@type": "g:Int32",
            "@value": 31
        }))
        assert isinstance(x, int)
        assert 31 == x
        ##
        x = GraphSONReader.readObject(json.dumps({
            "@type": "g:Int64",
            "@value": 31
        }))
        assert isinstance(x, long)
        assert long(31) == x
        ##
        x = GraphSONReader.readObject(json.dumps({
            "@type": "g:Float",
            "@value": 31.3
        }))
        assert isinstance(x, float)
        assert 31.3 == x
        ##
        x = GraphSONReader.readObject(json.dumps({
            "@type": "g:Double",
            "@value": 31.2
        }))
        assert isinstance(x, float)
        assert 31.2 == x

    def test_graph(self):
        vertex = GraphSONReader.readObject(
            """{"@type":"g:Vertex", "@value":{"id":{"@type":"g:Int32","@value":1},"label":"person","outE":{"created":[{"id":{"@type":"g:Int32","@value":9},"inV":{"@type":"g:Int32","@value":3},"properties":{"weight":{"@type":"g:Double","@value":0.4}}}],"knows":[{"id":{"@type":"g:Int32","@value":7},"inV":{"@type":"g:Int32","@value":2},"properties":{"weight":{"@type":"g:Double","@value":0.5}}},{"id":{"@type":"g:Int32","@value":8},"inV":{"@type":"g:Int32","@value":4},"properties":{"weight":{"@type":"g:Double","@value":1.0}}}]},"properties":{"name":[{"id":{"@type":"g:Int64","@value":0},"value":"marko"}],"age":[{"id":{"@type":"g:Int64","@value":1},"value":{"@type":"g:Int32","@value":29}}]}}}""")
        assert isinstance(vertex, Vertex)
        assert "person" == vertex.label
        assert 1 == vertex.id
        assert isinstance(vertex.id, int)
        assert vertex == Vertex(1)

    def test_path(self):
        path = GraphSONReader.readObject(
            """{"@type":"g:Path","@value":{"labels":[["a"],["b","c"],[]],"objects":[{"@type":"g:Vertex","@value":{"id":{"@type":"g:Int32","@value":1},"label":"person","properties":{"name":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":0},"value":"marko","label":"name"}}],"age":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":1},"value":{"@type":"g:Int32","@value":29},"label":"age"}}]}}},{"@type":"g:Vertex","@value":{"id":{"@type":"g:Int32","@value":3},"label":"software","properties":{"name":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":4},"value":"lop","label":"name"}}],"lang":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":5},"value":"java","label":"lang"}}]}}},"lop"]}}"""
        )
        assert isinstance(path, Path)
        if six.PY3:
            assert "[v[1], v[3], 'lop']" == str(path)
        else:
            assert "[v[1], v[3], u'lop']" == str(path)
        assert Vertex(1) == path[0]
        assert Vertex(1) == path["a"]
        assert "lop" == path[2]
        assert 3 == len(path)


class TestGraphSONWriter(TestCase):
    def test_numbers(self):
        assert {"@type":"g:Int64","@value":2} == json.loads(GraphSONWriter.writeObject(long(2)))
        assert {"@type":"g:Int32","@value":1} == json.loads(GraphSONWriter.writeObject(1))
        assert {"@type":"g:Float","@value":3.2} == json.loads(GraphSONWriter.writeObject(3.2))
        assert """true""" == GraphSONWriter.writeObject(True)

    def test_P(self):
        assert """{"@type":"g:P","@value":{"predicate":"and","value":[{"@type":"g:P","@value":{"predicate":"or","value":[{"@type":"g:P","@value":{"predicate":"lt","value":"b"}},{"@type":"g:P","@value":{"predicate":"gt","value":"c"}}]}},{"@type":"g:P","@value":{"predicate":"neq","value":"d"}}]}}""" == GraphSONWriter.writeObject(
            P.lt("b").or_(P.gt("c")).and_(P.neq("d")))


if __name__ == '__main__':
    unittest.main()
