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

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'

from gremlin_python.structure.graph import Graph
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.traversal import P
from gremlin_python.process.traversal import Binding, Bindings
from gremlin_python.process.graph_traversal import __


class TestTraversal(object):
    def test_bytecode(self):
        g = traversal().withGraph(Graph())
        bytecode = g.V().out("created").bytecode
        assert 0 == len(bytecode.bindings.keys())
        assert 0 == len(bytecode.source_instructions)
        assert 2 == len(bytecode.step_instructions)
        assert "V" == bytecode.step_instructions[0][0]
        assert "out" == bytecode.step_instructions[1][0]
        assert "created" == bytecode.step_instructions[1][1]
        assert 1 == len(bytecode.step_instructions[0])
        assert 2 == len(bytecode.step_instructions[1])
        ##
        bytecode = g.withSack(1).E().groupCount().by("weight").bytecode
        assert 0 == len(bytecode.bindings.keys())
        assert 1 == len(bytecode.source_instructions)
        assert "withSack" == bytecode.source_instructions[0][0]
        assert 1 == bytecode.source_instructions[0][1]
        assert 3 == len(bytecode.step_instructions)
        assert "E" == bytecode.step_instructions[0][0]
        assert "groupCount" == bytecode.step_instructions[1][0]
        assert "by" == bytecode.step_instructions[2][0]
        assert "weight" == bytecode.step_instructions[2][1]
        assert 1 == len(bytecode.step_instructions[0])
        assert 1 == len(bytecode.step_instructions[1])
        assert 2 == len(bytecode.step_instructions[2])
        ##
        bytecode = g.V(Bindings.of('a', [1,2,3])) \
            .out(Bindings.of('b','created')) \
            .where(__.in_(Bindings.of('c','created'), Bindings.of('d','knows')) \
            .count().is_(Bindings.of('e',P.gt(2)))).bytecode
        assert 5 == len(bytecode.bindings.keys())
        assert [1,2,3] == bytecode.bindings['a']
        assert 'created' == bytecode.bindings['b']
        assert 'created' == bytecode.bindings['c']
        assert 'knows' == bytecode.bindings['d']
        assert P.gt(2) == bytecode.bindings['e']
        assert Binding('b','created') == bytecode.step_instructions[1][1]
        assert 'binding[b=created]' == str(bytecode.step_instructions[1][1])
        assert isinstance(hash(bytecode.step_instructions[1][1]),int)

    def test_P(self):
        # verify that the order of operations is respected
        assert "and(eq(a),lt(b))" == str(P.eq("a").and_(P.lt("b")))
        assert "and(or(lt(b),gt(c)),neq(d))" == str(P.lt("b").or_(P.gt("c")).and_(P.neq("d")))
        assert "and(or(lt(b),gt(c)),or(neq(d),gte(e)))" == str(
            P.lt("b").or_(P.gt("c")).and_(P.neq("d").or_(P.gte("e"))))

    def test_anonymous_traversal(self):
        bytecode = __.__(1).bytecode
        assert 0 == len(bytecode.bindings.keys())
        assert 0 == len(bytecode.source_instructions)
        assert 1 == len(bytecode.step_instructions)
        assert "inject" == bytecode.step_instructions[0][0]
        assert 1 == bytecode.step_instructions[0][1]
        ##
        bytecode = __.start().bytecode
        assert 0 == len(bytecode.bindings.keys())
        assert 0 == len(bytecode.source_instructions)
        assert 0 == len(bytecode.step_instructions)

    def test_clone_traversal(self):
        g = traversal().withGraph(Graph())
        original = g.V().out("created")
        clone = original.clone().out("knows")
        cloneClone = clone.clone().out("created")

        assert 2 == len(original.bytecode.step_instructions)
        assert 3 == len(clone.bytecode.step_instructions)
        assert 4 == len(cloneClone.bytecode.step_instructions)

        original.has("person", "name", "marko")
        clone.V().out()

        assert 3 == len(original.bytecode.step_instructions)
        assert 5 == len(clone.bytecode.step_instructions)
        assert 4 == len(cloneClone.bytecode.step_instructions)


