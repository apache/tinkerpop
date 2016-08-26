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

import unittest
from unittest import TestCase

from gremlin_python.structure.graph import Graph


class TestTraversal(TestCase):
    def test_bytecode(self):
        g = Graph().traversal()
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


if __name__ == '__main__':
    unittest.main()
