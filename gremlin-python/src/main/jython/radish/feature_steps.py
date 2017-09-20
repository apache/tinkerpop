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

from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import Scope
from radish import before, given, when, then

out = __.out


@given("the {graphName:w} graph")
def choose_graph(step, graphName):
    # only have modern atm but graphName would be used to select the right one
    step.context.g = Graph().traversal().withRemote(step.context.remote_conn_modern)


@given("the traversal of")
def translate_traversal(step):
    g = step.context.g
    step.context.traversal = eval(step.text, {"g": g, "Scope": Scope})


@when("iterated to list")
def iterate_the_traversal(step):
    step.context.result = step.context.traversal.toList()


@then("the result should be {characterized_as:w}")
def assert_result(step, characterized_as):
    if characterized_as == "empty":
        assert len(step.context.result) == 0
    elif characterized_as == "ordered":
        data = step.table
    
        # results from traversal should have the same number of entries as the feature data table
        assert len(step.context.result) == len(data)

        # assert the results by type where the first column will hold the type and the second column
        # the data to assert. the contents of the second column will be dependent on the type specified
        # in te first column
        for ix, line in enumerate(data):
            if line[0] == "numeric":
                assert long(step.context.result[ix]) == long(line[1])
            elif line[0] == "string":
                assert str(step.context.result[ix]) == str(line[1])
            else:
                assert step.context.result[ix] == line[1]
    elif characterized_as == "unordered":
        data = step.table

        # results from traversal should have the same number of entries as the feature data table
        assert len(step.context.result) == len(data)



@then("the results should be empty")
def assert_result(step):
    assert len(step.context.result) == 0


