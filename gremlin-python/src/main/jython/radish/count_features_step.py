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
    if step.text == "g.V().count()":
        step.context.traversal = g.V().count()
    elif step.text == "g.V().both().both().count()":
        step.context.traversal = g.V().both().both().count()
    elif step.text == "g.V().fold().count(Scope.local)":
        step.context.traversal = g.V().fold().count(Scope.local)
    elif step.text == "g.V().has(\"no\").count()":
        step.context.traversal = g.V().has("no").count()
    else:
        raise ValueError("Gremlin translation to python not found - missing: " + step.text)


@when("iterating")
def iterate_the_traversal(step):
    step.context.result = step.context.traversal.toList()


@then("the result should be {number:d}")
def assert_single_result_of_number(step, number):
    assert len(step.context.result) == 1
    assert step.context.result[0] == number


