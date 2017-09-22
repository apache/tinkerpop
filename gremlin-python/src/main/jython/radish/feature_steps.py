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

import json
from gremlin_python.structure.graph import Graph, Vertex, Edge
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import P, Scope
from radish import given, when, then, custom_type, register_custom_type, TypeBuilder
from hamcrest import *

out = __.out


@custom_type('words', r'\w+')
def parse_word(text):
    return str(text)


register_custom_type(WordList=TypeBuilder.with_many(parse_word, listsep=','))


@given("the {graph_name:w} graph")
def choose_graph(step, graph_name):
    # only have modern atm but graphName would be used to select the right one
    step.context.g = Graph().traversal().withRemote(step.context.remote_conn[graph_name])


@given("the traversal of")
def translate_traversal(step):
    g = step.context.g
    step.context.traversal = eval(step.text, {"g": g,
                                              "P": P,
                                              "Scope": Scope})


@when("iterated to list")
def iterate_the_traversal(step):
    step.context.result = step.context.traversal.toList()


@then("the result should be {characterized_as:w}")
def assert_result(step, characterized_as):
    if characterized_as == "empty":
        assert_that(len(step.context.result), equal_to(0))
    elif characterized_as == "ordered":
        data = step.table
    
        # results from traversal should have the same number of entries as the feature data table
        assert_that(len(step.context.result), equal_to(len(data)))

        # assert the results by type where the first column will hold the type and the second column
        # the data to assert. the contents of the second column will be dependent on the type specified
        # in the first column
        for ix, line in enumerate(data):
            if line[0] == "numeric":
                assert_that(long(step.context.result[ix]), equal_to(long(line[1])))
            elif line[0] == "string":
                assert_that(str(step.context.result[ix]), equal_to(str(line[1])))
            elif line[0] == "vertex":
                assert_that(step.context.result[ix].label, equal_to(line[1]))
            elif line[0] == "map":
                assert_that(step.context.result[ix], json.loads(line[1]))
            else:
                raise ValueError("unknown type of " + line[0])
    elif characterized_as == "unordered":
        data = step.table

        # results from traversal should have the same number of entries as the feature data table
        assert_that(len(step.context.result), equal_to(len(data)))

        results_to_test = list(step.context.result)

        for line in data:
            if line[0] == "numeric":
                val = long(line[1])
                assert_that(val, is_in(list(map(long, results_to_test))))
                results_to_test.remove(val)
            elif line[0] == "string":
                val = str(line[1])
                assert_that(val, is_in(list(map(str, results_to_test))))
                results_to_test.remove(val)
            elif line[0] == "vertex":
                val = str(line[1])
                v = step.context.lookup["modern"][val]
                assert_that(v, is_in(results_to_test))
                results_to_test.remove(v)
            elif line[0] == "map":
                val = json.load(line[1])
                assert_that(val, is_in(results_to_test))
                results_to_test.remove(val)
            else:
                raise ValueError("unknown type of " + line[0])

        assert_that(len(results_to_test), is_(0))
    else:
        raise ValueError("unknown data characterization of " + characterized_as)


@then("the number of results should be {number:d}")
def assert_number_of_results(step, number):
    assert_that(len(step.context.result), equal_to(number))


@then("the results should all be {element_type:w}")
def assert_elements(step, element_type):
    if element_type == "vertices":
        t = Vertex
    elif element_type == "edges":
        t = Edge
    else:
        raise ValueError("unknown element type of " + element_type)

    for r in step.context.result:
        assert_that(r, instance_of(t))
