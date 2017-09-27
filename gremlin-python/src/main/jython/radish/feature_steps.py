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
import re
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import P, Scope, Column
from radish import given, when, then
from hamcrest import *

regex_as = re.compile(r"\.as\(")
regex_in = re.compile(r"\.in\(")


@given("the {graph_name:w} graph")
def choose_graph(step, graph_name):
    # only have modern atm but graphName would be used to select the right one
    step.context.g = Graph().traversal().withRemote(step.context.remote_conn[graph_name])


@given("using the parameter {param_name:w} is {param:QuotedString}")
def add_parameter(step, param_name, param):
    if not hasattr(step.context, "traversal_params"):
        step.context.traversal_params = {}

    step.context.traversal_params[param_name.encode('utf-8')] = __convert(param, step.context)


@given("the traversal of")
def translate_traversal(step):
    g = step.context.g
    b = {"g": g,
         "Column": Column,
         "P": P,
         "Scope": Scope,
         "bothE": __.bothE}

    if hasattr(step.context, "traversal_params"):
        b.update(step.context.traversal_params)

    step.context.traversal = eval(__translate(step.text), b)


@when("iterated to list")
def iterate_the_traversal(step):
    step.context.result = step.context.traversal.toList()


@then("the result should be {characterized_as:w}")
def assert_result(step, characterized_as):
    if characterized_as == "empty":
        assert_that(len(step.context.result), equal_to(0))
    elif characterized_as == "ordered":
        __ordered_assertion(step)
    elif characterized_as == "unordered":
        __unordered_assertion(step)
    else:
        raise ValueError("unknown data characterization of " + characterized_as)


def __convert(val, ctx):
    if isinstance(val, dict):                                                    # convert dictionary keys/values
        n = {}
        for key, value in val.items():
            n[__convert(key, ctx)] = __convert(value, ctx)
        return n
    elif isinstance(val, (str, unicode)) and re.match("^l\[.*\]$", val):         # parse list
        return list(map((lambda x: __convert(x, ctx)), val[2:-1].split(",")))
    elif isinstance(val, (str, unicode)) and re.match("^d\[.*\]$", val):         # parse numeric
        return long(val[2:-1])
    elif isinstance(val, (str, unicode)) and re.match("^v\[.*\]\.id$", val):     # parse vertex id
        return ctx.lookup_v["modern"][val[2:-4]].id
    elif isinstance(val, (str, unicode)) and re.match("^v\[.*\]$", val):         # parse vertex
        return ctx.lookup_v["modern"][val[2:-1]]
    elif isinstance(val, (str, unicode)) and re.match("^e\[.*\]\.id$", val):     # parse edge id
        return ctx.lookup_e["modern"][val[2:-4]].id
    elif isinstance(val, (str, unicode)) and re.match("^e\[.*\]$", val):         # parse edge
        return ctx.lookup_e["modern"][val[2:-1]]
    elif isinstance(val, unicode):
        return val.encode('utf-8')
    else:
        return str(val)


def __ordered_assertion(step):
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
        elif line[0] == "edge":
            assert_that(step.context.result[ix].label, equal_to(line[1]))
        elif line[0] == "map":
            assert_that(__convert(step.context.result[ix], step.context), json.loads(line[1]))
        else:
            raise ValueError("unknown type of " + line[0])


def __unordered_assertion(step):
    data = step.table

    # results from traversal should have the same number of entries as the feature data table
    assert_that(len(step.context.result), equal_to(len(data)))

    results_to_test = list(step.context.result)

    # finds a match in the results for each line of data to assert and then removes that item
    # from the list - in the end there should be no items left over and each will have been asserted
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
            v = step.context.lookup_v["modern"][val]
            assert_that(v, is_in(results_to_test))
            results_to_test.remove(v)
        elif line[0] == "edge":
            val = str(line[1])
            e = step.context.lookup_e["modern"][val]
            assert_that(e, is_in(results_to_test))
            results_to_test.remove(e)
        elif line[0] == "map":
            val = __convert(json.loads(line[1]), step.context)
            assert_that(val, is_in(results_to_test))
            results_to_test.remove(val)
        else:
            raise ValueError("unknown type of " + line[0])

    assert_that(len(results_to_test), is_(0))


def __translate(traversal):
    replaced = regex_as.sub(".as_(", traversal)
    return regex_in.sub(".in_(", replaced)
