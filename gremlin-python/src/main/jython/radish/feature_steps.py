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

import json
import re
from gremlin_python.structure.graph import Path
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import Barrier, Cardinality, P, TextP, Pop, Scope, Column, Order, Direction, T, Pick, Operator, IO, WithOptions
from radish import given, when, then, world
from hamcrest import *

regex_all = re.compile(r"Pop\.all")
regex_and = re.compile(r"([(.,\s])and\(")
regex_as = re.compile(r"([(.,\s])as\(")
regex_from = re.compile(r"([(.,\s])from\(")
regex_global = re.compile(r"([(.,\s])global")
regex_cardlist = re.compile(r"Cardinality\.list")
regex_in = re.compile(r"([(.,\s])in\(")
regex_is = re.compile(r"([(.,\s])is\(")
regex_not = re.compile(r"([(.,\s])not\(")
regex_or = re.compile(r"([(.,\s])or\(")
regex_with = re.compile(r"([(.,\s])with\(")
regex_true = re.compile(r"(true)")
regex_false = re.compile(r"(false)")

outV = __.outV
label = __.label
inV = __.inV
project = __.project
tail = __.tail

ignores = []


@given("the {graph_name:w} graph")
def choose_graph(step, graph_name):
    step.context.graph_name = graph_name
    step.context.g = traversal().withRemote(step.context.remote_conn[graph_name])


@given("the graph initializer of")
def initialize_graph(step):
    t = _make_traversal(step.context.g, step.text, {})

    # just be sure that the traversal returns something to prove that it worked to some degree. probably
    # is overkill to try to assert the complete success of this init operation. presumably the test
    # suite would fail elsewhere if this didn't work which would help identify a problem.
    result = t.toList()
    assert len(result) > 0

    # add the first result - if a map - to the bindings. this is useful for cases when parameters for
    # the test traversal need to come from the original graph initializer (i.e. a new graph is created
    # and you need the id of a vertex from that graph)


@given("an unsupported test")
def unsupported_scenario(step):
    # this is a do nothing step as the test can't be supported for whatever reason
    return


@given("using the parameter {param_name:w} defined as {param:QuotedString}")
def add_parameter(step, param_name, param):
    if not hasattr(step.context, "traversal_params"):
        step.context.traversal_params = {}

    step.context.traversal_params[param_name] = _convert(param.replace('\\"', '"'), step.context)


@given("the traversal of")
def translate_traversal(step):
    step.context.ignore = ignores.__contains__(step.text)
    step.context.traversal = _make_traversal(
        step.context.g, step.text,
        step.context.traversal_params if hasattr(step.context, "traversal_params") else {})


@when("iterated to list")
def iterate_the_traversal(step):
    if step.context.ignore:
        return
    
    step.context.result = map(lambda x: _convert_results(x), step.context.traversal.toList())


@when("iterated next")
def next_the_traversal(step):
    if step.context.ignore:
        return

    step.context.result = map(lambda x: _convert_results(x), step.context.traversal.next())


@then("the result should be {characterized_as:w}")
def assert_result(step, characterized_as):
    if step.context.ignore:
        return

    if characterized_as == "empty":        # no results
        assert_that(len(step.context.result), equal_to(0))
    elif characterized_as == "ordered":    # results asserted in the order of the data table
        _table_assertion(step.table, step.context.result, step.context, True)
    elif characterized_as == "unordered":  # results asserted in any order
        _table_assertion(step.table, step.context.result, step.context, False)
    elif characterized_as == "of":         # results may be of any of the specified items in the data table
        _any_assertion(step.table, step.context.result, step.context)
    else:
        raise ValueError("unknown data characterization of " + characterized_as)


@then("the graph should return {count:d} for count of {traversal_string:QuotedString}")
def assert_side_effects(step, count, traversal_string):
    if step.context.ignore:
        return

    t = _make_traversal(step.context.g, traversal_string.replace('\\"', '"'),
                        step.context.traversal_params if hasattr(step.context, "traversal_params") else {})
    assert_that(t.count().next(), equal_to(count))


@then("the result should have a count of {count:d}")
def assert_count(step, count):
    assert_that(len(step.context.result), equal_to(count))


@then("nothing should happen because")
def nothing_happening(step):
    return


def _convert(val, ctx):
    graph_name = ctx.graph_name
    if isinstance(val, dict):                                           # convert dictionary keys/values
        n = {}
        for key, value in val.items():
            n[_convert(key, ctx)] = _convert(value, ctx)
        return n
    elif isinstance(val, unicode):                                      # convert annoying python 2.x unicode nonsense
        return _convert(val.encode('utf-8'), ctx)
    elif isinstance(val, str) and re.match("^l\[.*\]$", val):           # parse list
        return [] if val == "l[]" else list(map((lambda x: _convert(x, ctx)), val[2:-1].split(",")))
    elif isinstance(val, str) and re.match("^s\[.*\]$", val):           # parse set
        return set() if val == "s[]" else set(map((lambda x: _convert(x, ctx)), val[2:-1].split(",")))
    elif isinstance(val, str) and re.match("^d\[.*\]\.[ilfdm]$", val):  # parse numeric
        return float(val[2:-3]) if val[2:-3].__contains__(".") else long(val[2:-3])
    elif isinstance(val, str) and re.match("^v\[.*\]\.id$", val):       # parse vertex id
        return __find_cached_element(ctx, graph_name, val[2:-4], "v").id
    elif isinstance(val, str) and re.match("^v\[.*\]\.sid$", val):      # parse vertex id as string
        return str(__find_cached_element(ctx, graph_name, val[2:-5], "v").id)
    elif isinstance(val, str) and re.match("^v\[.*\]$", val):           # parse vertex
        return __find_cached_element(ctx, graph_name, val[2:-1], "v")
    elif isinstance(val, str) and re.match("^e\[.*\]\.id$", val):       # parse edge id
        return __find_cached_element(ctx, graph_name, val[2:-4], "e").id
    elif isinstance(val, str) and re.match("^e\[.*\]\.sid$", val):      # parse edge id as string
        return str(__find_cached_element(ctx, graph_name, val[2:-5], "e").id)
    elif isinstance(val, str) and re.match("^e\[.*\]$", val):           # parse edge
        return __find_cached_element(ctx, graph_name, val[2:-1], "e")
    elif isinstance(val, str) and re.match("^m\[.*\]$", val):           # parse json as a map
        return _convert(json.loads(val[2:-1]), ctx)
    elif isinstance(val, str) and re.match("^p\[.*\]$", val):           # parse path
        path_objects = list(map((lambda x: _convert(x, ctx)), val[2:-1].split(",")))
        return Path([set([])], path_objects)
    elif isinstance(val, str) and re.match("^c\[.*\]$", val):           # parse lambda/closure
        return lambda: (val[2:-1], "gremlin-groovy")
    elif isinstance(val, str) and re.match("^t\[.*\]$", val):           # parse instance of T enum
        return T[val[2:-1]]
    elif isinstance(val, str) and re.match("^D\[.*\]$", val):           # parse instance of Direction enum
        return Direction[val[2:-1]]
    else:
        return val


def __find_cached_element(ctx, graph_name, identifier, element_type):
    if graph_name == "empty":
        cache = world.create_lookup_v(ctx.remote_conn["empty"]) if element_type == "v" else world.create_lookup_e(ctx.remote_conn["empty"])
    else:
        cache = ctx.lookup_v[graph_name] if element_type == "v" else ctx.lookup_e[graph_name]

    return cache[identifier]


def _convert_results(val):
    if isinstance(val, Path):
        # kill out labels as they aren't in the assertion logic
        return Path([set([])], map(lambda p: p.encode("utf-8") if isinstance(p, unicode) else p, val.objects))
    else:
        return val


def _any_assertion(data, result, ctx):
    converted = [_convert(line['result'], ctx) for line in data]
    for r in result:
        assert_that(r, is_in(converted))


def _table_assertion(data, result, ctx, ordered):
    # results from traversal should have the same number of entries as the feature data table
    assert_that(len(result), equal_to(len(data)), "result:" + str(result))

    results_to_test = list(result)

    # finds a match in the results for each line of data to assert and then removes that item
    # from the list - in the end there should be no items left over and each will have been asserted
    for ix, line in enumerate(data):
        val = _convert(line['result'], ctx)

        # clear the labels since we don't define them in .feature files
        if isinstance(val, Path):
            val.labels = [set([])]

        if ordered:
            assert_that(results_to_test[ix], equal_to(val))
        else:
            assert_that(val, is_in(results_to_test))
            results_to_test.remove(val)

    if not ordered:
        assert_that(len(results_to_test), is_(0))


def _translate(traversal_):
    replaced = traversal_.replace("\n", "")
    replaced = regex_all.sub(r"Pop.all_", replaced)
    replaced = regex_cardlist.sub(r"Cardinality.list_", replaced)
    replaced = regex_and.sub(r"\1and_(", replaced)
    replaced = regex_from.sub(r"\1from_(", replaced)
    replaced = regex_global.sub(r"\1global_", replaced)
    replaced = regex_as.sub(r"\1as_(", replaced)
    replaced = regex_is.sub(r"\1is_(", replaced)
    replaced = regex_not.sub(r"\1not_(", replaced)
    replaced = regex_or.sub(r"\1or_(", replaced)
    replaced = regex_in.sub(r"\1in_(", replaced)
    replaced = regex_with.sub(r"\1with_(", replaced)
    replaced = regex_true.sub(r"True", replaced)
    return regex_false.sub(r"False", replaced)


def _make_traversal(g, traversal_string, params):
    b = {"g": g,
         "__": __,
         "Barrier": Barrier,
         "Cardinality": Cardinality,
         "Column": Column,
         "Direction": Direction,
         "Order": Order,
         "P": P,
         "TextP": TextP,
         "IO": IO,
         "Pick": Pick,
         "Pop": Pop,
         "Scope": Scope,
         "Operator": Operator,
         "T": T,
         "WithOptions": WithOptions}

    b.update(params)

    return eval(_translate(traversal_string), b)
