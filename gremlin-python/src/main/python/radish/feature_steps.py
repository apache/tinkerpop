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

from datetime import datetime
import json
import re
from gremlin_python.statics import long
from gremlin_python.structure.graph import Path, Vertex
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import Barrier, Cardinality, P, TextP, Pop, Scope, Column, Order, Direction, T, \
    Pick, Operator, IO, WithOptions, Merge, GValue
from radish import given, when, then, world
from hamcrest import *

outV = __.out_v
label = __.label
inV = __.in_v
project = __.project
tail = __.tail

ignores = [
    "g.withoutStrategies(CountStrategy).V().count()",  # serialization issues with Class in GraphSON
    "g.withoutStrategies(LazyBarrierStrategy).V().as(\"label\").aggregate(local,\"x\").select(\"x\").select(\"label\")",
    "g.withSack(xx1, Operator.assign).V().local(__.out(\"knows\").barrier(Barrier.normSack)).in(\"knows\").barrier().sack()", # issues with BigInteger/BigDecimal - why do we carry BigDecimal? just use python Decimal module?
    "g.withSack(2).V().sack(Operator.div).by(__.constant(xx1)).sack()", # issues with BigInteger/BigDecimal - why do we carry BigDecimal? just use python Decimal module?
    ## The following section has queries that aren't supported by gremlin-lang parameters
    'g.V().branch(l1).option("a", __.values("age")).option("b", __.values("lang")).option("b", __.values("name"))',
    'g.V().choose(pred1, __.out("knows"), __.in("created")).values("name")',
    'g.V().repeat(__.both()).until(pred1).groupCount().by("name")',
    'g.V().both().properties("name").order().by(c1).dedup().value()',
    'g.V().filter(pred1)',
    'g.V(vid1).filter(pred1)',
    'g.V(vid2).filter(pred1)',
    'g.V(vid1).out().filter(pred1)',
    'g.E().filter(pred1)',
    'g.V().out("created").has("name", __.map(l1).is(P.gt(3))).values("name")',
    'g.V(vid1).map(l1)',
    'g.V(vid1).outE().label().map(l1)',
    'g.V(vid1).out().map(l1).map(l2)',
    'g.withPath().V().as("a").out().map(l1)',
    'g.withPath().V().as("a").out().out().map(l1)',
    'g.V().values("name").order().by(c1).by(c2)',
    'g.V().order().by("name", c1).by("name", c2).values("name")',
    'g.V().hasLabel("person").order().by(l1, Order.desc).values("name")',
    'g.V(v1).hasLabel("person").map(l1).order(Scope.local).by(Column.values, Order.desc).by(Column.keys, Order.asc)',
    'g.V().valueMap().unfold().map(l1)',
    'g.E(e11)',
    'g.E(e7,e11)',
    'g.E(xx1)',
    'g.withSideEffect("a", xx1).V().both().values("name").aggregate(Scope.local,"a").cap("a")',
    'g.V().group().by(l1).by(__.constant(1))',
    'g.V(vid1).out().values("name").inject("daniel").as("a").map(l1).path()',
    'g.V().group("a").by(l1).by(__.constant(1)).cap("a")',
    'g.withSideEffect("a", xx1).V().both().values("name").store("a").cap("a")'
    ## section end
]


@given("the {graph_name:w} graph")
def choose_graph(step, graph_name):
    # if we have no traversals then we are ignoring the test - should be temporary until we can settle out the
    # issue of handling the removal of lambdas from Gremlin as a language
    step.context.ignore = len(step.context.traversals) == 0
    tagset = [tag.name for tag in step.all_tags]
    if not step.context.ignore:
        step.context.ignore = "AllowNullPropertyValues" in tagset

    if (step.context.ignore):
        return

    step.context.graph_name = graph_name
    step.context.g = traversal().with_(step.context.remote_conn[graph_name]).with_('language', 'gremlin-lang')


@given("the graph initializer of")
def initialize_graph(step):
    if (step.context.ignore):
        return

    t = step.context.traversals.pop(0)(g=step.context.g)

    # just be sure that the traversal returns something to prove that it worked to some degree. probably
    # is overkill to try to assert the complete success of this init operation. presumably the test
    # suite would fail elsewhere if this didn't work which would help identify a problem.
    result = t.to_list()
    assert len(result) > 0


@given("an unsupported test")
def unsupported_scenario(step):
    # this is a do nothing step as the test can't be supported for whatever reason
    return


@given("using the parameter {param_name:w} defined as {param:QuotedString}")
def add_parameter(step, param_name, param):
    if (step.context.ignore):
        return

    if not hasattr(step.context, "traversal_params"):
        step.context.traversal_params = {}

    step.context.traversal_params[param_name] = _convert(param.replace('\\"', '"'), step.context)


@given("the traversal of")
def translate_traversal(step):
    if step.context.ignore == False:
        step.context.ignore = step.text in ignores
    if step.context.ignore:
        return

    p = {}
    if hasattr(step.context, "traversal_params"):
        # user flag "parameterize", when set to 'true' will use GValue to parameterize parameters instead of flattening them out into string
        if world.config.user_data.get("parameterize"):
            for k, v in step.context.traversal_params.items():
                p[k] = GValue(k, v)
        else:
            p = step.context.traversal_params

    localg = step.context.g.with_('language', 'gremlin-lang')
    tagset = [tag.name for tag in step.all_tags]
    if "GraphComputerOnly" in tagset:
        localg = step.context.g.with_('language', 'gremlin-lang').with_computer()
    p['g'] = localg
    step.context.traversal = step.context.traversals.pop(0)(**p)


@when("iterated to list")
def iterate_the_traversal(step):
    if step.context.ignore:
        return

    try:
        step.context.result = list(map(lambda x: _convert_results(x), step.context.traversal.to_list()))
        step.context.failed = False
        step.context.failed_message = ''
    except Exception as e:
        step.context.failed = True
        step.context.failed_message = getattr(e, 'message', repr(e))


@when("iterated next")
def next_the_traversal(step):
    if step.context.ignore:
        return

    try:
        step.context.result = list(map(lambda x: _convert_results(x), step.context.traversal.next()))
        step.context.failed = False
        step.context.failed_message = ''
    except Exception as e:
        step.context.failed = True
        step.context.failed_message = getattr(e, 'message', repr(e))


@then("the traversal will raise an error")
def raise_an_error(step):
    assert_that(step.context.failed, equal_to(True))


@then("the traversal will raise an error with message {comparison:w} text of {expected_message:QuotedString}")
def raise_an_error_with_message(step, comparison, expected_message):
    assert_that(step.context.failed, equal_to(True))

    if comparison == "containing":
        assert_that(step.context.failed_message.upper(), contains_string(expected_message.upper()))
    elif comparison == "ending":
        assert_that(step.context.failed_message.upper(), ends_with(expected_message.upper()))
    elif comparison == "starting":
        assert_that(step.context.failed_message.upper(), starts_with(expected_message.upper()))
    else:
        raise ValueError("unknown comparison '" + comparison + "'- must be: containing, ending or starting")


@then("the result should be {characterized_as:w}")
def assert_result(step, characterized_as):
    if step.context.ignore:
        return

    assert_that(step.context.failed, equal_to(False), step.context.failed_message)

    if characterized_as == "empty":  # no results
        assert_that(len(step.context.result), equal_to(0))
    elif characterized_as == "ordered":  # results asserted in the order of the data table
        _table_assertion(step.table, step.context.result, step.context, True)
    elif characterized_as == "unordered":  # results asserted in any order
        _table_assertion(step.table, step.context.result, step.context, False)
    elif characterized_as == "of":  # results may be of any of the specified items in the data table
        _any_assertion(step.table, step.context.result, step.context)
    else:
        raise ValueError("unknown data characterization of " + characterized_as)


@then("the graph should return {count:d} for count of {traversal_string:QuotedString}")
def assert_side_effects(step, count, traversal_string):
    if step.context.ignore:
        return

    assert_that(step.context.failed, equal_to(False), step.context.failed_message)

    p = step.context.traversal_params if hasattr(step.context, "traversal_params") else {}
    p['g'] = step.context.g
    t = step.context.traversals.pop(0)(**p)

    assert_that(t.count().next(), equal_to(count))


@then("the result should have a count of {count:d}")
def assert_count(step, count):
    if step.context.ignore:
        return

    assert_that(step.context.failed, equal_to(False), step.context.failed_message)

    assert_that(len(list(step.context.result)), equal_to(count))


@then("nothing should happen because")
def nothing_happening(step):
    return


def _convert(val, ctx):
    graph_name = ctx.graph_name
    if isinstance(val, dict):  # convert dictionary keys/values
        n = {}
        for key, value in val.items():
            k = _convert(key, ctx)
            # convert to tuple key if list/set as neither are hashable
            n[tuple(k) if isinstance(k, (set, list)) else k] = _convert(value, ctx)
        return n
    elif isinstance(val, str) and re.match(r"^l\[.*\]$", val):  # parse list
        return [] if val == "l[]" else list(map((lambda x: _convert(x, ctx)), val[2:-1].split(",")))
    elif isinstance(val, str) and re.match(r"^s\[.*\]$", val):  # parse set
        return set() if val == "s[]" else set(map((lambda x: _convert(x, ctx)), val[2:-1].split(",")))
    elif isinstance(val, str) and re.match(r"^str\[.*\]$", val):  # return string as is
        return val[4:-1]
    elif isinstance(val, str) and re.match(r"^dt\[.*\]$", val):  # parse datetime
        # python 3.8 can handle only subset of ISO 8601 dates
        return datetime.fromisoformat(val[3:-1].replace('Z', '+00:00'))
    elif isinstance(val, str) and re.match(r"^d\[NaN\]$", val):  # parse nan
        return float("nan")
    elif isinstance(val, str) and re.match(r"^d\[Infinity\]$", val):  # parse +inf
        return float("inf")
    elif isinstance(val, str) and re.match(r"^d\[-Infinity\]$", val):  # parse -inf
        return float("-inf")
    elif isinstance(val, str) and re.match(r"^d\[.*\]\.[bsilfdmn]$", val):  # parse numeric
        return float(val[2:-3]) if val[2:-3].__contains__(".") else long(val[2:-3])
    elif isinstance(val, str) and re.match(r"^v\[.*\]\.id$", val):  # parse vertex id
        return __find_cached_element(ctx, graph_name, val[2:-4], "v").id
    elif isinstance(val, str) and re.match(r"^v\[.*\]\.sid$", val):  # parse vertex id as string
        return str(__find_cached_element(ctx, graph_name, val[2:-5], "v").id)
    elif isinstance(val, str) and re.match(r"^v\[.*\]$", val):  # parse vertex
        return __find_cached_element(ctx, graph_name, val[2:-1], "v")
    elif isinstance(val, str) and re.match(r"^e\[.*\]\.id$", val):  # parse edge id
        return __find_cached_element(ctx, graph_name, val[2:-4], "e").id
    elif isinstance(val, str) and re.match(r"^e\[.*\]\.sid$", val):  # parse edge id as string
        return str(__find_cached_element(ctx, graph_name, val[2:-5], "e").id)
    elif isinstance(val, str) and re.match(r"^e\[.*\]$", val):  # parse edge
        return __find_cached_element(ctx, graph_name, val[2:-1], "e")
    elif isinstance(val, str) and re.match(r"^vp\[.*\]$", val):  # parse vertexproperty
        return __find_cached_element(ctx, graph_name, val[3:-1], "vp")
    elif isinstance(val, str) and re.match(r"^m\[.*\]$", val):  # parse json as a map
        return _convert(json.loads(val[2:-1]), ctx)
    elif isinstance(val, str) and re.match(r"^p\[.*\]$", val):  # parse path
        path_objects = list(map((lambda x: _convert(x, ctx)), val[2:-1].split(",")))
        return Path([set([])], path_objects)
    elif isinstance(val, str) and re.match(r"^c\[.*\]$", val):  # parse lambda/closure
        return lambda: (val[2:-1], "gremlin-groovy")
    elif isinstance(val, str) and re.match(r"^t\[.*\]$", val):  # parse instance of T enum
        return T[val[2:-1]]
    elif isinstance(val, str) and re.match(r"^D\[.*\]$", val):  # parse instance of Direction enum
        return Direction[__alias_direction(val[2:-1])]
    elif isinstance(val, str) and re.match(r"^M\[.*\]$", val):  # parse instance of Merge enum
        return Merge[__alias_merge(val[2:-1])]
    elif isinstance(val, str) and re.match(r"^null$", val):  # parse null to None
        return None
    elif isinstance(val, str) and re.match(r"^true$", val):  # parse to boolean
        return True
    elif isinstance(val, str) and re.match(r"^false$", val):  # parse to boolean
        return False
    else:
        return val


def __alias_direction(d):
    return "from_" if d == "from" else d


def __alias_merge(m):
    if m == "inV":
        return "in_v"
    elif m == "outV":
        return "out_v"
    elif m == "onCreate":
        return "on_create"
    elif m == "onMatch":
        return "on_match"
    else:
        return


def __find_cached_element(ctx, graph_name, identifier, element_type):
    if graph_name == "empty":
        if element_type == "v":
            cache = world.create_lookup_v(ctx.remote_conn["empty"])
        elif element_type == "vp":
            cache = world.create_lookup_vp(ctx.remote_conn["empty"])
        else:
            cache = world.create_lookup_e(ctx.remote_conn["empty"])
    else:
        if element_type == "v":
            cache = ctx.lookup_v[graph_name]
        elif element_type == "vp":
            cache = ctx.lookup_vp[graph_name]
        else:
            cache = ctx.lookup_e[graph_name]

    # try to lookup the element - if it can't be found then it must be a reference Vertex
    if identifier in cache:
        return cache[identifier]
    else:
        return Vertex(identifier)


def _is_nan(val):
    return isinstance(val, float) and (val != val)


def _convert_results(val):
    if isinstance(val, Path):
        # kill out labels as they aren't in the assertion logic
        return Path([set([])], val.objects)
    elif _is_nan(val):
        # we need to use the string form for NaN to test the results since float.nan != float.nan
        return "d[NaN]"
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
        # we need to use the string form for NaN to test the results since float.nan != float.nan
        if _is_nan(val):
            val = "d[NaN]"

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
