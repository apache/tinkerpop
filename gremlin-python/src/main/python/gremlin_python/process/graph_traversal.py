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
import logging
import sys
import copy
import warnings
from threading import Lock
from .traversal import Traversal
from .traversal import TraversalStrategies
from .strategies import VertexProgramStrategy, OptionsStrategy
from .traversal import Bytecode
from ..driver.remote_connection import RemoteStrategy
from .. import statics
from ..statics import long

log = logging.getLogger("gremlinpython")

__author__ = 'Stephen Mallette (http://stephen.genoprime.com), Lyndon Bauto (lyndonb@bitquilltech.com)'


class GraphTraversalSource(object):
    def __init__(self, graph, traversal_strategies, bytecode=None, remote_connection=None):
        log.info("Creating GraphTraversalSource.")
        self.graph = graph
        self.traversal_strategies = traversal_strategies
        if bytecode is None:
            bytecode = Bytecode()
        self.bytecode = bytecode
        self.graph_traversal = GraphTraversal
        if remote_connection:
            self.traversal_strategies.add_strategies([RemoteStrategy(remote_connection)])
        self.remote_connection = remote_connection

    def __repr__(self):
        return "graphtraversalsource[" + str(self.graph) + "]"

    def get_graph_traversal_source(self):
        return self.__class__(self.graph, TraversalStrategies(self.traversal_strategies), Bytecode(self.bytecode))

    def get_graph_traversal(self):
        return self.graph_traversal(self.graph, self.traversal_strategies, Bytecode(self.bytecode))

    def withBulk(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.withBulk will be replaced by "
            "gremlin_python.process.GraphTraversalSource.with_bulk.",
            DeprecationWarning)
        return self.with_bulk(*args)

    def with_bulk(self, *args):
        source = self.get_graph_traversal_source()
        source.bytecode.add_source("withBulk", *args)
        return source

    def withPath(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.withPath will be replaced by "
            "gremlin_python.process.Traversal.with_path.",
            DeprecationWarning)
        return self.with_path(*args)

    def with_path(self, *args):
        source = self.get_graph_traversal_source()
        source.bytecode.add_source("withPath", *args)
        return source

    def withSack(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.withSack will be replaced by "
            "gremlin_python.process.GraphTraversalSource.with_sack.",
            DeprecationWarning)
        return self.with_sack(*args)

    def with_sack(self, *args):
        source = self.get_graph_traversal_source()
        source.bytecode.add_source("withSack", *args)
        return source

    def withSideEffect(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.with_side_effect will be replaced by "
            "gremlin_python.process.GraphTraversalSource.with_sack.",
            DeprecationWarning)
        return self.with_side_effect(*args)

    def with_side_effect(self, *args):
        source = self.get_graph_traversal_source()
        source.bytecode.add_source("withSideEffect", *args)
        return source

    def withStrategies(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.withStrategies will be replaced by "
            "gremlin_python.process.GraphTraversalSource.with_strategies.",
            DeprecationWarning)
        return self.with_strategies(*args)

    def with_strategies(self, *args):
        source = self.get_graph_traversal_source()
        source.bytecode.add_source("withStrategies", *args)
        return source

    def withoutStrategies(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.withoutStrategies will be replaced by "
            "gremlin_python.process.GraphTraversalSource.without_strategies.",
            DeprecationWarning)
        return self.without_strategies(*args)

    def without_strategies(self, *args):
        source = self.get_graph_traversal_source()
        source.bytecode.add_source("withoutStrategies", *args)
        return source

    def with_(self, k, v=None):
        source = self.get_graph_traversal_source()
        options_strategy = next((x for x in source.bytecode.source_instructions
                                 if x[0] == "withStrategies" and type(x[1]) is OptionsStrategy), None)

        val = True if v is None else v
        if options_strategy is None:
            options_strategy = OptionsStrategy(**{k: val})
            source = self.with_strategies(options_strategy)
        else:
            options_strategy[1].configuration[k] = val

        return source

    def tx(self):
        # In order to keep the constructor unchanged within 3.5.x we can try to pop the RemoteConnection out of the
        # TraversalStrategies. keeping this unchanged will allow user DSLs to not take a break.
        # This is the same strategy as gremlin-javascript.
        # TODO https://issues.apache.org/jira/browse/TINKERPOP-2664: refactor this to be nicer in 3.6.0 when
        #  we can take a breaking change
        remote_connection = next((x.remote_connection for x in self.traversal_strategies.traversal_strategies if
                                  x.fqcn == "py:RemoteStrategy"), None)

        if remote_connection is None:
            raise Exception("Error, remote connection is required for transaction.")

        # You can't do g.tx().begin().tx() i.e child transactions are not supported.
        if remote_connection and remote_connection.is_session_bound():
            raise Exception("This TraversalSource is already bound to a transaction - child transactions are not "
                            "supported")
        return Transaction(self, remote_connection)

    def withComputer(self, graph_computer=None, workers=None, result=None, persist=None, vertices=None,
                     edges=None, configuration=None):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.withComputer will be replaced by "
            "gremlin_python.process.GraphTraversalSource.with_computer.",
            DeprecationWarning)
        return self.with_computer(graph_computer, workers, result, persist, vertices, edges, configuration)

    def with_computer(self, graph_computer=None, workers=None, result=None, persist=None, vertices=None,
                      edges=None, configuration=None):
        return self.with_strategies(
            VertexProgramStrategy(graph_computer, workers, result, persist, vertices, edges, configuration))

    def E(self, *args):
        traversal = self.get_graph_traversal()
        traversal.bytecode.add_step("E", *args)
        return traversal

    def V(self, *args):
        traversal = self.get_graph_traversal()
        traversal.bytecode.add_step("V", *args)
        return traversal

    def addE(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.addE will be replaced by "
            "gremlin_python.process.GraphTraversalSource.add_e.",
            DeprecationWarning)
        return self.add_e(*args)

    def add_e(self, *args):
        traversal = self.get_graph_traversal()
        traversal.bytecode.add_step("addE", *args)
        return traversal

    def addV(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.addV will be replaced by "
            "gremlin_python.process.GraphTraversalSource.add_v.",
            DeprecationWarning)
        return self.add_v(*args)

    def add_v(self, *args):
        traversal = self.get_graph_traversal()
        traversal.bytecode.add_step("addV", *args)
        return traversal

    def merge_v(self, *args):
        traversal = self.get_graph_traversal()
        traversal.bytecode.add_step("mergeV", *args)
        return traversal

    def merge_e(self, *args):
        traversal = self.get_graph_traversal()
        traversal.bytecode.add_step("mergeE", *args)
        return traversal

    def inject(self, *args):
        traversal = self.get_graph_traversal()
        traversal.bytecode.add_step("inject", *args)
        return traversal

    def io(self, *args):
        traversal = self.get_graph_traversal()
        traversal.bytecode.add_step("io", *args)
        return traversal

    def call(self, *args):
        traversal = self.get_graph_traversal()
        traversal.bytecode.add_step("call", *args)
        return traversal

    def union(self, *args):
        traversal = self.get_graph_traversal()
        traversal.bytecode.add_step("union", *args)
        return traversal


class GraphTraversal(Traversal):
    def __init__(self, graph, traversal_strategies, bytecode):
        super(GraphTraversal, self).__init__(graph, traversal_strategies, bytecode)

    def __getitem__(self, index):
        if isinstance(index, int):
            return self.range_(long(index), long(index + 1))
        elif isinstance(index, slice):
            low = long(0) if index.start is None else long(index.start)
            high = long(sys.maxsize) if index.stop is None else long(index.stop)
            if low == long(0):
                return self.limit(high)
            else:
                return self.range_(low, high)
        else:
            raise TypeError("Index must be int or slice")

    def __getattr__(self, key):
        if key.startswith('__'):
            raise AttributeError(
                'Python magic methods or keys starting with double underscore cannot be used for Gremlin sugar - prefer values(' + key + ')')
        return self.values(key)

    def clone(self):
        return GraphTraversal(self.graph, self.traversal_strategies, copy.deepcopy(self.bytecode))

    def V(self, *args):
        self.bytecode.add_step("V", *args)
        return self

    def E(self, *args):
        self.bytecode.add_step("E", *args)
        return self

    def addE(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversal.addE will be replaced by "
            "gremlin_python.process.GraphTraversal.add_e.",
            DeprecationWarning)
        return self.add_e(*args)

    def add_e(self, *args):
        self.bytecode.add_step("addE", *args)
        return self

    def addV(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.addV will be replaced by "
            "gremlin_python.process.GraphTraversalSource.add_v.",
            DeprecationWarning)
        return self.add_v(*args)

    def add_v(self, *args):
        self.bytecode.add_step("addV", *args)
        return self

    def aggregate(self, *args):
        self.bytecode.add_step("aggregate", *args)
        return self

    def all_(self, *args):
        self.bytecode.add_step("all", *args)
        return self

    def and_(self, *args):
        self.bytecode.add_step("and", *args)
        return self

    def any_(self, *args):
        self.bytecode.add_step("any", *args)
        return self

    def as_(self, *args):
        self.bytecode.add_step("as", *args)
        return self

    def as_bool(self, *args):
        self.bytecode.add_step("asBool", *args)
        return self

    def as_date(self, *args):
        self.bytecode.add_step("asDate", *args)
        return self

    def as_number(self, *args):
        self.bytecode.add_step("asNumber", *args)
        return self

    def as_string(self, *args):
        self.bytecode.add_step("asString", *args)
        return self

    def barrier(self, *args):
        self.bytecode.add_step("barrier", *args)
        return self

    def both(self, *args):
        self.bytecode.add_step("both", *args)
        return self

    def bothE(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.bothE will be replaced by "
            "gremlin_python.process.GraphTraversalSource.both_e.",
            DeprecationWarning)
        return self.both_e(*args)

    def both_e(self, *args):
        self.bytecode.add_step("bothE", *args)
        return self

    def bothV(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.bothV will be replaced by "
            "gremlin_python.process.GraphTraversalSource.both_v.",
            DeprecationWarning)
        return self.both_v(*args)

    def both_v(self, *args):
        self.bytecode.add_step("bothV", *args)
        return self

    def branch(self, *args):
        self.bytecode.add_step("branch", *args)
        return self

    def by(self, *args):
        self.bytecode.add_step("by", *args)
        return self

    def call(self, *args):
        self.bytecode.add_step("call", *args)
        return self

    def cap(self, *args):
        self.bytecode.add_step("cap", *args)
        return self

    def choose(self, *args):
        self.bytecode.add_step("choose", *args)
        return self

    def coalesce(self, *args):
        self.bytecode.add_step("coalesce", *args)
        return self

    def coin(self, *args):
        self.bytecode.add_step("coin", *args)
        return self

    def combine(self, *args):
        self.bytecode.add_step("combine", *args)
        return self

    def concat(self, *args):
        self.bytecode.add_step("concat", *args)
        return self

    def conjoin(self, *args):
        self.bytecode.add_step("conjoin", *args)
        return self

    def connectedComponent(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.connectedComponent will be replaced by "
            "gremlin_python.process.GraphTraversalSource.connected_component.",
            DeprecationWarning)
        return self.connected_component(*args)

    def connected_component(self, *args):
        self.bytecode.add_step("connectedComponent", *args)
        return self

    def constant(self, *args):
        self.bytecode.add_step("constant", *args)
        return self

    def count(self, *args):
        self.bytecode.add_step("count", *args)
        return self

    def cyclicPath(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.cyclicPath will be replaced by "
            "gremlin_python.process.GraphTraversalSource.cyclic_path.",
            DeprecationWarning)
        return self.cyclic_path(*args)

    def cyclic_path(self, *args):
        self.bytecode.add_step("cyclicPath", *args)
        return self

    def date_add(self, *args):
        self.bytecode.add_step("dateAdd", *args)
        return self

    def date_diff(self, *args):
        self.bytecode.add_step("dateDiff", *args)
        return self

    def dedup(self, *args):
        self.bytecode.add_step("dedup", *args)
        return self

    def difference(self, *args):
        self.bytecode.add_step("difference", *args)
        return self

    def discard(self, *args):
        self.bytecode.add_step("discard", *args)
        return self

    def disjunct(self, *args):
        self.bytecode.add_step("disjunct", *args)
        return self

    def drop(self, *args):
        self.bytecode.add_step("drop", *args)
        return self

    def element(self, *args):
        self.bytecode.add_step("element", *args)
        return self

    def elementMap(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.elementMap will be replaced by "
            "gremlin_python.process.GraphTraversalSource.element_map.",
            DeprecationWarning)
        return self.element_map(*args)

    def element_map(self, *args):
        self.bytecode.add_step("elementMap", *args)
        return self

    def emit(self, *args):
        self.bytecode.add_step("emit", *args)
        return self

    def fail(self, *args):
        self.bytecode.add_step("fail", *args)
        return self

    def filter_(self, *args):
        self.bytecode.add_step("filter", *args)
        return self

    def flatMap(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.flatMap will be replaced by "
            "gremlin_python.process.GraphTraversalSource.flat_map.",
            DeprecationWarning)
        return self.flat_map(*args)

    def flat_map(self, *args):
        self.bytecode.add_step("flatMap", *args)
        return self

    def fold(self, *args):
        self.bytecode.add_step("fold", *args)
        return self

    def format_(self, *args):
        self.bytecode.add_step("format", *args)
        return self

    def from_(self, *args):
        self.bytecode.add_step("from", *args)
        return self

    def group(self, *args):
        self.bytecode.add_step("group", *args)
        return self

    def groupCount(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.groupCount will be replaced by "
            "gremlin_python.process.GraphTraversalSource.group_count.",
            DeprecationWarning)
        return self.group_count(*args)

    def group_count(self, *args):
        self.bytecode.add_step("groupCount", *args)
        return self

    def has(self, *args):
        self.bytecode.add_step("has", *args)
        return self

    def hasId(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.hasId will be replaced by "
            "gremlin_python.process.GraphTraversalSource.has_id.",
            DeprecationWarning)
        return self.has_id(*args)

    def has_id(self, *args):
        self.bytecode.add_step("hasId", *args)
        return self

    def hasKey(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.hasKey will be replaced by "
            "gremlin_python.process.GraphTraversalSource.has_key.",
            DeprecationWarning)
        return self.has_key(*args)

    def has_key_(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.has_key_ will be replaced by "
            "gremlin_python.process.GraphTraversalSource.has_key.",
            DeprecationWarning)
        return self.has_key(*args)
    
    def has_key(self, *args):
        self.bytecode.add_step("hasKey", *args)
        return self

    def has_key(self, *args):
        self.bytecode.add_step("hasKey", *args)
        return self

    def hasLabel(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.hasLabel will be replaced by "
            "gremlin_python.process.GraphTraversalSource.has_label.",
            DeprecationWarning)
        return self.has_label(*args)

    def has_label(self, *args):
        self.bytecode.add_step("hasLabel", *args)
        return self

    def hasNot(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.hasNot will be replaced by "
            "gremlin_python.process.GraphTraversalSource.has_not.",
            DeprecationWarning)
        return self.has_not(*args)

    def has_not(self, *args):
        self.bytecode.add_step("hasNot", *args)
        return self

    def hasValue(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.hasValue will be replaced by "
            "gremlin_python.process.GraphTraversalSource.has_value.",
            DeprecationWarning)
        return self.has_value(*args)

    def has_value(self, *args):
        self.bytecode.add_step("hasValue", *args)
        return self

    def id_(self, *args):
        self.bytecode.add_step("id", *args)
        return self

    def identity(self, *args):
        self.bytecode.add_step("identity", *args)
        return self

    def inE(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.inE will be replaced by "
            "gremlin_python.process.GraphTraversalSource.in_e.",
            DeprecationWarning)
        return self.in_e(*args)

    def in_e(self, *args):
        self.bytecode.add_step("inE", *args)
        return self

    def inV(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.inV will be replaced by "
            "gremlin_python.process.GraphTraversalSource.in_v.",
            DeprecationWarning)
        return self.in_v(*args)

    def in_v(self, *args):
        self.bytecode.add_step("inV", *args)
        return self

    def in_(self, *args):
        self.bytecode.add_step("in", *args)
        return self

    def index(self, *args):
        self.bytecode.add_step("index", *args)
        return self

    def inject(self, *args):
        self.bytecode.add_step("inject", *args)
        return self

    def intersect(self, *args):
        self.bytecode.add_step("intersect", *args)
        return self

    def is_(self, *args):
        self.bytecode.add_step("is", *args)
        return self

    def key(self, *args):
        self.bytecode.add_step("key", *args)
        return self

    def label(self, *args):
        self.bytecode.add_step("label", *args)
        return self

    def length(self, *args):
        self.bytecode.add_step("length", *args)
        return self

    def limit(self, *args):
        self.bytecode.add_step("limit", *args)
        return self

    def local(self, *args):
        self.bytecode.add_step("local", *args)
        return self

    def loops(self, *args):
        self.bytecode.add_step("loops", *args)
        return self

    def lTrim(self, *args):
        self.bytecode.add_step("lTrim", *args)
        return self

    def l_trim(self, *args):
        self.bytecode.add_step("lTrim", *args)
        return self

    def map(self, *args):
        self.bytecode.add_step("map", *args)
        return self

    def match(self, *args):
        self.bytecode.add_step("match", *args)
        return self

    def math(self, *args):
        self.bytecode.add_step("math", *args)
        return self

    def max_(self, *args):
        self.bytecode.add_step("max", *args)
        return self

    def mean(self, *args):
        self.bytecode.add_step("mean", *args)
        return self

    def merge(self, *args):
        self.bytecode.add_step("merge", *args)
        return self

    def merge_e(self, *args):
        self.bytecode.add_step("mergeE", *args)
        return self

    def merge_v(self, *args):
        self.bytecode.add_step("mergeV", *args)
        return self

    def min_(self, *args):
        self.bytecode.add_step("min", *args)
        return self

    def none(self, *args):
        self.bytecode.add_step("none", *args)
        return self

    def not_(self, *args):
        self.bytecode.add_step("not", *args)
        return self

    def option(self, *args):
        self.bytecode.add_step("option", *args)
        return self

    def optional(self, *args):
        self.bytecode.add_step("optional", *args)
        return self

    def or_(self, *args):
        self.bytecode.add_step("or", *args)
        return self

    def order(self, *args):
        self.bytecode.add_step("order", *args)
        return self

    def otherV(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.otherV will be replaced by "
            "gremlin_python.process.GraphTraversalSource.other_v.",
            DeprecationWarning)
        return self.other_v(*args)

    def other_v(self, *args):
        self.bytecode.add_step("otherV", *args)
        return self

    def out(self, *args):
        self.bytecode.add_step("out", *args)
        return self

    def outE(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.outE will be replaced by "
            "gremlin_python.process.GraphTraversalSource.out_e.",
            DeprecationWarning)
        return self.out_e(*args)

    def out_e(self, *args):
        self.bytecode.add_step("outE", *args)
        return self

    def outV(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.outV will be replaced by "
            "gremlin_python.process.GraphTraversalSource.out_v.",
            DeprecationWarning)
        return self.out_v(*args)

    def out_v(self, *args):
        self.bytecode.add_step("outV", *args)
        return self

    def pageRank(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.pageRank will be replaced by "
            "gremlin_python.process.GraphTraversalSource.page_rank.",
            DeprecationWarning)
        return self.page_rank(*args)

    def page_rank(self, *args):
        self.bytecode.add_step("pageRank", *args)
        return self

    def path(self, *args):
        self.bytecode.add_step("path", *args)
        return self

    def peerPressure(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.peerPressure will be replaced by "
            "gremlin_python.process.GraphTraversalSource.peer_pressure.",
            DeprecationWarning)
        return self.peer_pressure(*args)

    def peer_pressure(self, *args):
        self.bytecode.add_step("peerPressure", *args)
        return self

    def product(self, *args):
        self.bytecode.add_step("product", *args)
        return self

    def profile(self, *args):
        self.bytecode.add_step("profile", *args)
        return self

    def program(self, *args):
        self.bytecode.add_step("program", *args)
        return self

    def project(self, *args):
        self.bytecode.add_step("project", *args)
        return self

    def properties(self, *args):
        self.bytecode.add_step("properties", *args)
        return self

    def property(self, *args):
        self.bytecode.add_step("property", *args)
        return self

    def propertyMap(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.propertyMap will be replaced by "
            "gremlin_python.process.GraphTraversalSource.property_map.",
            DeprecationWarning)
        return self.property_map(*args)

    def property_map(self, *args):
        self.bytecode.add_step("propertyMap", *args)
        return self

    def range_(self, *args):
        self.bytecode.add_step("range", *args)
        return self

    def read(self, *args):
        self.bytecode.add_step("read", *args)
        return self

    def repeat(self, *args):
        self.bytecode.add_step("repeat", *args)
        return self

    def replace(self, *args):
        self.bytecode.add_step("replace", *args)
        return self

    def reverse(self, *args):
        self.bytecode.add_step("reverse", *args)
        return self

    def rTrim(self, *args):
        self.bytecode.add_step("rTrim", *args)
        return self

    def r_trim(self, *args):
        self.bytecode.add_step("rTrim", *args)
        return self

    def sack(self, *args):
        self.bytecode.add_step("sack", *args)
        return self

    def sample(self, *args):
        self.bytecode.add_step("sample", *args)
        return self

    def select(self, *args):
        self.bytecode.add_step("select", *args)
        return self

    def shortestPath(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.shortestPath will be replaced by "
            "gremlin_python.process.GraphTraversalSource.shortest_path.",
            DeprecationWarning)
        return self.shortest_path(*args)

    def shortest_path(self, *args):
        self.bytecode.add_step("shortestPath", *args)
        return self

    def sideEffect(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.sideEffect will be replaced by "
            "gremlin_python.process.GraphTraversalSource.side_effect.",
            DeprecationWarning)
        return self.side_effect(*args)

    def side_effect(self, *args):
        self.bytecode.add_step("sideEffect", *args)
        return self

    def simplePath(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.simplePath will be replaced by "
            "gremlin_python.process.GraphTraversalSource.simple_path.",
            DeprecationWarning)
        return self.simple_path(*args)

    def simple_path(self, *args):
        self.bytecode.add_step("simplePath", *args)
        return self

    def skip(self, *args):
        self.bytecode.add_step("skip", *args)
        return self

    def split(self, *args):
        self.bytecode.add_step("split", *args)
        return self

    def subgraph(self, *args):
        self.bytecode.add_step("subgraph", *args)
        return self

    def substring(self, *args):
        self.bytecode.add_step("substring", *args)
        return self

    def sum_(self, *args):
        self.bytecode.add_step("sum", *args)
        return self

    def tail(self, *args):
        self.bytecode.add_step("tail", *args)
        return self

    def timeLimit(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.timeLimit will be replaced by "
            "gremlin_python.process.GraphTraversalSource.time_limit.",
            DeprecationWarning)
        return self.time_limit(*args)

    def time_limit(self, *args):
        self.bytecode.add_step("timeLimit", *args)
        return self

    def times(self, *args):
        self.bytecode.add_step("times", *args)
        return self

    def to(self, *args):
        self.bytecode.add_step("to", *args)
        return self

    def toE(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.toE will be replaced by "
            "gremlin_python.process.GraphTraversalSource.to_e.",
            DeprecationWarning)
        return self.to_e(*args)

    def to_e(self, *args):
        self.bytecode.add_step("toE", *args)
        return self

    def to_lower(self, *args):
        self.bytecode.add_step("toLower", *args)
        return self

    def to_upper(self, *args):
        self.bytecode.add_step("toUpper", *args)
        return self

    def toV(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.toV will be replaced by "
            "gremlin_python.process.GraphTraversalSource.to_v.",
            DeprecationWarning)
        return self.to_v(*args)

    def to_v(self, *args):
        self.bytecode.add_step("toV", *args)
        return self

    def tree(self, *args):
        self.bytecode.add_step("tree", *args)
        return self

    def trim(self, *args):
        self.bytecode.add_step("trim", *args)
        return self

    def unfold(self, *args):
        self.bytecode.add_step("unfold", *args)
        return self

    def union(self, *args):
        self.bytecode.add_step("union", *args)
        return self

    def until(self, *args):
        self.bytecode.add_step("until", *args)
        return self

    def value(self, *args):
        self.bytecode.add_step("value", *args)
        return self

    def valueMap(self, *args):
        warnings.warn(
            "gremlin_python.process.GraphTraversalSource.valueMap will be replaced by "
            "gremlin_python.process.GraphTraversalSource.value_map.",
            DeprecationWarning)
        return self.value_map(*args)

    def value_map(self, *args):
        self.bytecode.add_step("valueMap", *args)
        return self

    def values(self, *args):
        self.bytecode.add_step("values", *args)
        return self

    def where(self, *args):
        self.bytecode.add_step("where", *args)
        return self

    def with_(self, *args):
        self.bytecode.add_step("with", *args)
        return self

    def write(self, *args):
        self.bytecode.add_step("write", *args)
        return self


class MagicType(type):

    def __getattr__(cls, k):
        if k.startswith('__'):
            raise AttributeError(
                'Python magic methods or keys starting with double underscore cannot be used for Gremlin sugar - prefer values(' + k + ')')
        return __.values(k)


class __(object, metaclass=MagicType):
    graph_traversal = GraphTraversal

    @classmethod
    def start(cls):
        return GraphTraversal(None, None, Bytecode())

    @classmethod
    def __(cls, *args):
        return __.inject(*args)

    @classmethod
    def E(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).E(*args)

    @classmethod
    def V(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).V(*args)

    @classmethod
    def addE(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.addE will be replaced by "
            "gremlin_python.process.__.add_e.",
            DeprecationWarning)
        return cls.add_e(*args)

    @classmethod
    def add_e(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).add_e(*args)

    @classmethod
    def addV(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.addV will be replaced by "
            "gremlin_python.process.__.add_v.",
            DeprecationWarning)
        return cls.add_v(*args)

    @classmethod
    def add_v(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).add_v(*args)

    @classmethod
    def aggregate(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).aggregate(*args)

    @classmethod
    def all_(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).all_(*args)

    @classmethod
    def and_(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).and_(*args)

    @classmethod
    def any_(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).any_(*args)

    @classmethod
    def as_(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).as_(*args)

    @classmethod
    def as_bool(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).as_bool(*args)

    @classmethod
    def as_date(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).as_date(*args)

    @classmethod
    def as_number(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).as_number(*args)

    @classmethod
    def as_string(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).as_string(*args)

    @classmethod
    def barrier(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).barrier(*args)

    @classmethod
    def both(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).both(*args)

    @classmethod
    def bothE(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.bothE will be replaced by "
            "gremlin_python.process.__.both_e.",
            DeprecationWarning)
        return cls.both_e(*args)

    @classmethod
    def both_e(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).both_e(*args)

    @classmethod
    def bothV(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.bothV will be replaced by "
            "gremlin_python.process.__.both_v.",
            DeprecationWarning)
        return cls.both_v(*args)

    @classmethod
    def both_v(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).both_v(*args)

    @classmethod
    def branch(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).branch(*args)

    @classmethod
    def call(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).call(*args)

    @classmethod
    def cap(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).cap(*args)

    @classmethod
    def choose(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).choose(*args)

    @classmethod
    def coalesce(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).coalesce(*args)

    @classmethod
    def coin(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).coin(*args)

    @classmethod
    def combine(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).combine(*args)

    @classmethod
    def concat(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).concat(*args)

    @classmethod
    def conjoin(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).conjoin(*args)

    @classmethod
    def constant(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).constant(*args)

    @classmethod
    def count(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).count(*args)

    @classmethod
    def cyclicPath(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.cyclicPath will be replaced by "
            "gremlin_python.process.__.cyclic_path.",
            DeprecationWarning)
        return cls.cyclic_path(*args)

    @classmethod
    def cyclic_path(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).cyclic_path(*args)

    @classmethod
    def date_add(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).date_add(*args)

    @classmethod
    def date_diff(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).date_diff(*args)

    @classmethod
    def dedup(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).dedup(*args)

    @classmethod
    def difference(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).difference(*args)

    @classmethod
    def discard(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).discard(*args)

    @classmethod
    def disjunct(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).disjunct(*args)

    @classmethod
    def drop(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).drop(*args)

    @classmethod
    def element(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).element(*args)

    @classmethod
    def elementMap(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.elementMap will be replaced by "
            "gremlin_python.process.__.element_map.",
            DeprecationWarning)
        return cls.element_map(*args)

    @classmethod
    def element_map(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).element_map(*args)

    @classmethod
    def emit(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).emit(*args)

    @classmethod
    def fail(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).fail(*args)

    @classmethod
    def filter_(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).filter_(*args)

    @classmethod
    def flatMap(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.flatMap will be replaced by "
            "gremlin_python.process.__.flat_map.",
            DeprecationWarning)
        return cls.flat_map(*args)

    @classmethod
    def flat_map(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).flat_map(*args)

    @classmethod
    def fold(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).fold(*args)

    @classmethod
    def format_(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).format_(*args)

    @classmethod
    def group(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).group(*args)

    @classmethod
    def groupCount(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.groupCount will be replaced by "
            "gremlin_python.process.__.group_count.",
            DeprecationWarning)
        return cls.group_count(*args)

    @classmethod
    def group_count(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).group_count(*args)

    @classmethod
    def has(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).has(*args)

    @classmethod
    def hasId(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.hasId will be replaced by "
            "gremlin_python.process.__.has_id.",
            DeprecationWarning)
        return cls.has_id(*args)

    @classmethod
    def has_id(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).has_id(*args)

    @classmethod
    def hasKey(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.hasKey will be replaced by "
            "gremlin_python.process.__.has_key.",
            DeprecationWarning)
        return cls.has_key(*args)

    @classmethod
    def has_key_(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.has_key_ will be replaced by "
            "gremlin_python.process.__.has_key.",
            DeprecationWarning)
        return cls.has_key(*args)
    
    @classmethod
    def has_key (cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).has_key(*args)

    @classmethod
    def hasLabel(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.hasLabel will be replaced by "
            "gremlin_python.process.__.has_label.",
            DeprecationWarning)
        return cls.has_label(*args)

    @classmethod
    def has_label(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).has_label(*args)

    @classmethod
    def hasNot(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.hasNot will be replaced by "
            "gremlin_python.process.__.has_not.",
            DeprecationWarning)
        return cls.has_not(*args)

    @classmethod
    def has_not(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).has_not(*args)

    @classmethod
    def hasValue(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.hasValue will be replaced by "
            "gremlin_python.process.__.has_value.",
            DeprecationWarning)
        return cls.has_value(*args)

    @classmethod
    def has_value(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).has_value(*args)

    @classmethod
    def id_(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).id_(*args)

    @classmethod
    def identity(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).identity(*args)

    @classmethod
    def inE(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.inE will be replaced by "
            "gremlin_python.process.__.in_e.",
            DeprecationWarning)
        return cls.in_e(*args)

    @classmethod
    def in_e(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).in_e(*args)

    @classmethod
    def inV(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.inV will be replaced by "
            "gremlin_python.process.__.in_v.",
            DeprecationWarning)
        return cls.in_v(*args)

    @classmethod
    def in_v(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).in_v(*args)

    @classmethod
    def in_(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).in_(*args)

    @classmethod
    def index(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).index(*args)

    @classmethod
    def inject(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).inject(*args)

    @classmethod
    def intersect(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).intersect(*args)

    @classmethod
    def is_(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).is_(*args)

    @classmethod
    def key(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).key(*args)

    @classmethod
    def label(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).label(*args)

    @classmethod
    def length(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).length(*args)

    @classmethod
    def limit(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).limit(*args)

    @classmethod
    def local(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).local(*args)

    @classmethod
    def loops(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).loops(*args)

    @classmethod
    def ltrim(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.ltrim will be replaced by "
            "gremlin_python.process.__.l_trim.",
            DeprecationWarning)
        return cls.l_trim(*args)

    @classmethod
    def l_trim(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).l_trim(*args)

    @classmethod
    def map(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).map(*args)

    @classmethod
    def match(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).match(*args)

    @classmethod
    def math(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).math(*args)

    @classmethod
    def max_(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).max_(*args)

    @classmethod
    def mean(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).mean(*args)

    @classmethod
    def merge(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).merge(*args)

    @classmethod
    def merge_e(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).merge_e(*args)

    @classmethod
    def merge_v(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).merge_v(*args)

    @classmethod
    def min_(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).min_(*args)

    @classmethod
    def none(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).none(*args)

    @classmethod
    def not_(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).not_(*args)

    @classmethod
    def optional(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).optional(*args)

    @classmethod
    def or_(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).or_(*args)

    @classmethod
    def order(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).order(*args)

    @classmethod
    def otherV(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.otherV will be replaced by "
            "gremlin_python.process.__.other_v.",
            DeprecationWarning)
        return cls.other_v(*args)

    @classmethod
    def other_v(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).other_v(*args)

    @classmethod
    def out(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).out(*args)

    @classmethod
    def outE(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.outE will be replaced by "
            "gremlin_python.process.__.out_e.",
            DeprecationWarning)
        return cls.out_e(*args)

    @classmethod
    def out_e(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).out_e(*args)

    @classmethod
    def outV(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.outV will be replaced by "
            "gremlin_python.process.__.out_v.",
            DeprecationWarning)
        return cls.out_v(*args)

    @classmethod
    def out_v(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).out_v(*args)

    @classmethod
    def path(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).path(*args)

    @classmethod
    def product(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).product(*args)

    @classmethod
    def project(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).project(*args)

    @classmethod
    def properties(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).properties(*args)

    @classmethod
    def property(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).property(*args)

    @classmethod
    def propertyMap(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.propertyMap will be replaced by "
            "gremlin_python.process.__.property_map.",
            DeprecationWarning)
        return cls.property_map(*args)

    @classmethod
    def property_map(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).property_map(*args)

    @classmethod
    def range_(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).range_(*args)

    @classmethod
    def repeat(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).repeat(*args)

    @classmethod
    def replace(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).replace(*args)

    @classmethod
    def reverse(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).reverse(*args)

    @classmethod
    def rTrim(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.rTrim will be replaced by "
            "gremlin_python.process.__.r_trim.",
            DeprecationWarning)
        return cls.r_trim(*args)

    @classmethod
    def r_trim(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).r_trim(*args)

    @classmethod
    def sack(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).sack(*args)

    @classmethod
    def sample(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).sample(*args)

    @classmethod
    def select(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).select(*args)

    @classmethod
    def sideEffect(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.sideEffect will be replaced by "
            "gremlin_python.process.__.side_effect.",
            DeprecationWarning)
        return cls.side_effect(*args)

    @classmethod
    def side_effect(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).side_effect(*args)

    @classmethod
    def simplePath(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.simplePath will be replaced by "
            "gremlin_python.process.__.simple_path.",
            DeprecationWarning)
        return cls.simple_path(*args)

    @classmethod
    def simple_path(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).simple_path(*args)

    @classmethod
    def skip(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).skip(*args)

    @classmethod
    def split(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).split(*args)

    @classmethod
    def subgraph(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).subgraph(*args)

    @classmethod
    def substring(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).substring(*args)

    @classmethod
    def sum_(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).sum_(*args)

    @classmethod
    def tail(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).tail(*args)

    @classmethod
    def timeLimit(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.timeLimit will be replaced by "
            "gremlin_python.process.__.time_limit.",
            DeprecationWarning)
        return cls.time_limit(*args)

    @classmethod
    def time_limit(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).time_limit(*args)

    @classmethod
    def times(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).times(*args)

    @classmethod
    def to(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).to(*args)

    @classmethod
    def toE(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.toE will be replaced by "
            "gremlin_python.process.__.to_e.",
            DeprecationWarning)
        return cls.to_e(*args)

    @classmethod
    def to_e(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).to_e(*args)

    @classmethod
    def to_lower(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).to_lower(*args)

    @classmethod
    def to_upper(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).to_upper(*args)

    @classmethod
    def toV(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.toV will be replaced by "
            "gremlin_python.process.__.to_v.",
            DeprecationWarning)
        return cls.to_v(*args)

    @classmethod
    def to_v(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).to_v(*args)

    @classmethod
    def tree(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).tree(*args)

    @classmethod
    def trim(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).trim(*args)

    @classmethod
    def unfold(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).unfold(*args)

    @classmethod
    def union(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).union(*args)

    @classmethod
    def until(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).until(*args)

    @classmethod
    def value(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).value(*args)

    @classmethod
    def valueMap(cls, *args):
        warnings.warn(
            "gremlin_python.process.__.valueMap will be replaced by "
            "gremlin_python.process.__.value_map.",
            DeprecationWarning)
        return cls.value_map(*args)

    @classmethod
    def value_map(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).value_map(*args)

    @classmethod
    def values(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).values(*args)

    @classmethod
    def where(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).where(*args)


# Class to handle transactions.
class Transaction:

    def __init__(self, g, remote_connection):
        self._g = g
        self._session_based_connection = None
        self._remote_connection = remote_connection
        self.__is_open = False
        self.__mutex = Lock()

    # Begins transaction.
    def begin(self):
        with self.__mutex:
            # Verify transaction is not open.
            self.__verify_transaction_state(False, "Transaction already started on this object")

            # Create new session using the remote connection.
            self._session_based_connection = self._remote_connection.create_session()
            self.__is_open = True

            # Set the session as a remote strategy within the traversal strategy.
            traversal_strategy = TraversalStrategies()
            traversal_strategy.add_strategies([RemoteStrategy(self._session_based_connection)])

            # Return new GraphTraversalSource.
            return GraphTraversalSource(self._g.graph, traversal_strategy, self._g.bytecode)

    # Rolls transaction back.
    def rollback(self):
        with self.__mutex:
            # Verify transaction is open, close session and return result of transaction's rollback.
            self.__verify_transaction_state(True, "Cannot rollback a transaction that is not started.")
            return self.__close_session(self._session_based_connection.rollback())

    # Commits the current transaction.
    def commit(self):
        with self.__mutex:
            # Verify transaction is open, close session and return result of transaction's commit.
            self.__verify_transaction_state(True, "Cannot commit a transaction that is not started.")
            return self.__close_session(self._session_based_connection.commit())

    # Closes session.
    def close(self):
        with self.__mutex:
            # Verify transaction is open.
            self.__verify_transaction_state(True, "Cannot close a transaction that has previously been closed.")
            self.__close_session(None)

    # Return whether or not transaction is open.
    # Allow camelcase function here to keep api consistent with other languages.
    def isOpen(self):
        warnings.warn(
            "gremlin_python.process.Transaction.isOpen will be replaced by "
            "gremlin_python.process.Transaction.is_open.",
            DeprecationWarning)
        return self.is_open()

    def is_open(self):
        # if the underlying DriverRemoteConnection is closed then the Transaction can't be open
        if (self._session_based_connection and self._session_based_connection.is_closed()) or \
                self._remote_connection.is_closed():
            self.__is_open = False

        return self.__is_open

    def __verify_transaction_state(self, state, error_message):
        if self.__is_open != state:
            raise Exception(error_message)

    def __close_session(self, session):
        self.__is_open = False
        self._remote_connection.remove_session(self._session_based_connection)
        return session


def E(*args):
    return __.E(*args)


def V(*args):
    return __.V(*args)


def addE(*args):
    return __.add_e(*args)


def add_e(*args):
    return __.add_e(*args)


def addV(*args):
    return __.add_v(*args)


def add_v(*args):
    return __.add_v(*args)


def aggregate(*args):
    return __.aggregate(*args)


def all_(*args):
    return __.all_(*args)


def and_(*args):
    return __.and_(*args)


def any_(*args):
    return __.any_(*args)


def as_(*args):
    return __.as_(*args)


def as_bool(*args):
    return __.as_bool(*args)


def as_date(*args):
    return __.as_date(*args)


def as_number(*args):
    return __.as_number(*args)


def as_string(*args):
    return __.as_string(*args)


def barrier(*args):
    return __.barrier(*args)


def both(*args):
    return __.both(*args)


def bothE(*args):
    return __.both_e(*args)


def both_e(*args):
    return __.both_e(*args)


def bothV(*args):
    return __.both_v(*args)


def both_v(*args):
    return __.both_v(*args)


def branch(*args):
    return __.branch(*args)


def call(*args):
    return __.call(*args)


def cap(*args):
    return __.cap(*args)


def choose(*args):
    return __.choose(*args)


def coalesce(*args):
    return __.coalesce(*args)


def coin(*args):
    return __.coin(*args)


def combine(*args):
    return __.combine(*args)


def concat(*args):
    return __.concat(*args)


def conjoin(*args):
    return __.conjoin(*args)


def constant(*args):
    return __.constant(*args)


def count(*args):
    return __.count(*args)


def cyclicPath(*args):
    return __.cyclic_path(*args)


def cyclic_path(*args):
    return __.cyclic_path(*args)


def date_add(*args):
    return __.date_add(*args)


def date_diff(*args):
    return __.date_diff(*args)


def dedup(*args):
    return __.dedup(*args)


def difference(*args):
    return __.difference(*args)


def discard(*args):
    return __.discard(*args)


def disjunct(*args):
    return __.disjunct(*args)


def drop(*args):
    return __.drop(*args)


def element(*args):
    return __.element(*args)


def elementMap(*args):
    return __.element_map(*args)


def element_map(*args):
    return __.element_map(*args)


def emit(*args):
    return __.emit(*args)


def fail(*args):
    return __.fail(*args)


def filter_(*args):
    return __.filter_(*args)


def flatMap(*args):
    return __.flat_map(*args)


def flat_map(*args):
    return __.flat_map(*args)


def fold(*args):
    return __.fold(*args)


def format_(*args):
    return __.format_(*args)


def group(*args):
    return __.group(*args)


def groupCount(*args):
    return __.group_count(*args)


def group_count(*args):
    return __.group_count(*args)


def has(*args):
    return __.has(*args)


def hasId(*args):
    return __.has_id(*args)


def has_id(*args):
    return __.has_id(*args)


def hasKey(*args):
    return __.has_key(*args)


def has_key_(*args):
    return __.has_key(*args)

def has_key(*args):
    return __.has_key(*args)

def hasLabel(*args):
    return __.has_label(*args)


def has_label(*args):
    return __.has_label(*args)


def hasNot(*args):
    return __.has_not(*args)


def has_not(*args):
    return __.has_not(*args)


def hasValue(*args):
    return __.has_value(*args)


def has_value(*args):
    return __.has_value(*args)


def id_(*args):
    return __.id_(*args)


def identity(*args):
    return __.identity(*args)


def inE(*args):
    return __.in_e(*args)


def in_e(*args):
    return __.in_e(*args)


def inV(*args):
    return __.in_v(*args)


def in_v(*args):
    return __.in_v(*args)


def in_(*args):
    return __.in_(*args)


def index(*args):
    return __.index(*args)


def inject(*args):
    return __.inject(*args)


def intersect(*args):
    return __.intersect(*args)


def is_(*args):
    return __.is_(*args)


def key(*args):
    return __.key(*args)


def label(*args):
    return __.label(*args)


def length(*args):
    return __.length(*args)


def limit(*args):
    return __.limit(*args)


def local(*args):
    return __.local(*args)


def loops(*args):
    return __.loops(*args)


def ltrim(*args):
    return __.l_trim(*args)


def l_trim(*args):
    return __.l_trim(*args)


def map(*args):
    return __.map(*args)


def match(*args):
    return __.match(*args)


def math(*args):
    return __.math(*args)


def max_(*args):
    return __.max_(*args)


def mean(*args):
    return __.mean(*args)


def merge(*args):
    return __.merge(*args)


def merge_e(*args):
    return __.merge_e(*args)


def merge_v(*args):
    return __.merge_v(*args)


def min_(*args):
    return __.min_(*args)


def none(*args):
    return __.none(*args)


def not_(*args):
    return __.not_(*args)


def optional(*args):
    return __.optional(*args)


def or_(*args):
    return __.or_(*args)


def order(*args):
    return __.order(*args)


def otherV(*args):
    return __.other_v(*args)


def other_v(*args):
    return __.other_v(*args)


def out(*args):
    return __.out(*args)


def outE(*args):
    return __.out_e(*args)


def out_e(*args):
    return __.out_e(*args)


def outV(*args):
    return __.out_v(*args)


def out_v(*args):
    return __.out_v(*args)


def path(*args):
    return __.path(*args)


def product(*args):
    return __.product(*args)


def project(*args):
    return __.project(*args)


def properties(*args):
    return __.properties(*args)


def property(*args):
    return __.property(*args)


def propertyMap(*args):
    return __.property_map(*args)


def property_map(*args):
    return __.property_map(*args)


def range_(*args):
    return __.range_(*args)


def repeat(*args):
    return __.repeat(*args)


def replace(*args):
    return __.replace(*args)


def reverse(*args):
    return __.reverse(*args)


def rTrim(*args):
    return __.r_trim(*args)


def r_trim(*args):
    return __.r_trim(*args)


def sack(*args):
    return __.sack(*args)


def sample(*args):
    return __.sample(*args)


def select(*args):
    return __.select(*args)


def sideEffect(*args):
    return __.side_effect(*args)


def side_effect(*args):
    return __.side_effect(*args)


def simplePath(*args):
    return __.simple_path(*args)


def simple_path(*args):
    return __.simple_path(*args)


def skip(*args):
    return __.skip(*args)


def split(*args):
    return __.split(*args)


def subgraph(*args):
    return __.subgraph(*args)


def substring(*args):
    return __.substring(*args)


def sum_(*args):
    return __.sum_(*args)


def tail(*args):
    return __.tail(*args)


def timeLimit(*args):
    return __.time_limit(*args)


def time_limit(*args):
    return __.time_limit(*args)


def times(*args):
    return __.times(*args)


def to(*args):
    return __.to(*args)


def toE(*args):
    return __.to_e(*args)


def to_e(*args):
    return __.to_e(*args)


def to_lower(*args):
    return __.to_lower(*args)


def to_upper(*args):
    return __.to_upper(*args)


def toV(*args):
    return __.to_v(*args)


def to_v(*args):
    return __.to_v(*args)


def tree(*args):
    return __.tree(*args)


def trim(*args):
    return __.trim(*args)


def unfold(*args):
    return __.unfold(*args)


def union(*args):
    return __.union(*args)


def until(*args):
    return __.until(*args)


def value(*args):
    return __.value(*args)


def valueMap(*args):
    return __.value_map(*args)


def value_map(*args):
    return __.value_map(*args)


def values(*args):
    return __.values(*args)


def where(*args):
    return __.where(*args)

statics.add_static('E', E)

statics.add_static('V', V)

statics.add_static('addE', addE)

statics.add_static('add_E', add_e)

statics.add_static('addV', addV)

statics.add_static('add_v', add_v)

statics.add_static('aggregate', aggregate)

statics.add_static('all_', all_)

statics.add_static('and_', and_)

statics.add_static('any_', any_)

statics.add_static('as_', as_)

statics.add_static('as_bool', as_bool)

statics.add_static('as_date', as_date)

statics.add_static('as_number', as_number)

statics.add_static('as_string', as_string)

statics.add_static('barrier', barrier)

statics.add_static('both', both)

statics.add_static('bothE', bothE)

statics.add_static('both_e', both_e)

statics.add_static('bothV', bothV)

statics.add_static('both_v', both_v)

statics.add_static('branch', branch)

statics.add_static('call', call)

statics.add_static('cap', cap)

statics.add_static('choose', choose)

statics.add_static('coalesce', coalesce)

statics.add_static('coin', coin)

statics.add_static('combine', combine)

statics.add_static('concat', concat)

statics.add_static('conjoin', conjoin)

statics.add_static('constant', constant)

statics.add_static('count', count)

statics.add_static('cyclicPath', cyclicPath)

statics.add_static('cyclicpath', cyclic_path)

statics.add_static('date_add', date_add)

statics.add_static('date_diff', date_diff)

statics.add_static('dedup', dedup)

statics.add_static('difference', difference)

statics.add_static('discard', discard)

statics.add_static('disjunct', disjunct)

statics.add_static('drop', drop)

statics.add_static('element', element)

statics.add_static('elementMap', elementMap)

statics.add_static('element_map', element_map)

statics.add_static('emit', emit)

statics.add_static('fail', fail)

statics.add_static('filter_', filter_)

statics.add_static('flatMap', flatMap)

statics.add_static('flat_map', flat_map)

statics.add_static('fold', fold)

statics.add_static('format_', format_)

statics.add_static('group', group)

statics.add_static('groupCount', groupCount)

statics.add_static('group_count', group_count)

statics.add_static('has', has)

statics.add_static('hasId', hasId)

statics.add_static('has_id', has_id)

statics.add_static('hasKey', hasKey)

statics.add_static('has_key', has_key)

statics.add_static('hasLabel', hasLabel)

statics.add_static('has_label', has_label)

statics.add_static('hasNot', hasNot)

statics.add_static('has_not', has_not)

statics.add_static('hasValue', hasValue)

statics.add_static('has_value', has_value)

statics.add_static('id_', id_)

statics.add_static('identity', identity)

statics.add_static('inE', inE)

statics.add_static('in_e', in_e)

statics.add_static('in_v', in_v)

statics.add_static('in_', in_)

statics.add_static('index', index)

statics.add_static('inject', inject)

statics.add_static('intersect', intersect)

statics.add_static('is_', is_)

statics.add_static('key', key)

statics.add_static('label', label)

statics.add_static('length', length)

statics.add_static('limit', limit)

statics.add_static('local', local)

statics.add_static('loops', loops)

statics.add_static('ltrim', ltrim)

statics.add_static('l_trim', l_trim)

statics.add_static('map', map)

statics.add_static('match', match)

statics.add_static('math', math)

statics.add_static('max_', max_)

statics.add_static('mean', mean)

statics.add_static('merge', merge)

statics.add_static('merge_e', merge_e)

statics.add_static('merge_v', merge_v)

statics.add_static('min_', min_)

statics.add_static('none', none)

statics.add_static('not_', not_)

statics.add_static('optional', optional)

statics.add_static('or_', or_)

statics.add_static('order', order)

statics.add_static('otherV', otherV)

statics.add_static('other_v', other_v)

statics.add_static('out', out)

statics.add_static('outE', outE)

statics.add_static('out_e', out_e)

statics.add_static('outV', outV)

statics.add_static('out_v', out_v)

statics.add_static('path', path)

statics.add_static('product', product)

statics.add_static('project', project)

statics.add_static('properties', properties)

statics.add_static('property', property)

statics.add_static('propertyMap', propertyMap)

statics.add_static('property_map', property_map)

statics.add_static('range_', range_)

statics.add_static('repeat', repeat)

statics.add_static('replace', replace)

statics.add_static('reverse', reverse)

statics.add_static('rTrim', rTrim)

statics.add_static('r_trim', r_trim)

statics.add_static('sack', sack)

statics.add_static('sample', sample)

statics.add_static('select', select)

statics.add_static('sideEffect', sideEffect)

statics.add_static('side_effect', side_effect)

statics.add_static('simplePath', simplePath)

statics.add_static('simple_path', simple_path)

statics.add_static('skip', skip)

statics.add_static('split', split)

statics.add_static('subgraph', subgraph)

statics.add_static('substring', substring)

statics.add_static('sum_', sum_)

statics.add_static('tail', tail)

statics.add_static('timeLimit', timeLimit)

statics.add_static('time_limit', time_limit)

statics.add_static('times', times)

statics.add_static('to', to)

statics.add_static('toE', toE)

statics.add_static('to_e', to_e)

statics.add_static('toV', toV)

statics.add_static('to_lower', to_lower)

statics.add_static('to_upper', to_upper)

statics.add_static('to_v', to_v)

statics.add_static('tree', tree)

statics.add_static('trim', trim)

statics.add_static('unfold', unfold)

statics.add_static('union', union)

statics.add_static('until', until)

statics.add_static('value', value)

statics.add_static('valueMap', valueMap)

statics.add_static('value_map', value_map)

statics.add_static('values', values)

statics.add_static('where', where)
