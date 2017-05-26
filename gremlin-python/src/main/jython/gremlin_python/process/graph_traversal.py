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

import sys
from .traversal import Traversal
from .traversal import TraversalStrategies
from .strategies import VertexProgramStrategy
from .traversal import Bytecode
from ..driver.remote_connection import RemoteStrategy
from .. import statics
from ..statics import long

class GraphTraversalSource(object):
    def __init__(self, graph, traversal_strategies, bytecode=None):
        self.graph = graph
        self.traversal_strategies = traversal_strategies
        if bytecode is None:
          bytecode = Bytecode()
        self.bytecode = bytecode
        self.graph_traversal = GraphTraversal
    def __repr__(self):
        return "graphtraversalsource[" + str(self.graph) + "]"
    def get_graph_traversal_source(self):
        return self.__class__(self.graph, TraversalStrategies(self.traversal_strategies), Bytecode(self.bytecode))
    def get_graph_traversal(self):
        return self.graph_traversal(self.graph, self.traversal_strategies, Bytecode(self.bytecode))


    def withBulk(self, *args):
        source = self.get_graph_traversal_source()
        source.bytecode.add_source("withBulk", *args)
        return source

    def withPath(self, *args):
        source = self.get_graph_traversal_source()
        source.bytecode.add_source("withPath", *args)
        return source

    def withSack(self, *args):
        source = self.get_graph_traversal_source()
        source.bytecode.add_source("withSack", *args)
        return source

    def withSideEffect(self, *args):
        source = self.get_graph_traversal_source()
        source.bytecode.add_source("withSideEffect", *args)
        return source

    def withStrategies(self, *args):
        source = self.get_graph_traversal_source()
        source.bytecode.add_source("withStrategies", *args)
        return source

    def withoutStrategies(self, *args):
        source = self.get_graph_traversal_source()
        source.bytecode.add_source("withoutStrategies", *args)
        return source


    def withRemote(self, remote_connection):
        source = self.get_graph_traversal_source()
        source.traversal_strategies.add_strategies([RemoteStrategy(remote_connection)])
        return source
    def withComputer(self,graph_computer=None, workers=None, result=None, persist=None, vertices=None, edges=None, configuration=None):
        return self.withStrategies(VertexProgramStrategy(graph_computer,workers,result,persist,vertices,edges,configuration))


    def E(self, *args):
        traversal = self.get_graph_traversal()
        traversal.bytecode.add_step("E", *args)
        return traversal

    def V(self, *args):
        traversal = self.get_graph_traversal()
        traversal.bytecode.add_step("V", *args)
        return traversal

    def addV(self, *args):
        traversal = self.get_graph_traversal()
        traversal.bytecode.add_step("addV", *args)
        return traversal

    def inject(self, *args):
        traversal = self.get_graph_traversal()
        traversal.bytecode.add_step("inject", *args)
        return traversal


class GraphTraversal(Traversal):
    def __init__(self, graph, traversal_strategies, bytecode):
        super(GraphTraversal, self).__init__(graph, traversal_strategies, bytecode)
    def __getitem__(self, index):
        if isinstance(index, int):
            return self.range(long(index), long(index + 1))
        elif isinstance(index, slice):
            return self.range(long(0) if index.start is None else long(index.start), long(sys.maxsize) if index.stop is None else long(index.stop))
        else:
            raise TypeError("Index must be int or slice")
    def __getattr__(self, key):
        return self.values(key)

    def V(self, *args):
        self.bytecode.add_step("V", *args)
        return self

    def addE(self, *args):
        self.bytecode.add_step("addE", *args)
        return self

    def addInE(self, *args):
        self.bytecode.add_step("addInE", *args)
        return self

    def addOutE(self, *args):
        self.bytecode.add_step("addOutE", *args)
        return self

    def addV(self, *args):
        self.bytecode.add_step("addV", *args)
        return self

    def aggregate(self, *args):
        self.bytecode.add_step("aggregate", *args)
        return self

    def and_(self, *args):
        self.bytecode.add_step("and", *args)
        return self

    def as_(self, *args):
        self.bytecode.add_step("as", *args)
        return self

    def barrier(self, *args):
        self.bytecode.add_step("barrier", *args)
        return self

    def both(self, *args):
        self.bytecode.add_step("both", *args)
        return self

    def bothE(self, *args):
        self.bytecode.add_step("bothE", *args)
        return self

    def bothV(self, *args):
        self.bytecode.add_step("bothV", *args)
        return self

    def branch(self, *args):
        self.bytecode.add_step("branch", *args)
        return self

    def by(self, *args):
        self.bytecode.add_step("by", *args)
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

    def constant(self, *args):
        self.bytecode.add_step("constant", *args)
        return self

    def count(self, *args):
        self.bytecode.add_step("count", *args)
        return self

    def cyclicPath(self, *args):
        self.bytecode.add_step("cyclicPath", *args)
        return self

    def dedup(self, *args):
        self.bytecode.add_step("dedup", *args)
        return self

    def drop(self, *args):
        self.bytecode.add_step("drop", *args)
        return self

    def emit(self, *args):
        self.bytecode.add_step("emit", *args)
        return self

    def filter(self, *args):
        self.bytecode.add_step("filter", *args)
        return self

    def flatMap(self, *args):
        self.bytecode.add_step("flatMap", *args)
        return self

    def fold(self, *args):
        self.bytecode.add_step("fold", *args)
        return self

    def from_(self, *args):
        self.bytecode.add_step("from", *args)
        return self

    def group(self, *args):
        self.bytecode.add_step("group", *args)
        return self

    def groupCount(self, *args):
        self.bytecode.add_step("groupCount", *args)
        return self

    def groupV3d0(self, *args):
        self.bytecode.add_step("groupV3d0", *args)
        return self

    def has(self, *args):
        self.bytecode.add_step("has", *args)
        return self

    def hasId(self, *args):
        self.bytecode.add_step("hasId", *args)
        return self

    def hasKey(self, *args):
        self.bytecode.add_step("hasKey", *args)
        return self

    def hasLabel(self, *args):
        self.bytecode.add_step("hasLabel", *args)
        return self

    def hasNot(self, *args):
        self.bytecode.add_step("hasNot", *args)
        return self

    def hasValue(self, *args):
        self.bytecode.add_step("hasValue", *args)
        return self

    def id(self, *args):
        self.bytecode.add_step("id", *args)
        return self

    def identity(self, *args):
        self.bytecode.add_step("identity", *args)
        return self

    def inE(self, *args):
        self.bytecode.add_step("inE", *args)
        return self

    def inV(self, *args):
        self.bytecode.add_step("inV", *args)
        return self

    def in_(self, *args):
        self.bytecode.add_step("in", *args)
        return self

    def inject(self, *args):
        self.bytecode.add_step("inject", *args)
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

    def limit(self, *args):
        self.bytecode.add_step("limit", *args)
        return self

    def local(self, *args):
        self.bytecode.add_step("local", *args)
        return self

    def loops(self, *args):
        self.bytecode.add_step("loops", *args)
        return self

    def map(self, *args):
        self.bytecode.add_step("map", *args)
        return self

    def mapKeys(self, *args):
        self.bytecode.add_step("mapKeys", *args)
        return self

    def mapValues(self, *args):
        self.bytecode.add_step("mapValues", *args)
        return self

    def match(self, *args):
        self.bytecode.add_step("match", *args)
        return self

    def max(self, *args):
        self.bytecode.add_step("max", *args)
        return self

    def mean(self, *args):
        self.bytecode.add_step("mean", *args)
        return self

    def min(self, *args):
        self.bytecode.add_step("min", *args)
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
        self.bytecode.add_step("otherV", *args)
        return self

    def out(self, *args):
        self.bytecode.add_step("out", *args)
        return self

    def outE(self, *args):
        self.bytecode.add_step("outE", *args)
        return self

    def outV(self, *args):
        self.bytecode.add_step("outV", *args)
        return self

    def pageRank(self, *args):
        self.bytecode.add_step("pageRank", *args)
        return self

    def path(self, *args):
        self.bytecode.add_step("path", *args)
        return self

    def peerPressure(self, *args):
        self.bytecode.add_step("peerPressure", *args)
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
        self.bytecode.add_step("propertyMap", *args)
        return self

    def range(self, *args):
        self.bytecode.add_step("range", *args)
        return self

    def repeat(self, *args):
        self.bytecode.add_step("repeat", *args)
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

    def sideEffect(self, *args):
        self.bytecode.add_step("sideEffect", *args)
        return self

    def simplePath(self, *args):
        self.bytecode.add_step("simplePath", *args)
        return self

    def store(self, *args):
        self.bytecode.add_step("store", *args)
        return self

    def subgraph(self, *args):
        self.bytecode.add_step("subgraph", *args)
        return self

    def sum(self, *args):
        self.bytecode.add_step("sum", *args)
        return self

    def tail(self, *args):
        self.bytecode.add_step("tail", *args)
        return self

    def timeLimit(self, *args):
        self.bytecode.add_step("timeLimit", *args)
        return self

    def times(self, *args):
        self.bytecode.add_step("times", *args)
        return self

    def to(self, *args):
        self.bytecode.add_step("to", *args)
        return self

    def toE(self, *args):
        self.bytecode.add_step("toE", *args)
        return self

    def toV(self, *args):
        self.bytecode.add_step("toV", *args)
        return self

    def tree(self, *args):
        self.bytecode.add_step("tree", *args)
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
        self.bytecode.add_step("valueMap", *args)
        return self

    def values(self, *args):
        self.bytecode.add_step("values", *args)
        return self

    def where(self, *args):
        self.bytecode.add_step("where", *args)
        return self


class __(object):
    graph_traversal = GraphTraversal
    @classmethod
    def start(cls):
        return GraphTraversal(None, None, Bytecode())
    @classmethod
    def __(cls, *args):
        return __.inject(*args)


    @classmethod
    def V(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).V(*args)

    @classmethod
    def addE(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).addE(*args)

    @classmethod
    def addInE(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).addInE(*args)

    @classmethod
    def addOutE(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).addOutE(*args)

    @classmethod
    def addV(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).addV(*args)

    @classmethod
    def aggregate(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).aggregate(*args)

    @classmethod
    def and_(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).and_(*args)

    @classmethod
    def as_(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).as_(*args)

    @classmethod
    def barrier(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).barrier(*args)

    @classmethod
    def both(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).both(*args)

    @classmethod
    def bothE(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).bothE(*args)

    @classmethod
    def bothV(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).bothV(*args)

    @classmethod
    def branch(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).branch(*args)

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
    def constant(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).constant(*args)

    @classmethod
    def count(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).count(*args)

    @classmethod
    def cyclicPath(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).cyclicPath(*args)

    @classmethod
    def dedup(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).dedup(*args)

    @classmethod
    def drop(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).drop(*args)

    @classmethod
    def emit(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).emit(*args)

    @classmethod
    def filter(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).filter(*args)

    @classmethod
    def flatMap(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).flatMap(*args)

    @classmethod
    def fold(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).fold(*args)

    @classmethod
    def group(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).group(*args)

    @classmethod
    def groupCount(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).groupCount(*args)

    @classmethod
    def groupV3d0(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).groupV3d0(*args)

    @classmethod
    def has(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).has(*args)

    @classmethod
    def hasId(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).hasId(*args)

    @classmethod
    def hasKey(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).hasKey(*args)

    @classmethod
    def hasLabel(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).hasLabel(*args)

    @classmethod
    def hasNot(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).hasNot(*args)

    @classmethod
    def hasValue(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).hasValue(*args)

    @classmethod
    def id(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).id(*args)

    @classmethod
    def identity(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).identity(*args)

    @classmethod
    def inE(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).inE(*args)

    @classmethod
    def inV(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).inV(*args)

    @classmethod
    def in_(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).in_(*args)

    @classmethod
    def inject(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).inject(*args)

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
    def limit(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).limit(*args)

    @classmethod
    def local(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).local(*args)

    @classmethod
    def loops(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).loops(*args)

    @classmethod
    def map(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).map(*args)

    @classmethod
    def mapKeys(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).mapKeys(*args)

    @classmethod
    def mapValues(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).mapValues(*args)

    @classmethod
    def match(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).match(*args)

    @classmethod
    def max(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).max(*args)

    @classmethod
    def mean(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).mean(*args)

    @classmethod
    def min(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).min(*args)

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
        return cls.graph_traversal(None, None, Bytecode()).otherV(*args)

    @classmethod
    def out(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).out(*args)

    @classmethod
    def outE(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).outE(*args)

    @classmethod
    def outV(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).outV(*args)

    @classmethod
    def path(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).path(*args)

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
        return cls.graph_traversal(None, None, Bytecode()).propertyMap(*args)

    @classmethod
    def range(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).range(*args)

    @classmethod
    def repeat(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).repeat(*args)

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
        return cls.graph_traversal(None, None, Bytecode()).sideEffect(*args)

    @classmethod
    def simplePath(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).simplePath(*args)

    @classmethod
    def store(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).store(*args)

    @classmethod
    def subgraph(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).subgraph(*args)

    @classmethod
    def sum(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).sum(*args)

    @classmethod
    def tail(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).tail(*args)

    @classmethod
    def timeLimit(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).timeLimit(*args)

    @classmethod
    def times(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).times(*args)

    @classmethod
    def to(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).to(*args)

    @classmethod
    def toE(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).toE(*args)

    @classmethod
    def toV(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).toV(*args)

    @classmethod
    def tree(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).tree(*args)

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
        return cls.graph_traversal(None, None, Bytecode()).valueMap(*args)

    @classmethod
    def values(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).values(*args)

    @classmethod
    def where(cls, *args):
        return cls.graph_traversal(None, None, Bytecode()).where(*args)



def V(*args):
    return __.V(*args)
statics.add_static('V', V)

def addE(*args):
    return __.addE(*args)
statics.add_static('addE', addE)

def addInE(*args):
    return __.addInE(*args)
statics.add_static('addInE', addInE)

def addOutE(*args):
    return __.addOutE(*args)
statics.add_static('addOutE', addOutE)

def addV(*args):
    return __.addV(*args)
statics.add_static('addV', addV)

def aggregate(*args):
    return __.aggregate(*args)
statics.add_static('aggregate', aggregate)

def and_(*args):
    return __.and_(*args)
statics.add_static('and_', and_)

def as_(*args):
    return __.as_(*args)
statics.add_static('as_', as_)

def barrier(*args):
    return __.barrier(*args)
statics.add_static('barrier', barrier)

def both(*args):
    return __.both(*args)
statics.add_static('both', both)

def bothE(*args):
    return __.bothE(*args)
statics.add_static('bothE', bothE)

def bothV(*args):
    return __.bothV(*args)
statics.add_static('bothV', bothV)

def branch(*args):
    return __.branch(*args)
statics.add_static('branch', branch)

def cap(*args):
    return __.cap(*args)
statics.add_static('cap', cap)

def choose(*args):
    return __.choose(*args)
statics.add_static('choose', choose)

def coalesce(*args):
    return __.coalesce(*args)
statics.add_static('coalesce', coalesce)

def coin(*args):
    return __.coin(*args)
statics.add_static('coin', coin)

def constant(*args):
    return __.constant(*args)
statics.add_static('constant', constant)

def count(*args):
    return __.count(*args)
statics.add_static('count', count)

def cyclicPath(*args):
    return __.cyclicPath(*args)
statics.add_static('cyclicPath', cyclicPath)

def dedup(*args):
    return __.dedup(*args)
statics.add_static('dedup', dedup)

def drop(*args):
    return __.drop(*args)
statics.add_static('drop', drop)

def emit(*args):
    return __.emit(*args)
statics.add_static('emit', emit)

def filter(*args):
    return __.filter(*args)
statics.add_static('filter', filter)

def flatMap(*args):
    return __.flatMap(*args)
statics.add_static('flatMap', flatMap)

def fold(*args):
    return __.fold(*args)
statics.add_static('fold', fold)

def group(*args):
    return __.group(*args)
statics.add_static('group', group)

def groupCount(*args):
    return __.groupCount(*args)
statics.add_static('groupCount', groupCount)

def groupV3d0(*args):
    return __.groupV3d0(*args)
statics.add_static('groupV3d0', groupV3d0)

def has(*args):
    return __.has(*args)
statics.add_static('has', has)

def hasId(*args):
    return __.hasId(*args)
statics.add_static('hasId', hasId)

def hasKey(*args):
    return __.hasKey(*args)
statics.add_static('hasKey', hasKey)

def hasLabel(*args):
    return __.hasLabel(*args)
statics.add_static('hasLabel', hasLabel)

def hasNot(*args):
    return __.hasNot(*args)
statics.add_static('hasNot', hasNot)

def hasValue(*args):
    return __.hasValue(*args)
statics.add_static('hasValue', hasValue)

def id(*args):
    return __.id(*args)
statics.add_static('id', id)

def identity(*args):
    return __.identity(*args)
statics.add_static('identity', identity)

def inE(*args):
    return __.inE(*args)
statics.add_static('inE', inE)

def inV(*args):
    return __.inV(*args)
statics.add_static('inV', inV)

def in_(*args):
    return __.in_(*args)
statics.add_static('in_', in_)

def inject(*args):
    return __.inject(*args)
statics.add_static('inject', inject)

def is_(*args):
    return __.is_(*args)
statics.add_static('is_', is_)

def key(*args):
    return __.key(*args)
statics.add_static('key', key)

def label(*args):
    return __.label(*args)
statics.add_static('label', label)

def limit(*args):
    return __.limit(*args)
statics.add_static('limit', limit)

def local(*args):
    return __.local(*args)
statics.add_static('local', local)

def loops(*args):
    return __.loops(*args)
statics.add_static('loops', loops)

def map(*args):
    return __.map(*args)
statics.add_static('map', map)

def mapKeys(*args):
    return __.mapKeys(*args)
statics.add_static('mapKeys', mapKeys)

def mapValues(*args):
    return __.mapValues(*args)
statics.add_static('mapValues', mapValues)

def match(*args):
    return __.match(*args)
statics.add_static('match', match)

def max(*args):
    return __.max(*args)
statics.add_static('max', max)

def mean(*args):
    return __.mean(*args)
statics.add_static('mean', mean)

def min(*args):
    return __.min(*args)
statics.add_static('min', min)

def not_(*args):
    return __.not_(*args)
statics.add_static('not_', not_)

def optional(*args):
    return __.optional(*args)
statics.add_static('optional', optional)

def or_(*args):
    return __.or_(*args)
statics.add_static('or_', or_)

def order(*args):
    return __.order(*args)
statics.add_static('order', order)

def otherV(*args):
    return __.otherV(*args)
statics.add_static('otherV', otherV)

def out(*args):
    return __.out(*args)
statics.add_static('out', out)

def outE(*args):
    return __.outE(*args)
statics.add_static('outE', outE)

def outV(*args):
    return __.outV(*args)
statics.add_static('outV', outV)

def path(*args):
    return __.path(*args)
statics.add_static('path', path)

def project(*args):
    return __.project(*args)
statics.add_static('project', project)

def properties(*args):
    return __.properties(*args)
statics.add_static('properties', properties)

def property(*args):
    return __.property(*args)
statics.add_static('property', property)

def propertyMap(*args):
    return __.propertyMap(*args)
statics.add_static('propertyMap', propertyMap)

def range(*args):
    return __.range(*args)
statics.add_static('range', range)

def repeat(*args):
    return __.repeat(*args)
statics.add_static('repeat', repeat)

def sack(*args):
    return __.sack(*args)
statics.add_static('sack', sack)

def sample(*args):
    return __.sample(*args)
statics.add_static('sample', sample)

def select(*args):
    return __.select(*args)
statics.add_static('select', select)

def sideEffect(*args):
    return __.sideEffect(*args)
statics.add_static('sideEffect', sideEffect)

def simplePath(*args):
    return __.simplePath(*args)
statics.add_static('simplePath', simplePath)

def store(*args):
    return __.store(*args)
statics.add_static('store', store)

def subgraph(*args):
    return __.subgraph(*args)
statics.add_static('subgraph', subgraph)

def sum(*args):
    return __.sum(*args)
statics.add_static('sum', sum)

def tail(*args):
    return __.tail(*args)
statics.add_static('tail', tail)

def timeLimit(*args):
    return __.timeLimit(*args)
statics.add_static('timeLimit', timeLimit)

def times(*args):
    return __.times(*args)
statics.add_static('times', times)

def to(*args):
    return __.to(*args)
statics.add_static('to', to)

def toE(*args):
    return __.toE(*args)
statics.add_static('toE', toE)

def toV(*args):
    return __.toV(*args)
statics.add_static('toV', toV)

def tree(*args):
    return __.tree(*args)
statics.add_static('tree', tree)

def unfold(*args):
    return __.unfold(*args)
statics.add_static('unfold', unfold)

def union(*args):
    return __.union(*args)
statics.add_static('union', union)

def until(*args):
    return __.until(*args)
statics.add_static('until', until)

def value(*args):
    return __.value(*args)
statics.add_static('value', value)

def valueMap(*args):
    return __.valueMap(*args)
statics.add_static('valueMap', valueMap)

def values(*args):
    return __.values(*args)
statics.add_static('values', values)

def where(*args):
    return __.where(*args)
statics.add_static('where', where)
