'''
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

from org.apache.tinkerpop.gremlin.process.traversal import *
from org.apache.tinkerpop.gremlin.structure import *
from org.apache.tinkerpop.gremlin.process.traversal.dsl.graph import __ as anon

class JythonGraphTraversalSource(object):
  def __init__(self, traversalSource):
    self.traversalSource = traversalSource
  def __repr__(self):
    return self.traversalSource.toString()
  def toString(self, *args):
    return self.traversalSource.toString(*args)
  def clone(self, *args):
    self.traversalSource = self.traversalSource.clone(*args)
    return self
  def V(self, *args):
    return JythonGraphTraversal(self.traversalSource.V(*args))
  def E(self, *args):
    return JythonGraphTraversal(self.traversalSource.E(*args))
  def build(self, *args):
    return self.traversalSource.build(*args)
  def withSack(self, *args):
    self.traversalSource = self.traversalSource.withSack(*args)
    return self
  def computer(self, *args):
    return self.traversalSource.computer(*args)
  def withComputer(self, *args):
    self.traversalSource = self.traversalSource.withComputer(*args)
    return self
  def withStrategies(self, *args):
    self.traversalSource = self.traversalSource.withStrategies(*args)
    return self
  def withoutStrategies(self, *args):
    self.traversalSource = self.traversalSource.withoutStrategies(*args)
    return self
  def withSideEffect(self, *args):
    self.traversalSource = self.traversalSource.withSideEffect(*args)
    return self
  def withBulk(self, *args):
    self.traversalSource = self.traversalSource.withBulk(*args)
    return self
  def withPath(self, *args):
    self.traversalSource = self.traversalSource.withPath(*args)
    return self
  def standard(self, *args):
    return self.traversalSource.standard(*args)
  def inject(self, *args):
    return JythonGraphTraversal(self.traversalSource.inject(*args))
  def getStrategies(self, *args):
    return self.traversalSource.getStrategies(*args)
  def addV(self, *args):
    return JythonGraphTraversal(self.traversalSource.addV(*args))
  def getGraph(self, *args):
    return self.traversalSource.getGraph(*args)
  def tx(self, *args):
    return self.traversalSource.tx(*args)
  def wait(self, *args):
    return self.traversalSource.wait(*args)
  def equals(self, *args):
    return self.traversalSource.equals(*args)
  def hashCode(self, *args):
    return self.traversalSource.hashCode(*args)
  def getClass(self, *args):
    return self.traversalSource.getClass(*args)
  def notify(self, *args):
    return self.traversalSource.notify(*args)
  def notifyAll(self, *args):
    return self.traversalSource.notifyAll(*args)


class JythonGraphTraversal(object):
  def __init__(self, traversal):
    self.traversal = traversal
  def __repr__(self):
    return self.traversal.toString()
  def __getitem__(self,index):
    if type(index) is int:
      self.traversal = self.traversal.range(index,index+1)
    elif type(index) is slice:
        self.traversal = self.traversal.range(index.start,index.stop)
    else:
        raise TypeError("index must be int or slice")
    return self
  def __getattr__(self,key):
    return self.values(key)
  def group(self, *args):
    self.traversal = self.traversal.group(*args)
    return self
  def limit(self, *args):
    self.traversal = self.traversal.limit(*args)
    return self
  def value(self, *args):
    self.traversal = self.traversal.value(*args)
    return self
  def count(self, *args):
    self.traversal = self.traversal.count(*args)
    return self
  def profile(self, *args):
    self.traversal = self.traversal.profile(*args)
    return self
  def values(self, *args):
    self.traversal = self.traversal.values(*args)
    return self
  def min(self, *args):
    self.traversal = self.traversal.min(*args)
    return self
  def max(self, *args):
    self.traversal = self.traversal.max(*args)
    return self
  def V(self, *args):
    self.traversal = self.traversal.V(*args)
    return self
  def identity(self, *args):
    self.traversal = self.traversal.identity(*args)
    return self
  def _in(self, *args):
    return self.traversal.in(*args)
  def out(self, *args):
    self.traversal = self.traversal.out(*args)
    return self
  def key(self, *args):
    self.traversal = self.traversal.key(*args)
    return self
  def store(self, *args):
    self.traversal = self.traversal.store(*args)
    return self
  def path(self, *args):
    self.traversal = self.traversal.path(*args)
    return self
  def sum(self, *args):
    self.traversal = self.traversal.sum(*args)
    return self
  def toV(self, *args):
    self.traversal = self.traversal.toV(*args)
    return self
  def filter(self, *args):
    self.traversal = self.traversal.filter(*args)
    return self
  def tree(self, *args):
    self.traversal = self.traversal.tree(*args)
    return self
  def match(self, *args):
    self.traversal = self.traversal.match(*args)
    return self
  def range(self, *args):
    self.traversal = self.traversal.range(*args)
    return self
  def order(self, *args):
    self.traversal = self.traversal.order(*args)
    return self
  def map(self, *args):
    self.traversal = self.traversal.map(*args)
    return self
  def tail(self, *args):
    self.traversal = self.traversal.tail(*args)
    return self
  def _and(self, *args):
    return self.traversal.and(*args)
  def _or(self, *args):
    return self.traversal.or(*args)
  def id(self, *args):
    self.traversal = self.traversal.id(*args)
    return self
  def option(self, *args):
    self.traversal = self.traversal.option(*args)
    return self
  def _not(self, *args):
    return self.traversal.not(*args)
  def property(self, *args):
    self.traversal = self.traversal.property(*args)
    return self
  def program(self, *args):
    self.traversal = self.traversal.program(*args)
    return self
  def label(self, *args):
    self.traversal = self.traversal.label(*args)
    return self
  def choose(self, *args):
    self.traversal = self.traversal.choose(*args)
    return self
  def propertyMap(self, *args):
    self.traversal = self.traversal.propertyMap(*args)
    return self
  def inject(self, *args):
    self.traversal = self.traversal.inject(*args)
    return self
  def drop(self, *args):
    self.traversal = self.traversal.drop(*args)
    return self
  def times(self, *args):
    self.traversal = self.traversal.times(*args)
    return self
  def select(self, *args):
    self.traversal = self.traversal.select(*args)
    return self
  def _as(self, *args):
    return self.traversal.as(*args)
  def outE(self, *args):
    self.traversal = self.traversal.outE(*args)
    return self
  def inE(self, *args):
    self.traversal = self.traversal.inE(*args)
    return self
  def toE(self, *args):
    self.traversal = self.traversal.toE(*args)
    return self
  def bothE(self, *args):
    self.traversal = self.traversal.bothE(*args)
    return self
  def inV(self, *args):
    self.traversal = self.traversal.inV(*args)
    return self
  def outV(self, *args):
    self.traversal = self.traversal.outV(*args)
    return self
  def both(self, *args):
    self.traversal = self.traversal.both(*args)
    return self
  def bothV(self, *args):
    self.traversal = self.traversal.bothV(*args)
    return self
  def otherV(self, *args):
    self.traversal = self.traversal.otherV(*args)
    return self
  def valueMap(self, *args):
    self.traversal = self.traversal.valueMap(*args)
    return self
  def mapValues(self, *args):
    self.traversal = self.traversal.mapValues(*args)
    return self
  def mapKeys(self, *args):
    self.traversal = self.traversal.mapKeys(*args)
    return self
  def sack(self, *args):
    self.traversal = self.traversal.sack(*args)
    return self
  def loops(self, *args):
    self.traversal = self.traversal.loops(*args)
    return self
  def project(self, *args):
    self.traversal = self.traversal.project(*args)
    return self
  def unfold(self, *args):
    self.traversal = self.traversal.unfold(*args)
    return self
  def fold(self, *args):
    self.traversal = self.traversal.fold(*args)
    return self
  def mean(self, *args):
    self.traversal = self.traversal.mean(*args)
    return self
  def groupV3d0(self, *args):
    self.traversal = self.traversal.groupV3d0(*args)
    return self
  def addV(self, *args):
    self.traversal = self.traversal.addV(*args)
    return self
  def addE(self, *args):
    self.traversal = self.traversal.addE(*args)
    return self
  def addOutE(self, *args):
    self.traversal = self.traversal.addOutE(*args)
    return self
  def addInE(self, *args):
    self.traversal = self.traversal.addInE(*args)
    return self
  def dedup(self, *args):
    self.traversal = self.traversal.dedup(*args)
    return self
  def where(self, *args):
    self.traversal = self.traversal.where(*args)
    return self
  def hasNot(self, *args):
    self.traversal = self.traversal.hasNot(*args)
    return self
  def hasLabel(self, *args):
    self.traversal = self.traversal.hasLabel(*args)
    return self
  def hasId(self, *args):
    self.traversal = self.traversal.hasId(*args)
    return self
  def hasKey(self, *args):
    self.traversal = self.traversal.hasKey(*args)
    return self
  def hasValue(self, *args):
    self.traversal = self.traversal.hasValue(*args)
    return self
  def coin(self, *args):
    self.traversal = self.traversal.coin(*args)
    return self
  def timeLimit(self, *args):
    self.traversal = self.traversal.timeLimit(*args)
    return self
  def simplePath(self, *args):
    self.traversal = self.traversal.simplePath(*args)
    return self
  def cyclicPath(self, *args):
    self.traversal = self.traversal.cyclicPath(*args)
    return self
  def sample(self, *args):
    self.traversal = self.traversal.sample(*args)
    return self
  def sideEffect(self, *args):
    self.traversal = self.traversal.sideEffect(*args)
    return self
  def subgraph(self, *args):
    self.traversal = self.traversal.subgraph(*args)
    return self
  def aggregate(self, *args):
    self.traversal = self.traversal.aggregate(*args)
    return self
  def branch(self, *args):
    self.traversal = self.traversal.branch(*args)
    return self
  def optional(self, *args):
    self.traversal = self.traversal.optional(*args)
    return self
  def coalesce(self, *args):
    self.traversal = self.traversal.coalesce(*args)
    return self
  def repeat(self, *args):
    self.traversal = self.traversal.repeat(*args)
    return self
  def emit(self, *args):
    self.traversal = self.traversal.emit(*args)
    return self
  def local(self, *args):
    self.traversal = self.traversal.local(*args)
    return self
  def pageRank(self, *args):
    self.traversal = self.traversal.pageRank(*args)
    return self
  def peerPressure(self, *args):
    self.traversal = self.traversal.peerPressure(*args)
    return self
  def barrier(self, *args):
    self.traversal = self.traversal.barrier(*args)
    return self
  def by(self, *args):
    self.traversal = self.traversal.by(*args)
    return self
  def _is(self, *args):
    return self.traversal.is(*args)
  def until(self, *args):
    self.traversal = self.traversal.until(*args)
    return self
  def constant(self, *args):
    self.traversal = self.traversal.constant(*args)
    return self
  def properties(self, *args):
    self.traversal = self.traversal.properties(*args)
    return self
  def to(self, *args):
    self.traversal = self.traversal.to(*args)
    return self
  def _from(self, *args):
    return self.traversal.from(*args)
  def has(self, *args):
    self.traversal = self.traversal.has(*args)
    return self
  def union(self, *args):
    self.traversal = self.traversal.union(*args)
    return self
  def groupCount(self, *args):
    self.traversal = self.traversal.groupCount(*args)
    return self
  def flatMap(self, *args):
    self.traversal = self.traversal.flatMap(*args)
    return self
  def iterate(self, *args):
    self.traversal = self.traversal.iterate(*args)
    return self
  def cap(self, *args):
    self.traversal = self.traversal.cap(*args)
    return self
  def asAdmin(self, *args):
    self.traversal = self.traversal.asAdmin(*args)
    return self
  def next(self, *args):
    return self.traversal.next(*args)
  def fill(self, *args):
    return self.traversal.fill(*args)
  def forEachRemaining(self, *args):
    return self.traversal.forEachRemaining(*args)
  def toList(self, *args):
    return self.traversal.toList(*args)
  def toSet(self, *args):
    return self.traversal.toSet(*args)
  def toBulkSet(self, *args):
    return self.traversal.toBulkSet(*args)
  def tryNext(self, *args):
    return self.traversal.tryNext(*args)
  def toStream(self, *args):
    return self.traversal.toStream(*args)
  def explain(self, *args):
    return self.traversal.explain(*args)
  def remove(self, *args):
    return self.traversal.remove(*args)
  def hasNext(self, *args):
    return self.traversal.hasNext(*args)


class __(object):
  @staticmethod
  def group(*args):
    return anon.group(*args)
  @staticmethod
  def limit(*args):
    return anon.limit(*args)
  @staticmethod
  def value(*args):
    return anon.value(*args)
  @staticmethod
  def count(*args):
    return anon.count(*args)
  @staticmethod
  def values(*args):
    return anon.values(*args)
  @staticmethod
  def min(*args):
    return anon.min(*args)
  @staticmethod
  def max(*args):
    return anon.max(*args)
  @staticmethod
  def V(*args):
    return anon.V(*args)
  @staticmethod
  def identity(*args):
    return anon.identity(*args)
  @staticmethod
  def _in(*args):
    return anon.in(*args)
  @staticmethod
  def out(*args):
    return anon.out(*args)
  @staticmethod
  def key(*args):
    return anon.key(*args)
  @staticmethod
  def start(*args):
    return anon.start(*args)
  @staticmethod
  def store(*args):
    return anon.store(*args)
  @staticmethod
  def path(*args):
    return anon.path(*args)
  @staticmethod
  def sum(*args):
    return anon.sum(*args)
  @staticmethod
  def toV(*args):
    return anon.toV(*args)
  @staticmethod
  def filter(*args):
    return anon.filter(*args)
  @staticmethod
  def tree(*args):
    return anon.tree(*args)
  @staticmethod
  def match(*args):
    return anon.match(*args)
  @staticmethod
  def range(*args):
    return anon.range(*args)
  @staticmethod
  def order(*args):
    return anon.order(*args)
  @staticmethod
  def map(*args):
    return anon.map(*args)
  @staticmethod
  def tail(*args):
    return anon.tail(*args)
  @staticmethod
  def _and(*args):
    return anon.and(*args)
  @staticmethod
  def _or(*args):
    return anon.or(*args)
  @staticmethod
  def id(*args):
    return anon.id(*args)
  @staticmethod
  def _not(*args):
    return anon.not(*args)
  @staticmethod
  def property(*args):
    return anon.property(*args)
  @staticmethod
  def label(*args):
    return anon.label(*args)
  @staticmethod
  def choose(*args):
    return anon.choose(*args)
  @staticmethod
  def propertyMap(*args):
    return anon.propertyMap(*args)
  @staticmethod
  def inject(*args):
    return anon.inject(*args)
  @staticmethod
  def drop(*args):
    return anon.drop(*args)
  @staticmethod
  def times(*args):
    return anon.times(*args)
  @staticmethod
  def select(*args):
    return anon.select(*args)
  @staticmethod
  def _as(*args):
    return anon.as(*args)
  @staticmethod
  def outE(*args):
    return anon.outE(*args)
  @staticmethod
  def inE(*args):
    return anon.inE(*args)
  @staticmethod
  def toE(*args):
    return anon.toE(*args)
  @staticmethod
  def bothE(*args):
    return anon.bothE(*args)
  @staticmethod
  def inV(*args):
    return anon.inV(*args)
  @staticmethod
  def outV(*args):
    return anon.outV(*args)
  @staticmethod
  def both(*args):
    return anon.both(*args)
  @staticmethod
  def bothV(*args):
    return anon.bothV(*args)
  @staticmethod
  def otherV(*args):
    return anon.otherV(*args)
  @staticmethod
  def valueMap(*args):
    return anon.valueMap(*args)
  @staticmethod
  def mapValues(*args):
    return anon.mapValues(*args)
  @staticmethod
  def mapKeys(*args):
    return anon.mapKeys(*args)
  @staticmethod
  def sack(*args):
    return anon.sack(*args)
  @staticmethod
  def loops(*args):
    return anon.loops(*args)
  @staticmethod
  def project(*args):
    return anon.project(*args)
  @staticmethod
  def unfold(*args):
    return anon.unfold(*args)
  @staticmethod
  def fold(*args):
    return anon.fold(*args)
  @staticmethod
  def mean(*args):
    return anon.mean(*args)
  @staticmethod
  def groupV3d0(*args):
    return anon.groupV3d0(*args)
  @staticmethod
  def addV(*args):
    return anon.addV(*args)
  @staticmethod
  def addE(*args):
    return anon.addE(*args)
  @staticmethod
  def addOutE(*args):
    return anon.addOutE(*args)
  @staticmethod
  def addInE(*args):
    return anon.addInE(*args)
  @staticmethod
  def dedup(*args):
    return anon.dedup(*args)
  @staticmethod
  def where(*args):
    return anon.where(*args)
  @staticmethod
  def hasNot(*args):
    return anon.hasNot(*args)
  @staticmethod
  def hasLabel(*args):
    return anon.hasLabel(*args)
  @staticmethod
  def hasId(*args):
    return anon.hasId(*args)
  @staticmethod
  def hasKey(*args):
    return anon.hasKey(*args)
  @staticmethod
  def hasValue(*args):
    return anon.hasValue(*args)
  @staticmethod
  def coin(*args):
    return anon.coin(*args)
  @staticmethod
  def timeLimit(*args):
    return anon.timeLimit(*args)
  @staticmethod
  def simplePath(*args):
    return anon.simplePath(*args)
  @staticmethod
  def cyclicPath(*args):
    return anon.cyclicPath(*args)
  @staticmethod
  def sample(*args):
    return anon.sample(*args)
  @staticmethod
  def sideEffect(*args):
    return anon.sideEffect(*args)
  @staticmethod
  def subgraph(*args):
    return anon.subgraph(*args)
  @staticmethod
  def aggregate(*args):
    return anon.aggregate(*args)
  @staticmethod
  def branch(*args):
    return anon.branch(*args)
  @staticmethod
  def optional(*args):
    return anon.optional(*args)
  @staticmethod
  def coalesce(*args):
    return anon.coalesce(*args)
  @staticmethod
  def repeat(*args):
    return anon.repeat(*args)
  @staticmethod
  def emit(*args):
    return anon.emit(*args)
  @staticmethod
  def local(*args):
    return anon.local(*args)
  @staticmethod
  def barrier(*args):
    return anon.barrier(*args)
  @staticmethod
  def _is(*args):
    return anon.is(*args)
  @staticmethod
  def until(*args):
    return anon.until(*args)
  @staticmethod
  def __(*args):
    return anon.__(*args)
  @staticmethod
  def constant(*args):
    return anon.constant(*args)
  @staticmethod
  def properties(*args):
    return anon.properties(*args)
  @staticmethod
  def to(*args):
    return anon.to(*args)
  @staticmethod
  def has(*args):
    return anon.has(*args)
  @staticmethod
  def union(*args):
    return anon.union(*args)
  @staticmethod
  def groupCount(*args):
    return anon.groupCount(*args)
  @staticmethod
  def flatMap(*args):
    return anon.flatMap(*args)
  @staticmethod
  def cap(*args):
    return anon.cap(*args)
  @staticmethod
  def wait(*args):
    return anon.wait(*args)
  @staticmethod
  def equals(*args):
    return anon.equals(*args)
  @staticmethod
  def toString(*args):
    return anon.toString(*args)
  @staticmethod
  def hashCode(*args):
    return anon.hashCode(*args)
  @staticmethod
  def getClass(*args):
    return anon.getClass(*args)
  @staticmethod
  def notify(*args):
    return anon.notify(*args)
  @staticmethod
  def notifyAll(*args):
    return anon.notifyAll(*args)


