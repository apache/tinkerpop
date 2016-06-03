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
from collections import OrderedDict

statics = OrderedDict()


class Helper(object):
  @staticmethod
  def stringOrObject(arg):
    if (type(arg) is str and
      not(arg.startswith("SackFunctions.Barrier.")) and
      not(arg.startswith("VertexProperty.Cardinality.")) and
      not(arg.startswith("Column.")) and
      not(arg.startswith("Computer.")) and
      not(arg.startswith("Direction.")) and
      not(arg.startswith("Operator.")) and
      not(arg.startswith("Order.")) and
      not(arg.startswith("P.")) and
      not(arg.startswith("Pop.")) and
      not(arg.startswith("ReadOnlyStrategy.")) and
      not(arg.startswith("Scope.")) and
      not(arg.startswith("T.")) and
      not(len(arg)==0)):
         return "\"" + arg + "\""
    elif type(arg) is bool:
      return str(arg).lower()
    elif type(arg) is long:
      return str(arg) + "L"
    elif type(arg) is float:
      return str(arg) + "f"
    else:
      return str(arg)
  @staticmethod
  def stringify(*args):
    if len(args) == 0:
      return ""
    elif len(args) == 1:
      return Helper.stringOrObject(args[0])
    else:
      return ", ".join(Helper.stringOrObject(i) for i in args)


class PythonGraphTraversalSource(object):
  def __init__(self, traversalSourceString, remoteConnection):
    self.traversalSourceString = traversalSourceString
    self.remoteConnection = remoteConnection
  def __repr__(self):
    if self.remoteConnection is None:
      return "graphtraversalsource[no connection, " + self.traversalSourceString + "]"
    else:
      return "graphtraversalsource[" + str(self.remoteConnection) + ", " + self.traversalSourceString + "]"
  def E(self, *args):
    return PythonGraphTraversal(self.traversalSourceString + ".E(" + Helper.stringify(*args) + ")", self.remoteConnection)
  def V(self, *args):
    return PythonGraphTraversal(self.traversalSourceString + ".V(" + Helper.stringify(*args) + ")", self.remoteConnection)
  def addV(self, *args):
    return PythonGraphTraversal(self.traversalSourceString + ".addV(" + Helper.stringify(*args) + ")", self.remoteConnection)
  def clone(self, *args):
    return PythonGraphTraversalSource(self.traversalSourceString + ".clone(" + Helper.stringify(*args) + ")", self.remoteConnection)
  def inject(self, *args):
    return PythonGraphTraversal(self.traversalSourceString + ".inject(" + Helper.stringify(*args) + ")", self.remoteConnection)
  def withBulk(self, *args):
    return PythonGraphTraversalSource(self.traversalSourceString + ".withBulk(" + Helper.stringify(*args) + ")", self.remoteConnection)
  def withComputer(self, *args):
    return PythonGraphTraversalSource(self.traversalSourceString + ".withComputer(" + Helper.stringify(*args) + ")", self.remoteConnection)
  def withPath(self, *args):
    return PythonGraphTraversalSource(self.traversalSourceString + ".withPath(" + Helper.stringify(*args) + ")", self.remoteConnection)
  def withSack(self, *args):
    return PythonGraphTraversalSource(self.traversalSourceString + ".withSack(" + Helper.stringify(*args) + ")", self.remoteConnection)
  def withSideEffect(self, *args):
    return PythonGraphTraversalSource(self.traversalSourceString + ".withSideEffect(" + Helper.stringify(*args) + ")", self.remoteConnection)
  def withStrategies(self, *args):
    return PythonGraphTraversalSource(self.traversalSourceString + ".withStrategies(" + Helper.stringify(*args) + ")", self.remoteConnection)
  def withoutStrategies(self, *args):
    return PythonGraphTraversalSource(self.traversalSourceString + ".withoutStrategies(" + Helper.stringify(*args) + ")", self.remoteConnection)


class PythonGraphTraversal(object):
  def __init__(self, traversalString, remoteConnection=None):
    self.traversalString = traversalString
    self.remoteConnection = remoteConnection
    self.results = None
    self.lastTraverser = None
  def __repr__(self):
    return self.traversalString
  def __getitem__(self,index):
    if type(index) is int:
      return self.range(index,index+1)
    elif type(index) is slice:
      return self.range(index.start,index.stop)
    else:
      raise TypeError("Index must be int or slice")
  def __getattr__(self,key):
    return self.values(key)
  def __iter__(self):
        return self
  def toList(self):
    return list(iter(self))
  def next(self):
     if self.results is None:
        self.results = self.remoteConnection.submit(self.traversalString)
     if self.lastTraverser is None:
         self.lastTraverser = self.results.next()
     object = self.lastTraverser.object
     self.lastTraverser.bulk = self.lastTraverser.bulk - 1
     if self.lastTraverser.bulk <= 0:
         self.lastTraverser = None
     return object
  def V(self, *args):
    self.traversalString = self.traversalString + ".V(" + Helper.stringify(*args) + ")"
    return self
  def _and(self, *args):
    self.traversalString = self.traversalString + ".and(" + Helper.stringify(*args) + ")"
    return self
  def _as(self, *args):
    self.traversalString = self.traversalString + ".as(" + Helper.stringify(*args) + ")"
    return self
  def _from(self, *args):
    self.traversalString = self.traversalString + ".from(" + Helper.stringify(*args) + ")"
    return self
  def _in(self, *args):
    self.traversalString = self.traversalString + ".in(" + Helper.stringify(*args) + ")"
    return self
  def _is(self, *args):
    self.traversalString = self.traversalString + ".is(" + Helper.stringify(*args) + ")"
    return self
  def _not(self, *args):
    self.traversalString = self.traversalString + ".not(" + Helper.stringify(*args) + ")"
    return self
  def _or(self, *args):
    self.traversalString = self.traversalString + ".or(" + Helper.stringify(*args) + ")"
    return self
  def addE(self, *args):
    self.traversalString = self.traversalString + ".addE(" + Helper.stringify(*args) + ")"
    return self
  def addInE(self, *args):
    self.traversalString = self.traversalString + ".addInE(" + Helper.stringify(*args) + ")"
    return self
  def addOutE(self, *args):
    self.traversalString = self.traversalString + ".addOutE(" + Helper.stringify(*args) + ")"
    return self
  def addV(self, *args):
    self.traversalString = self.traversalString + ".addV(" + Helper.stringify(*args) + ")"
    return self
  def aggregate(self, *args):
    self.traversalString = self.traversalString + ".aggregate(" + Helper.stringify(*args) + ")"
    return self
  def asAdmin(self, *args):
    self.traversalString = self.traversalString + ".asAdmin(" + Helper.stringify(*args) + ")"
    return self
  def barrier(self, *args):
    self.traversalString = self.traversalString + ".barrier(" + Helper.stringify(*args) + ")"
    return self
  def both(self, *args):
    self.traversalString = self.traversalString + ".both(" + Helper.stringify(*args) + ")"
    return self
  def bothE(self, *args):
    self.traversalString = self.traversalString + ".bothE(" + Helper.stringify(*args) + ")"
    return self
  def bothV(self, *args):
    self.traversalString = self.traversalString + ".bothV(" + Helper.stringify(*args) + ")"
    return self
  def branch(self, *args):
    self.traversalString = self.traversalString + ".branch(" + Helper.stringify(*args) + ")"
    return self
  def by(self, *args):
    self.traversalString = self.traversalString + ".by(" + Helper.stringify(*args) + ")"
    return self
  def cap(self, *args):
    self.traversalString = self.traversalString + ".cap(" + Helper.stringify(*args) + ")"
    return self
  def choose(self, *args):
    self.traversalString = self.traversalString + ".choose(" + Helper.stringify(*args) + ")"
    return self
  def coalesce(self, *args):
    self.traversalString = self.traversalString + ".coalesce(" + Helper.stringify(*args) + ")"
    return self
  def coin(self, *args):
    self.traversalString = self.traversalString + ".coin(" + Helper.stringify(*args) + ")"
    return self
  def constant(self, *args):
    self.traversalString = self.traversalString + ".constant(" + Helper.stringify(*args) + ")"
    return self
  def count(self, *args):
    self.traversalString = self.traversalString + ".count(" + Helper.stringify(*args) + ")"
    return self
  def cyclicPath(self, *args):
    self.traversalString = self.traversalString + ".cyclicPath(" + Helper.stringify(*args) + ")"
    return self
  def dedup(self, *args):
    self.traversalString = self.traversalString + ".dedup(" + Helper.stringify(*args) + ")"
    return self
  def drop(self, *args):
    self.traversalString = self.traversalString + ".drop(" + Helper.stringify(*args) + ")"
    return self
  def emit(self, *args):
    self.traversalString = self.traversalString + ".emit(" + Helper.stringify(*args) + ")"
    return self
  def filter(self, *args):
    self.traversalString = self.traversalString + ".filter(" + Helper.stringify(*args) + ")"
    return self
  def flatMap(self, *args):
    self.traversalString = self.traversalString + ".flatMap(" + Helper.stringify(*args) + ")"
    return self
  def fold(self, *args):
    self.traversalString = self.traversalString + ".fold(" + Helper.stringify(*args) + ")"
    return self
  def group(self, *args):
    self.traversalString = self.traversalString + ".group(" + Helper.stringify(*args) + ")"
    return self
  def groupCount(self, *args):
    self.traversalString = self.traversalString + ".groupCount(" + Helper.stringify(*args) + ")"
    return self
  def groupV3d0(self, *args):
    self.traversalString = self.traversalString + ".groupV3d0(" + Helper.stringify(*args) + ")"
    return self
  def has(self, *args):
    self.traversalString = self.traversalString + ".has(" + Helper.stringify(*args) + ")"
    return self
  def hasId(self, *args):
    self.traversalString = self.traversalString + ".hasId(" + Helper.stringify(*args) + ")"
    return self
  def hasKey(self, *args):
    self.traversalString = self.traversalString + ".hasKey(" + Helper.stringify(*args) + ")"
    return self
  def hasLabel(self, *args):
    self.traversalString = self.traversalString + ".hasLabel(" + Helper.stringify(*args) + ")"
    return self
  def hasNot(self, *args):
    self.traversalString = self.traversalString + ".hasNot(" + Helper.stringify(*args) + ")"
    return self
  def hasValue(self, *args):
    self.traversalString = self.traversalString + ".hasValue(" + Helper.stringify(*args) + ")"
    return self
  def id(self, *args):
    self.traversalString = self.traversalString + ".id(" + Helper.stringify(*args) + ")"
    return self
  def identity(self, *args):
    self.traversalString = self.traversalString + ".identity(" + Helper.stringify(*args) + ")"
    return self
  def inE(self, *args):
    self.traversalString = self.traversalString + ".inE(" + Helper.stringify(*args) + ")"
    return self
  def inV(self, *args):
    self.traversalString = self.traversalString + ".inV(" + Helper.stringify(*args) + ")"
    return self
  def inject(self, *args):
    self.traversalString = self.traversalString + ".inject(" + Helper.stringify(*args) + ")"
    return self
  def iterate(self, *args):
    self.traversalString = self.traversalString + ".iterate(" + Helper.stringify(*args) + ")"
    return self
  def key(self, *args):
    self.traversalString = self.traversalString + ".key(" + Helper.stringify(*args) + ")"
    return self
  def label(self, *args):
    self.traversalString = self.traversalString + ".label(" + Helper.stringify(*args) + ")"
    return self
  def limit(self, *args):
    self.traversalString = self.traversalString + ".limit(" + Helper.stringify(*args) + ")"
    return self
  def local(self, *args):
    self.traversalString = self.traversalString + ".local(" + Helper.stringify(*args) + ")"
    return self
  def loops(self, *args):
    self.traversalString = self.traversalString + ".loops(" + Helper.stringify(*args) + ")"
    return self
  def map(self, *args):
    self.traversalString = self.traversalString + ".map(" + Helper.stringify(*args) + ")"
    return self
  def mapKeys(self, *args):
    self.traversalString = self.traversalString + ".mapKeys(" + Helper.stringify(*args) + ")"
    return self
  def mapValues(self, *args):
    self.traversalString = self.traversalString + ".mapValues(" + Helper.stringify(*args) + ")"
    return self
  def match(self, *args):
    self.traversalString = self.traversalString + ".match(" + Helper.stringify(*args) + ")"
    return self
  def max(self, *args):
    self.traversalString = self.traversalString + ".max(" + Helper.stringify(*args) + ")"
    return self
  def mean(self, *args):
    self.traversalString = self.traversalString + ".mean(" + Helper.stringify(*args) + ")"
    return self
  def min(self, *args):
    self.traversalString = self.traversalString + ".min(" + Helper.stringify(*args) + ")"
    return self
  def option(self, *args):
    self.traversalString = self.traversalString + ".option(" + Helper.stringify(*args) + ")"
    return self
  def optional(self, *args):
    self.traversalString = self.traversalString + ".optional(" + Helper.stringify(*args) + ")"
    return self
  def order(self, *args):
    self.traversalString = self.traversalString + ".order(" + Helper.stringify(*args) + ")"
    return self
  def otherV(self, *args):
    self.traversalString = self.traversalString + ".otherV(" + Helper.stringify(*args) + ")"
    return self
  def out(self, *args):
    self.traversalString = self.traversalString + ".out(" + Helper.stringify(*args) + ")"
    return self
  def outE(self, *args):
    self.traversalString = self.traversalString + ".outE(" + Helper.stringify(*args) + ")"
    return self
  def outV(self, *args):
    self.traversalString = self.traversalString + ".outV(" + Helper.stringify(*args) + ")"
    return self
  def pageRank(self, *args):
    self.traversalString = self.traversalString + ".pageRank(" + Helper.stringify(*args) + ")"
    return self
  def path(self, *args):
    self.traversalString = self.traversalString + ".path(" + Helper.stringify(*args) + ")"
    return self
  def peerPressure(self, *args):
    self.traversalString = self.traversalString + ".peerPressure(" + Helper.stringify(*args) + ")"
    return self
  def profile(self, *args):
    self.traversalString = self.traversalString + ".profile(" + Helper.stringify(*args) + ")"
    return self
  def program(self, *args):
    self.traversalString = self.traversalString + ".program(" + Helper.stringify(*args) + ")"
    return self
  def project(self, *args):
    self.traversalString = self.traversalString + ".project(" + Helper.stringify(*args) + ")"
    return self
  def properties(self, *args):
    self.traversalString = self.traversalString + ".properties(" + Helper.stringify(*args) + ")"
    return self
  def property(self, *args):
    self.traversalString = self.traversalString + ".property(" + Helper.stringify(*args) + ")"
    return self
  def propertyMap(self, *args):
    self.traversalString = self.traversalString + ".propertyMap(" + Helper.stringify(*args) + ")"
    return self
  def range(self, *args):
    self.traversalString = self.traversalString + ".range(" + Helper.stringify(*args) + ")"
    return self
  def repeat(self, *args):
    self.traversalString = self.traversalString + ".repeat(" + Helper.stringify(*args) + ")"
    return self
  def sack(self, *args):
    self.traversalString = self.traversalString + ".sack(" + Helper.stringify(*args) + ")"
    return self
  def sample(self, *args):
    self.traversalString = self.traversalString + ".sample(" + Helper.stringify(*args) + ")"
    return self
  def select(self, *args):
    self.traversalString = self.traversalString + ".select(" + Helper.stringify(*args) + ")"
    return self
  def sideEffect(self, *args):
    self.traversalString = self.traversalString + ".sideEffect(" + Helper.stringify(*args) + ")"
    return self
  def simplePath(self, *args):
    self.traversalString = self.traversalString + ".simplePath(" + Helper.stringify(*args) + ")"
    return self
  def store(self, *args):
    self.traversalString = self.traversalString + ".store(" + Helper.stringify(*args) + ")"
    return self
  def subgraph(self, *args):
    self.traversalString = self.traversalString + ".subgraph(" + Helper.stringify(*args) + ")"
    return self
  def sum(self, *args):
    self.traversalString = self.traversalString + ".sum(" + Helper.stringify(*args) + ")"
    return self
  def tail(self, *args):
    self.traversalString = self.traversalString + ".tail(" + Helper.stringify(*args) + ")"
    return self
  def timeLimit(self, *args):
    self.traversalString = self.traversalString + ".timeLimit(" + Helper.stringify(*args) + ")"
    return self
  def times(self, *args):
    self.traversalString = self.traversalString + ".times(" + Helper.stringify(*args) + ")"
    return self
  def to(self, *args):
    self.traversalString = self.traversalString + ".to(" + Helper.stringify(*args) + ")"
    return self
  def toE(self, *args):
    self.traversalString = self.traversalString + ".toE(" + Helper.stringify(*args) + ")"
    return self
  def toV(self, *args):
    self.traversalString = self.traversalString + ".toV(" + Helper.stringify(*args) + ")"
    return self
  def tree(self, *args):
    self.traversalString = self.traversalString + ".tree(" + Helper.stringify(*args) + ")"
    return self
  def unfold(self, *args):
    self.traversalString = self.traversalString + ".unfold(" + Helper.stringify(*args) + ")"
    return self
  def union(self, *args):
    self.traversalString = self.traversalString + ".union(" + Helper.stringify(*args) + ")"
    return self
  def until(self, *args):
    self.traversalString = self.traversalString + ".until(" + Helper.stringify(*args) + ")"
    return self
  def value(self, *args):
    self.traversalString = self.traversalString + ".value(" + Helper.stringify(*args) + ")"
    return self
  def valueMap(self, *args):
    self.traversalString = self.traversalString + ".valueMap(" + Helper.stringify(*args) + ")"
    return self
  def values(self, *args):
    self.traversalString = self.traversalString + ".values(" + Helper.stringify(*args) + ")"
    return self
  def where(self, *args):
    self.traversalString = self.traversalString + ".where(" + Helper.stringify(*args) + ")"
    return self


class __(object):
  @staticmethod
  def V(*args):
    return PythonGraphTraversal("__").V(*args)
  @staticmethod
  def __(*args):
    return PythonGraphTraversal("__").__(*args)
  @staticmethod
  def _and(*args):
    return PythonGraphTraversal("__")._and(*args)
  @staticmethod
  def _as(*args):
    return PythonGraphTraversal("__")._as(*args)
  @staticmethod
  def _in(*args):
    return PythonGraphTraversal("__")._in(*args)
  @staticmethod
  def _is(*args):
    return PythonGraphTraversal("__")._is(*args)
  @staticmethod
  def _not(*args):
    return PythonGraphTraversal("__")._not(*args)
  @staticmethod
  def _or(*args):
    return PythonGraphTraversal("__")._or(*args)
  @staticmethod
  def addE(*args):
    return PythonGraphTraversal("__").addE(*args)
  @staticmethod
  def addInE(*args):
    return PythonGraphTraversal("__").addInE(*args)
  @staticmethod
  def addOutE(*args):
    return PythonGraphTraversal("__").addOutE(*args)
  @staticmethod
  def addV(*args):
    return PythonGraphTraversal("__").addV(*args)
  @staticmethod
  def aggregate(*args):
    return PythonGraphTraversal("__").aggregate(*args)
  @staticmethod
  def barrier(*args):
    return PythonGraphTraversal("__").barrier(*args)
  @staticmethod
  def both(*args):
    return PythonGraphTraversal("__").both(*args)
  @staticmethod
  def bothE(*args):
    return PythonGraphTraversal("__").bothE(*args)
  @staticmethod
  def bothV(*args):
    return PythonGraphTraversal("__").bothV(*args)
  @staticmethod
  def branch(*args):
    return PythonGraphTraversal("__").branch(*args)
  @staticmethod
  def cap(*args):
    return PythonGraphTraversal("__").cap(*args)
  @staticmethod
  def choose(*args):
    return PythonGraphTraversal("__").choose(*args)
  @staticmethod
  def coalesce(*args):
    return PythonGraphTraversal("__").coalesce(*args)
  @staticmethod
  def coin(*args):
    return PythonGraphTraversal("__").coin(*args)
  @staticmethod
  def constant(*args):
    return PythonGraphTraversal("__").constant(*args)
  @staticmethod
  def count(*args):
    return PythonGraphTraversal("__").count(*args)
  @staticmethod
  def cyclicPath(*args):
    return PythonGraphTraversal("__").cyclicPath(*args)
  @staticmethod
  def dedup(*args):
    return PythonGraphTraversal("__").dedup(*args)
  @staticmethod
  def drop(*args):
    return PythonGraphTraversal("__").drop(*args)
  @staticmethod
  def emit(*args):
    return PythonGraphTraversal("__").emit(*args)
  @staticmethod
  def filter(*args):
    return PythonGraphTraversal("__").filter(*args)
  @staticmethod
  def flatMap(*args):
    return PythonGraphTraversal("__").flatMap(*args)
  @staticmethod
  def fold(*args):
    return PythonGraphTraversal("__").fold(*args)
  @staticmethod
  def group(*args):
    return PythonGraphTraversal("__").group(*args)
  @staticmethod
  def groupCount(*args):
    return PythonGraphTraversal("__").groupCount(*args)
  @staticmethod
  def groupV3d0(*args):
    return PythonGraphTraversal("__").groupV3d0(*args)
  @staticmethod
  def has(*args):
    return PythonGraphTraversal("__").has(*args)
  @staticmethod
  def hasId(*args):
    return PythonGraphTraversal("__").hasId(*args)
  @staticmethod
  def hasKey(*args):
    return PythonGraphTraversal("__").hasKey(*args)
  @staticmethod
  def hasLabel(*args):
    return PythonGraphTraversal("__").hasLabel(*args)
  @staticmethod
  def hasNot(*args):
    return PythonGraphTraversal("__").hasNot(*args)
  @staticmethod
  def hasValue(*args):
    return PythonGraphTraversal("__").hasValue(*args)
  @staticmethod
  def id(*args):
    return PythonGraphTraversal("__").id(*args)
  @staticmethod
  def identity(*args):
    return PythonGraphTraversal("__").identity(*args)
  @staticmethod
  def inE(*args):
    return PythonGraphTraversal("__").inE(*args)
  @staticmethod
  def inV(*args):
    return PythonGraphTraversal("__").inV(*args)
  @staticmethod
  def inject(*args):
    return PythonGraphTraversal("__").inject(*args)
  @staticmethod
  def key(*args):
    return PythonGraphTraversal("__").key(*args)
  @staticmethod
  def label(*args):
    return PythonGraphTraversal("__").label(*args)
  @staticmethod
  def limit(*args):
    return PythonGraphTraversal("__").limit(*args)
  @staticmethod
  def local(*args):
    return PythonGraphTraversal("__").local(*args)
  @staticmethod
  def loops(*args):
    return PythonGraphTraversal("__").loops(*args)
  @staticmethod
  def map(*args):
    return PythonGraphTraversal("__").map(*args)
  @staticmethod
  def mapKeys(*args):
    return PythonGraphTraversal("__").mapKeys(*args)
  @staticmethod
  def mapValues(*args):
    return PythonGraphTraversal("__").mapValues(*args)
  @staticmethod
  def match(*args):
    return PythonGraphTraversal("__").match(*args)
  @staticmethod
  def max(*args):
    return PythonGraphTraversal("__").max(*args)
  @staticmethod
  def mean(*args):
    return PythonGraphTraversal("__").mean(*args)
  @staticmethod
  def min(*args):
    return PythonGraphTraversal("__").min(*args)
  @staticmethod
  def optional(*args):
    return PythonGraphTraversal("__").optional(*args)
  @staticmethod
  def order(*args):
    return PythonGraphTraversal("__").order(*args)
  @staticmethod
  def otherV(*args):
    return PythonGraphTraversal("__").otherV(*args)
  @staticmethod
  def out(*args):
    return PythonGraphTraversal("__").out(*args)
  @staticmethod
  def outE(*args):
    return PythonGraphTraversal("__").outE(*args)
  @staticmethod
  def outV(*args):
    return PythonGraphTraversal("__").outV(*args)
  @staticmethod
  def path(*args):
    return PythonGraphTraversal("__").path(*args)
  @staticmethod
  def project(*args):
    return PythonGraphTraversal("__").project(*args)
  @staticmethod
  def properties(*args):
    return PythonGraphTraversal("__").properties(*args)
  @staticmethod
  def property(*args):
    return PythonGraphTraversal("__").property(*args)
  @staticmethod
  def propertyMap(*args):
    return PythonGraphTraversal("__").propertyMap(*args)
  @staticmethod
  def range(*args):
    return PythonGraphTraversal("__").range(*args)
  @staticmethod
  def repeat(*args):
    return PythonGraphTraversal("__").repeat(*args)
  @staticmethod
  def sack(*args):
    return PythonGraphTraversal("__").sack(*args)
  @staticmethod
  def sample(*args):
    return PythonGraphTraversal("__").sample(*args)
  @staticmethod
  def select(*args):
    return PythonGraphTraversal("__").select(*args)
  @staticmethod
  def sideEffect(*args):
    return PythonGraphTraversal("__").sideEffect(*args)
  @staticmethod
  def simplePath(*args):
    return PythonGraphTraversal("__").simplePath(*args)
  @staticmethod
  def start(*args):
    return PythonGraphTraversal("__").start(*args)
  @staticmethod
  def store(*args):
    return PythonGraphTraversal("__").store(*args)
  @staticmethod
  def subgraph(*args):
    return PythonGraphTraversal("__").subgraph(*args)
  @staticmethod
  def sum(*args):
    return PythonGraphTraversal("__").sum(*args)
  @staticmethod
  def tail(*args):
    return PythonGraphTraversal("__").tail(*args)
  @staticmethod
  def timeLimit(*args):
    return PythonGraphTraversal("__").timeLimit(*args)
  @staticmethod
  def times(*args):
    return PythonGraphTraversal("__").times(*args)
  @staticmethod
  def to(*args):
    return PythonGraphTraversal("__").to(*args)
  @staticmethod
  def toE(*args):
    return PythonGraphTraversal("__").toE(*args)
  @staticmethod
  def toV(*args):
    return PythonGraphTraversal("__").toV(*args)
  @staticmethod
  def tree(*args):
    return PythonGraphTraversal("__").tree(*args)
  @staticmethod
  def unfold(*args):
    return PythonGraphTraversal("__").unfold(*args)
  @staticmethod
  def union(*args):
    return PythonGraphTraversal("__").union(*args)
  @staticmethod
  def until(*args):
    return PythonGraphTraversal("__").until(*args)
  @staticmethod
  def value(*args):
    return PythonGraphTraversal("__").value(*args)
  @staticmethod
  def valueMap(*args):
    return PythonGraphTraversal("__").valueMap(*args)
  @staticmethod
  def values(*args):
    return PythonGraphTraversal("__").values(*args)
  @staticmethod
  def where(*args):
    return PythonGraphTraversal("__").where(*args)


def V(*args):
      return __.V(*args)

statics['V'] = V
def _and(*args):
      return __._and(*args)

statics['_and'] = _and
def _as(*args):
      return __._as(*args)

statics['_as'] = _as
def _in(*args):
      return __._in(*args)

statics['_in'] = _in
def _is(*args):
      return __._is(*args)

statics['_is'] = _is
def _not(*args):
      return __._not(*args)

statics['_not'] = _not
def _or(*args):
      return __._or(*args)

statics['_or'] = _or
def addE(*args):
      return __.addE(*args)

statics['addE'] = addE
def addInE(*args):
      return __.addInE(*args)

statics['addInE'] = addInE
def addOutE(*args):
      return __.addOutE(*args)

statics['addOutE'] = addOutE
def addV(*args):
      return __.addV(*args)

statics['addV'] = addV
def aggregate(*args):
      return __.aggregate(*args)

statics['aggregate'] = aggregate
def barrier(*args):
      return __.barrier(*args)

statics['barrier'] = barrier
def both(*args):
      return __.both(*args)

statics['both'] = both
def bothE(*args):
      return __.bothE(*args)

statics['bothE'] = bothE
def bothV(*args):
      return __.bothV(*args)

statics['bothV'] = bothV
def branch(*args):
      return __.branch(*args)

statics['branch'] = branch
def cap(*args):
      return __.cap(*args)

statics['cap'] = cap
def choose(*args):
      return __.choose(*args)

statics['choose'] = choose
def coalesce(*args):
      return __.coalesce(*args)

statics['coalesce'] = coalesce
def coin(*args):
      return __.coin(*args)

statics['coin'] = coin
def constant(*args):
      return __.constant(*args)

statics['constant'] = constant
def count(*args):
      return __.count(*args)

statics['count'] = count
def cyclicPath(*args):
      return __.cyclicPath(*args)

statics['cyclicPath'] = cyclicPath
def dedup(*args):
      return __.dedup(*args)

statics['dedup'] = dedup
def drop(*args):
      return __.drop(*args)

statics['drop'] = drop
def emit(*args):
      return __.emit(*args)

statics['emit'] = emit
def filter(*args):
      return __.filter(*args)

statics['filter'] = filter
def flatMap(*args):
      return __.flatMap(*args)

statics['flatMap'] = flatMap
def fold(*args):
      return __.fold(*args)

statics['fold'] = fold
def group(*args):
      return __.group(*args)

statics['group'] = group
def groupCount(*args):
      return __.groupCount(*args)

statics['groupCount'] = groupCount
def groupV3d0(*args):
      return __.groupV3d0(*args)

statics['groupV3d0'] = groupV3d0
def has(*args):
      return __.has(*args)

statics['has'] = has
def hasId(*args):
      return __.hasId(*args)

statics['hasId'] = hasId
def hasKey(*args):
      return __.hasKey(*args)

statics['hasKey'] = hasKey
def hasLabel(*args):
      return __.hasLabel(*args)

statics['hasLabel'] = hasLabel
def hasNot(*args):
      return __.hasNot(*args)

statics['hasNot'] = hasNot
def hasValue(*args):
      return __.hasValue(*args)

statics['hasValue'] = hasValue
def id(*args):
      return __.id(*args)

statics['id'] = id
def identity(*args):
      return __.identity(*args)

statics['identity'] = identity
def inE(*args):
      return __.inE(*args)

statics['inE'] = inE
def inV(*args):
      return __.inV(*args)

statics['inV'] = inV
def inject(*args):
      return __.inject(*args)

statics['inject'] = inject
def key(*args):
      return __.key(*args)

statics['key'] = key
def label(*args):
      return __.label(*args)

statics['label'] = label
def limit(*args):
      return __.limit(*args)

statics['limit'] = limit
def local(*args):
      return __.local(*args)

statics['local'] = local
def loops(*args):
      return __.loops(*args)

statics['loops'] = loops
def map(*args):
      return __.map(*args)

statics['map'] = map
def mapKeys(*args):
      return __.mapKeys(*args)

statics['mapKeys'] = mapKeys
def mapValues(*args):
      return __.mapValues(*args)

statics['mapValues'] = mapValues
def match(*args):
      return __.match(*args)

statics['match'] = match
def max(*args):
      return __.max(*args)

statics['max'] = max
def mean(*args):
      return __.mean(*args)

statics['mean'] = mean
def min(*args):
      return __.min(*args)

statics['min'] = min
def optional(*args):
      return __.optional(*args)

statics['optional'] = optional
def order(*args):
      return __.order(*args)

statics['order'] = order
def otherV(*args):
      return __.otherV(*args)

statics['otherV'] = otherV
def out(*args):
      return __.out(*args)

statics['out'] = out
def outE(*args):
      return __.outE(*args)

statics['outE'] = outE
def outV(*args):
      return __.outV(*args)

statics['outV'] = outV
def path(*args):
      return __.path(*args)

statics['path'] = path
def project(*args):
      return __.project(*args)

statics['project'] = project
def properties(*args):
      return __.properties(*args)

statics['properties'] = properties
def property(*args):
      return __.property(*args)

statics['property'] = property
def propertyMap(*args):
      return __.propertyMap(*args)

statics['propertyMap'] = propertyMap
def range(*args):
      return __.range(*args)

statics['range'] = range
def repeat(*args):
      return __.repeat(*args)

statics['repeat'] = repeat
def sack(*args):
      return __.sack(*args)

statics['sack'] = sack
def sample(*args):
      return __.sample(*args)

statics['sample'] = sample
def select(*args):
      return __.select(*args)

statics['select'] = select
def sideEffect(*args):
      return __.sideEffect(*args)

statics['sideEffect'] = sideEffect
def simplePath(*args):
      return __.simplePath(*args)

statics['simplePath'] = simplePath
def start(*args):
      return __.start(*args)

statics['start'] = start
def store(*args):
      return __.store(*args)

statics['store'] = store
def subgraph(*args):
      return __.subgraph(*args)

statics['subgraph'] = subgraph
def sum(*args):
      return __.sum(*args)

statics['sum'] = sum
def tail(*args):
      return __.tail(*args)

statics['tail'] = tail
def timeLimit(*args):
      return __.timeLimit(*args)

statics['timeLimit'] = timeLimit
def times(*args):
      return __.times(*args)

statics['times'] = times
def to(*args):
      return __.to(*args)

statics['to'] = to
def toE(*args):
      return __.toE(*args)

statics['toE'] = toE
def toV(*args):
      return __.toV(*args)

statics['toV'] = toV
def tree(*args):
      return __.tree(*args)

statics['tree'] = tree
def unfold(*args):
      return __.unfold(*args)

statics['unfold'] = unfold
def union(*args):
      return __.union(*args)

statics['union'] = union
def until(*args):
      return __.until(*args)

statics['until'] = until
def value(*args):
      return __.value(*args)

statics['value'] = value
def valueMap(*args):
      return __.valueMap(*args)

statics['valueMap'] = valueMap
def values(*args):
      return __.values(*args)

statics['values'] = values
def where(*args):
      return __.where(*args)

statics['where'] = where


class Cardinality(object):
   single = "VertexProperty.Cardinality.single"
   list = "VertexProperty.Cardinality.list"
   set = "VertexProperty.Cardinality.set"

class Column(object):
   keys = "Column.keys"
   values = "Column.values"

statics['keys'] = Column.keys
statics['values'] = Column.values

class Direction(object):
   OUT = "Direction.OUT"
   IN = "Direction.IN"
   BOTH = "Direction.BOTH"

statics['OUT'] = Direction.OUT
statics['IN'] = Direction.IN
statics['BOTH'] = Direction.BOTH

class Operator(object):
   sum = "Operator.sum"
   minus = "Operator.minus"
   mult = "Operator.mult"
   div = "Operator.div"
   min = "Operator.min"
   max = "Operator.max"
   assign = "Operator.assign"
   _and = "Operator.and"
   _or = "Operator.or"
   addAll = "Operator.addAll"
   sumLong = "Operator.sumLong"

statics['sum'] = Operator.sum
statics['minus'] = Operator.minus
statics['mult'] = Operator.mult
statics['div'] = Operator.div
statics['min'] = Operator.min
statics['max'] = Operator.max
statics['assign'] = Operator.assign
statics['_and'] = Operator._and
statics['_or'] = Operator._or
statics['addAll'] = Operator.addAll
statics['sumLong'] = Operator.sumLong

class Order(object):
   incr = "Order.incr"
   decr = "Order.decr"
   keyIncr = "Order.keyIncr"
   valueIncr = "Order.valueIncr"
   keyDecr = "Order.keyDecr"
   valueDecr = "Order.valueDecr"
   shuffle = "Order.shuffle"

statics['incr'] = Order.incr
statics['decr'] = Order.decr
statics['keyIncr'] = Order.keyIncr
statics['valueIncr'] = Order.valueIncr
statics['keyDecr'] = Order.keyDecr
statics['valueDecr'] = Order.valueDecr
statics['shuffle'] = Order.shuffle

class P(object):
   def __init__(self, pString):
      self.pString = pString
   def __repr__(self):
      return self.pString
   @staticmethod
   def _not(*args):
      return P("P.not(" + Helper.stringify(*args) + ")")
   @staticmethod
   def between(*args):
      return P("P.between(" + Helper.stringify(*args) + ")")
   @staticmethod
   def clone(*args):
      return P("P.clone(" + Helper.stringify(*args) + ")")
   @staticmethod
   def eq(*args):
      return P("P.eq(" + Helper.stringify(*args) + ")")
   @staticmethod
   def gt(*args):
      return P("P.gt(" + Helper.stringify(*args) + ")")
   @staticmethod
   def gte(*args):
      return P("P.gte(" + Helper.stringify(*args) + ")")
   @staticmethod
   def inside(*args):
      return P("P.inside(" + Helper.stringify(*args) + ")")
   @staticmethod
   def lt(*args):
      return P("P.lt(" + Helper.stringify(*args) + ")")
   @staticmethod
   def lte(*args):
      return P("P.lte(" + Helper.stringify(*args) + ")")
   @staticmethod
   def negate(*args):
      return P("P.negate(" + Helper.stringify(*args) + ")")
   @staticmethod
   def neq(*args):
      return P("P.neq(" + Helper.stringify(*args) + ")")
   @staticmethod
   def outside(*args):
      return P("P.outside(" + Helper.stringify(*args) + ")")
   @staticmethod
   def test(*args):
      return P("P.test(" + Helper.stringify(*args) + ")")
   @staticmethod
   def within(*args):
      return P("P.within(" + Helper.stringify(*args) + ")")
   @staticmethod
   def without(*args):
      return P("P.without(" + Helper.stringify(*args) + ")")
   def _and(self, arg):
      return P(self.pString + ".and(" + Helper.stringify(arg) + ")")
   def _or(self, arg):
      return P(self.pString + ".or(" + Helper.stringify(arg) + ")")

def _and(*args):
      return P._and(*args)

statics['_and'] = _and
def _not(*args):
      return P._not(*args)

statics['_not'] = _not
def _or(*args):
      return P._or(*args)

statics['_or'] = _or
def between(*args):
      return P.between(*args)

statics['between'] = between
def clone(*args):
      return P.clone(*args)

statics['clone'] = clone
def eq(*args):
      return P.eq(*args)

statics['eq'] = eq
def gt(*args):
      return P.gt(*args)

statics['gt'] = gt
def gte(*args):
      return P.gte(*args)

statics['gte'] = gte
def inside(*args):
      return P.inside(*args)

statics['inside'] = inside
def lt(*args):
      return P.lt(*args)

statics['lt'] = lt
def lte(*args):
      return P.lte(*args)

statics['lte'] = lte
def negate(*args):
      return P.negate(*args)

statics['negate'] = negate
def neq(*args):
      return P.neq(*args)

statics['neq'] = neq
def outside(*args):
      return P.outside(*args)

statics['outside'] = outside
def test(*args):
      return P.test(*args)

statics['test'] = test
def within(*args):
      return P.within(*args)

statics['within'] = within
def without(*args):
      return P.without(*args)

statics['without'] = without

class Pop(object):
   first = "Pop.first"
   last = "Pop.last"
   all = "Pop.all"

statics['first'] =  Pop.first
statics['last'] =  Pop.last
statics['all'] =  Pop.all

class Barrier(object):
   normSack = "SackFunctions.Barrier.normSack"

statics['normSack'] = Barrier.normSack

class Scope(object):
   _global = "Scope.global"
   local = "Scope.local"

statics['_global'] = Scope._global
statics['local'] = Scope.local

class T(object):
   label = "T.label"
   id = "T.id"
   key = "T.key"
   value = "T.value"

statics['label'] = T.label
statics['id'] = T.id
statics['key'] = T.key
statics['value'] = T.value

statics = OrderedDict(reversed(list(statics.items())))
