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

from tornado import gen
from tornado.ioloop import IOLoop
from gremlinclient.tornado_client import submit

class Helper(object):
  @staticmethod
  def stringOrObject(arg):
    if (type(arg) is str and
       not(arg.startswith("P.")) and
       not(arg.startswith("Order.")) and
       not(arg.startswith("T.")) and
       not(arg.startswith("Pop.")) and
       not(arg.startswith("Column."))):
      return "\"" + arg + "\""
    elif type(arg) is bool:
      return str(arg).lower()
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
  @staticmethod
  @gen.coroutine
  def submit(gremlinServerURI, traversalString):
    response = yield submit(gremlinServerURI, traversalString)
    while True:
      result = yield response.read()
      if result is None:
        break
      raise gen.Return(result.data)

class PythonGraphTraversalSource(object):
  def __init__(self, gremlinServerURI, traversalSourceString):
    self.gremlinServerURI = gremlinServerURI
    self.traversalSourceString = traversalSourceString
  def __repr__(self):
    return "graphtraversalsource[" + self.gremlinServerURI + ", " + self.traversalSourceString + "]"
  def toString(self, *args):
    return PythonGraphTraversalSource(self.gremlinServerURI, self.traversalSourceString + ".toString(" + Helper.stringify(*args) + ")")
  def clone(self, *args):
    return PythonGraphTraversalSource(self.gremlinServerURI, self.traversalSourceString + ".clone(" + Helper.stringify(*args) + ")")
  def V(self, *args):
    return PythonGraphTraversal(self.traversalSourceString + ".V(" + Helper.stringify(*args) + ")", self.gremlinServerURI)
  def E(self, *args):
    return PythonGraphTraversal(self.traversalSourceString + ".E(" + Helper.stringify(*args) + ")", self.gremlinServerURI)
  def build(self, *args):
    return PythonGraphTraversalSource(self.gremlinServerURI, self.traversalSourceString + ".build(" + Helper.stringify(*args) + ")")
  def getStrategies(self, *args):
    return PythonGraphTraversalSource(self.gremlinServerURI, self.traversalSourceString + ".getStrategies(" + Helper.stringify(*args) + ")")
  def addV(self, *args):
    return PythonGraphTraversal(self.traversalSourceString + ".addV(" + Helper.stringify(*args) + ")", self.gremlinServerURI)
  def getGraph(self, *args):
    return PythonGraphTraversalSource(self.gremlinServerURI, self.traversalSourceString + ".getGraph(" + Helper.stringify(*args) + ")")
  def inject(self, *args):
    return PythonGraphTraversal(self.traversalSourceString + ".inject(" + Helper.stringify(*args) + ")", self.gremlinServerURI)
  def computer(self, *args):
    return PythonGraphTraversalSource(self.gremlinServerURI, self.traversalSourceString + ".computer(" + Helper.stringify(*args) + ")")
  def tx(self, *args):
    return PythonGraphTraversalSource(self.gremlinServerURI, self.traversalSourceString + ".tx(" + Helper.stringify(*args) + ")")
  def withComputer(self, *args):
    return PythonGraphTraversalSource(self.gremlinServerURI, self.traversalSourceString + ".withComputer(" + Helper.stringify(*args) + ")")
  def withStrategies(self, *args):
    return PythonGraphTraversalSource(self.gremlinServerURI, self.traversalSourceString + ".withStrategies(" + Helper.stringify(*args) + ")")
  def withoutStrategies(self, *args):
    return PythonGraphTraversalSource(self.gremlinServerURI, self.traversalSourceString + ".withoutStrategies(" + Helper.stringify(*args) + ")")
  def withSideEffect(self, *args):
    return PythonGraphTraversalSource(self.gremlinServerURI, self.traversalSourceString + ".withSideEffect(" + Helper.stringify(*args) + ")")
  def withSack(self, *args):
    return PythonGraphTraversalSource(self.gremlinServerURI, self.traversalSourceString + ".withSack(" + Helper.stringify(*args) + ")")
  def withBulk(self, *args):
    return PythonGraphTraversalSource(self.gremlinServerURI, self.traversalSourceString + ".withBulk(" + Helper.stringify(*args) + ")")
  def withPath(self, *args):
    return PythonGraphTraversalSource(self.gremlinServerURI, self.traversalSourceString + ".withPath(" + Helper.stringify(*args) + ")")
  def standard(self, *args):
    return PythonGraphTraversalSource(self.gremlinServerURI, self.traversalSourceString + ".standard(" + Helper.stringify(*args) + ")")
  def wait(self, *args):
    return PythonGraphTraversalSource(self.gremlinServerURI, self.traversalSourceString + ".wait(" + Helper.stringify(*args) + ")")
  def equals(self, *args):
    return PythonGraphTraversalSource(self.gremlinServerURI, self.traversalSourceString + ".equals(" + Helper.stringify(*args) + ")")
  def hashCode(self, *args):
    return PythonGraphTraversalSource(self.gremlinServerURI, self.traversalSourceString + ".hashCode(" + Helper.stringify(*args) + ")")
  def getClass(self, *args):
    return PythonGraphTraversalSource(self.gremlinServerURI, self.traversalSourceString + ".getClass(" + Helper.stringify(*args) + ")")
  def notify(self, *args):
    return PythonGraphTraversalSource(self.gremlinServerURI, self.traversalSourceString + ".notify(" + Helper.stringify(*args) + ")")
  def notifyAll(self, *args):
    return PythonGraphTraversalSource(self.gremlinServerURI, self.traversalSourceString + ".notifyAll(" + Helper.stringify(*args) + ")")


class PythonGraphTraversal(object):
  def __init__(self, traversalString, gremlinServerURI=None):
    self.traversalString = traversalString
    self.gremlinServerURI = gremlinServerURI
    self.result = None
  def __repr__(self):
    return self.traversalString;
  def __getitem__(self,index):
    if type(index) is int:
      return self.range(index,index+1)
    elif type(index) is slice:
      return self.range(index.start,index.stop)
    else:
      raise TypeError("index must be int or slice")
  def __getattr__(self,key):
    return self.values(key)
  def toList(self):
    return IOLoop.current().run_sync(lambda: Helper.submit(self.gremlinServerURI, self.traversalString))
  def group(self, *args):
    self.traversalString = self.traversalString + ".group(" + Helper.stringify(*args) + ")"
    return self
  def limit(self, *args):
    self.traversalString = self.traversalString + ".limit(" + Helper.stringify(*args) + ")"
    return self
  def value(self, *args):
    self.traversalString = self.traversalString + ".value(" + Helper.stringify(*args) + ")"
    return self
  def count(self, *args):
    self.traversalString = self.traversalString + ".count(" + Helper.stringify(*args) + ")"
    return self
  def profile(self, *args):
    self.traversalString = self.traversalString + ".profile(" + Helper.stringify(*args) + ")"
    return self
  def values(self, *args):
    self.traversalString = self.traversalString + ".values(" + Helper.stringify(*args) + ")"
    return self
  def min(self, *args):
    self.traversalString = self.traversalString + ".min(" + Helper.stringify(*args) + ")"
    return self
  def max(self, *args):
    self.traversalString = self.traversalString + ".max(" + Helper.stringify(*args) + ")"
    return self
  def V(self, *args):
    self.traversalString = self.traversalString + ".V(" + Helper.stringify(*args) + ")"
    return self
  def identity(self, *args):
    self.traversalString = self.traversalString + ".identity(" + Helper.stringify(*args) + ")"
    return self
  def _in(self, *args):
    self.traversalString = self.traversalString + ".in(" + Helper.stringify(*args) + ")"
    return self
  def out(self, *args):
    self.traversalString = self.traversalString + ".out(" + Helper.stringify(*args) + ")"
    return self
  def key(self, *args):
    self.traversalString = self.traversalString + ".key(" + Helper.stringify(*args) + ")"
    return self
  def store(self, *args):
    self.traversalString = self.traversalString + ".store(" + Helper.stringify(*args) + ")"
    return self
  def path(self, *args):
    self.traversalString = self.traversalString + ".path(" + Helper.stringify(*args) + ")"
    return self
  def sum(self, *args):
    self.traversalString = self.traversalString + ".sum(" + Helper.stringify(*args) + ")"
    return self
  def toV(self, *args):
    self.traversalString = self.traversalString + ".toV(" + Helper.stringify(*args) + ")"
    return self
  def filter(self, *args):
    self.traversalString = self.traversalString + ".filter(" + Helper.stringify(*args) + ")"
    return self
  def tree(self, *args):
    self.traversalString = self.traversalString + ".tree(" + Helper.stringify(*args) + ")"
    return self
  def match(self, *args):
    self.traversalString = self.traversalString + ".match(" + Helper.stringify(*args) + ")"
    return self
  def range(self, *args):
    self.traversalString = self.traversalString + ".range(" + Helper.stringify(*args) + ")"
    return self
  def order(self, *args):
    self.traversalString = self.traversalString + ".order(" + Helper.stringify(*args) + ")"
    return self
  def map(self, *args):
    self.traversalString = self.traversalString + ".map(" + Helper.stringify(*args) + ")"
    return self
  def tail(self, *args):
    self.traversalString = self.traversalString + ".tail(" + Helper.stringify(*args) + ")"
    return self
  def _and(self, *args):
    self.traversalString = self.traversalString + ".and(" + Helper.stringify(*args) + ")"
    return self
  def _or(self, *args):
    self.traversalString = self.traversalString + ".or(" + Helper.stringify(*args) + ")"
    return self
  def id(self, *args):
    self.traversalString = self.traversalString + ".id(" + Helper.stringify(*args) + ")"
    return self
  def outE(self, *args):
    self.traversalString = self.traversalString + ".outE(" + Helper.stringify(*args) + ")"
    return self
  def toE(self, *args):
    self.traversalString = self.traversalString + ".toE(" + Helper.stringify(*args) + ")"
    return self
  def inE(self, *args):
    self.traversalString = self.traversalString + ".inE(" + Helper.stringify(*args) + ")"
    return self
  def property(self, *args):
    self.traversalString = self.traversalString + ".property(" + Helper.stringify(*args) + ")"
    return self
  def both(self, *args):
    self.traversalString = self.traversalString + ".both(" + Helper.stringify(*args) + ")"
    return self
  def bothE(self, *args):
    self.traversalString = self.traversalString + ".bothE(" + Helper.stringify(*args) + ")"
    return self
  def inV(self, *args):
    self.traversalString = self.traversalString + ".inV(" + Helper.stringify(*args) + ")"
    return self
  def outV(self, *args):
    self.traversalString = self.traversalString + ".outV(" + Helper.stringify(*args) + ")"
    return self
  def bothV(self, *args):
    self.traversalString = self.traversalString + ".bothV(" + Helper.stringify(*args) + ")"
    return self
  def otherV(self, *args):
    self.traversalString = self.traversalString + ".otherV(" + Helper.stringify(*args) + ")"
    return self
  def valueMap(self, *args):
    self.traversalString = self.traversalString + ".valueMap(" + Helper.stringify(*args) + ")"
    return self
  def mapValues(self, *args):
    self.traversalString = self.traversalString + ".mapValues(" + Helper.stringify(*args) + ")"
    return self
  def mapKeys(self, *args):
    self.traversalString = self.traversalString + ".mapKeys(" + Helper.stringify(*args) + ")"
    return self
  def sack(self, *args):
    self.traversalString = self.traversalString + ".sack(" + Helper.stringify(*args) + ")"
    return self
  def loops(self, *args):
    self.traversalString = self.traversalString + ".loops(" + Helper.stringify(*args) + ")"
    return self
  def project(self, *args):
    self.traversalString = self.traversalString + ".project(" + Helper.stringify(*args) + ")"
    return self
  def unfold(self, *args):
    self.traversalString = self.traversalString + ".unfold(" + Helper.stringify(*args) + ")"
    return self
  def fold(self, *args):
    self.traversalString = self.traversalString + ".fold(" + Helper.stringify(*args) + ")"
    return self
  def mean(self, *args):
    self.traversalString = self.traversalString + ".mean(" + Helper.stringify(*args) + ")"
    return self
  def groupV3d0(self, *args):
    self.traversalString = self.traversalString + ".groupV3d0(" + Helper.stringify(*args) + ")"
    return self
  def addV(self, *args):
    self.traversalString = self.traversalString + ".addV(" + Helper.stringify(*args) + ")"
    return self
  def addE(self, *args):
    self.traversalString = self.traversalString + ".addE(" + Helper.stringify(*args) + ")"
    return self
  def addOutE(self, *args):
    self.traversalString = self.traversalString + ".addOutE(" + Helper.stringify(*args) + ")"
    return self
  def addInE(self, *args):
    self.traversalString = self.traversalString + ".addInE(" + Helper.stringify(*args) + ")"
    return self
  def dedup(self, *args):
    self.traversalString = self.traversalString + ".dedup(" + Helper.stringify(*args) + ")"
    return self
  def where(self, *args):
    self.traversalString = self.traversalString + ".where(" + Helper.stringify(*args) + ")"
    return self
  def hasNot(self, *args):
    self.traversalString = self.traversalString + ".hasNot(" + Helper.stringify(*args) + ")"
    return self
  def hasLabel(self, *args):
    self.traversalString = self.traversalString + ".hasLabel(" + Helper.stringify(*args) + ")"
    return self
  def hasId(self, *args):
    self.traversalString = self.traversalString + ".hasId(" + Helper.stringify(*args) + ")"
    return self
  def hasKey(self, *args):
    self.traversalString = self.traversalString + ".hasKey(" + Helper.stringify(*args) + ")"
    return self
  def hasValue(self, *args):
    self.traversalString = self.traversalString + ".hasValue(" + Helper.stringify(*args) + ")"
    return self
  def coin(self, *args):
    self.traversalString = self.traversalString + ".coin(" + Helper.stringify(*args) + ")"
    return self
  def timeLimit(self, *args):
    self.traversalString = self.traversalString + ".timeLimit(" + Helper.stringify(*args) + ")"
    return self
  def simplePath(self, *args):
    self.traversalString = self.traversalString + ".simplePath(" + Helper.stringify(*args) + ")"
    return self
  def cyclicPath(self, *args):
    self.traversalString = self.traversalString + ".cyclicPath(" + Helper.stringify(*args) + ")"
    return self
  def sample(self, *args):
    self.traversalString = self.traversalString + ".sample(" + Helper.stringify(*args) + ")"
    return self
  def sideEffect(self, *args):
    self.traversalString = self.traversalString + ".sideEffect(" + Helper.stringify(*args) + ")"
    return self
  def subgraph(self, *args):
    self.traversalString = self.traversalString + ".subgraph(" + Helper.stringify(*args) + ")"
    return self
  def aggregate(self, *args):
    self.traversalString = self.traversalString + ".aggregate(" + Helper.stringify(*args) + ")"
    return self
  def branch(self, *args):
    self.traversalString = self.traversalString + ".branch(" + Helper.stringify(*args) + ")"
    return self
  def optional(self, *args):
    self.traversalString = self.traversalString + ".optional(" + Helper.stringify(*args) + ")"
    return self
  def coalesce(self, *args):
    self.traversalString = self.traversalString + ".coalesce(" + Helper.stringify(*args) + ")"
    return self
  def repeat(self, *args):
    self.traversalString = self.traversalString + ".repeat(" + Helper.stringify(*args) + ")"
    return self
  def emit(self, *args):
    self.traversalString = self.traversalString + ".emit(" + Helper.stringify(*args) + ")"
    return self
  def local(self, *args):
    self.traversalString = self.traversalString + ".local(" + Helper.stringify(*args) + ")"
    return self
  def pageRank(self, *args):
    self.traversalString = self.traversalString + ".pageRank(" + Helper.stringify(*args) + ")"
    return self
  def peerPressure(self, *args):
    self.traversalString = self.traversalString + ".peerPressure(" + Helper.stringify(*args) + ")"
    return self
  def barrier(self, *args):
    self.traversalString = self.traversalString + ".barrier(" + Helper.stringify(*args) + ")"
    return self
  def by(self, *args):
    self.traversalString = self.traversalString + ".by(" + Helper.stringify(*args) + ")"
    return self
  def option(self, *args):
    self.traversalString = self.traversalString + ".option(" + Helper.stringify(*args) + ")"
    return self
  def choose(self, *args):
    self.traversalString = self.traversalString + ".choose(" + Helper.stringify(*args) + ")"
    return self
  def cap(self, *args):
    self.traversalString = self.traversalString + ".cap(" + Helper.stringify(*args) + ")"
    return self
  def inject(self, *args):
    self.traversalString = self.traversalString + ".inject(" + Helper.stringify(*args) + ")"
    return self
  def drop(self, *args):
    self.traversalString = self.traversalString + ".drop(" + Helper.stringify(*args) + ")"
    return self
  def times(self, *args):
    self.traversalString = self.traversalString + ".times(" + Helper.stringify(*args) + ")"
    return self
  def _as(self, *args):
    self.traversalString = self.traversalString + ".as(" + Helper.stringify(*args) + ")"
    return self
  def label(self, *args):
    self.traversalString = self.traversalString + ".label(" + Helper.stringify(*args) + ")"
    return self
  def propertyMap(self, *args):
    self.traversalString = self.traversalString + ".propertyMap(" + Helper.stringify(*args) + ")"
    return self
  def constant(self, *args):
    self.traversalString = self.traversalString + ".constant(" + Helper.stringify(*args) + ")"
    return self
  def until(self, *args):
    self.traversalString = self.traversalString + ".until(" + Helper.stringify(*args) + ")"
    return self
  def has(self, *args):
    self.traversalString = self.traversalString + ".has(" + Helper.stringify(*args) + ")"
    return self
  def union(self, *args):
    self.traversalString = self.traversalString + ".union(" + Helper.stringify(*args) + ")"
    return self
  def groupCount(self, *args):
    self.traversalString = self.traversalString + ".groupCount(" + Helper.stringify(*args) + ")"
    return self
  def flatMap(self, *args):
    self.traversalString = self.traversalString + ".flatMap(" + Helper.stringify(*args) + ")"
    return self
  def iterate(self, *args):
    self.traversalString = self.traversalString + ".iterate(" + Helper.stringify(*args) + ")"
    return self
  def properties(self, *args):
    self.traversalString = self.traversalString + ".properties(" + Helper.stringify(*args) + ")"
    return self
  def to(self, *args):
    self.traversalString = self.traversalString + ".to(" + Helper.stringify(*args) + ")"
    return self
  def _from(self, *args):
    self.traversalString = self.traversalString + ".from(" + Helper.stringify(*args) + ")"
    return self
  def _not(self, *args):
    self.traversalString = self.traversalString + ".not(" + Helper.stringify(*args) + ")"
    return self
  def _is(self, *args):
    self.traversalString = self.traversalString + ".is(" + Helper.stringify(*args) + ")"
    return self
  def select(self, *args):
    self.traversalString = self.traversalString + ".select(" + Helper.stringify(*args) + ")"
    return self
  def asAdmin(self, *args):
    self.traversalString = self.traversalString + ".asAdmin(" + Helper.stringify(*args) + ")"
    return self
  def program(self, *args):
    self.traversalString = self.traversalString + ".program(" + Helper.stringify(*args) + ")"
    return self
  def next(self, *args):
    self.traversalString = self.traversalString + ".next(" + Helper.stringify(*args) + ")"
    return self.toList()
  def fill(self, *args):
    self.traversalString = self.traversalString + ".fill(" + Helper.stringify(*args) + ")"
    return self.toList()
  def forEachRemaining(self, *args):
    self.traversalString = self.traversalString + ".forEachRemaining(" + Helper.stringify(*args) + ")"
    return self.toList()
  def toSet(self, *args):
    self.traversalString = self.traversalString + ".toSet(" + Helper.stringify(*args) + ")"
    return self.toList()
  def tryNext(self, *args):
    self.traversalString = self.traversalString + ".tryNext(" + Helper.stringify(*args) + ")"
    return self.toList()
  def explain(self, *args):
    self.traversalString = self.traversalString + ".explain(" + Helper.stringify(*args) + ")"
    return self.toList()
  def toStream(self, *args):
    self.traversalString = self.traversalString + ".toStream(" + Helper.stringify(*args) + ")"
    return self.toList()
  def toBulkSet(self, *args):
    self.traversalString = self.traversalString + ".toBulkSet(" + Helper.stringify(*args) + ")"
    return self.toList()
  def remove(self, *args):
    self.traversalString = self.traversalString + ".remove(" + Helper.stringify(*args) + ")"
    return self.toList()
  def hasNext(self, *args):
    self.traversalString = self.traversalString + ".hasNext(" + Helper.stringify(*args) + ")"
    return self.toList()


class __(object):
  @staticmethod
  def group(*args):
    return PythonGraphTraversal("__").group(*args)
  @staticmethod
  def limit(*args):
    return PythonGraphTraversal("__").limit(*args)
  @staticmethod
  def value(*args):
    return PythonGraphTraversal("__").value(*args)
  @staticmethod
  def count(*args):
    return PythonGraphTraversal("__").count(*args)
  @staticmethod
  def values(*args):
    return PythonGraphTraversal("__").values(*args)
  @staticmethod
  def min(*args):
    return PythonGraphTraversal("__").min(*args)
  @staticmethod
  def max(*args):
    return PythonGraphTraversal("__").max(*args)
  @staticmethod
  def V(*args):
    return PythonGraphTraversal("__").V(*args)
  @staticmethod
  def identity(*args):
    return PythonGraphTraversal("__").identity(*args)
  @staticmethod
  def _in(*args):
    return PythonGraphTraversal("__")._in(*args)
  @staticmethod
  def out(*args):
    return PythonGraphTraversal("__").out(*args)
  @staticmethod
  def key(*args):
    return PythonGraphTraversal("__").key(*args)
  @staticmethod
  def start(*args):
    return PythonGraphTraversal("__").start(*args)
  @staticmethod
  def store(*args):
    return PythonGraphTraversal("__").store(*args)
  @staticmethod
  def path(*args):
    return PythonGraphTraversal("__").path(*args)
  @staticmethod
  def sum(*args):
    return PythonGraphTraversal("__").sum(*args)
  @staticmethod
  def toV(*args):
    return PythonGraphTraversal("__").toV(*args)
  @staticmethod
  def filter(*args):
    return PythonGraphTraversal("__").filter(*args)
  @staticmethod
  def tree(*args):
    return PythonGraphTraversal("__").tree(*args)
  @staticmethod
  def match(*args):
    return PythonGraphTraversal("__").match(*args)
  @staticmethod
  def range(*args):
    return PythonGraphTraversal("__").range(*args)
  @staticmethod
  def order(*args):
    return PythonGraphTraversal("__").order(*args)
  @staticmethod
  def map(*args):
    return PythonGraphTraversal("__").map(*args)
  @staticmethod
  def tail(*args):
    return PythonGraphTraversal("__").tail(*args)
  @staticmethod
  def _and(*args):
    return PythonGraphTraversal("__")._and(*args)
  @staticmethod
  def _or(*args):
    return PythonGraphTraversal("__")._or(*args)
  @staticmethod
  def id(*args):
    return PythonGraphTraversal("__").id(*args)
  @staticmethod
  def outE(*args):
    return PythonGraphTraversal("__").outE(*args)
  @staticmethod
  def toE(*args):
    return PythonGraphTraversal("__").toE(*args)
  @staticmethod
  def inE(*args):
    return PythonGraphTraversal("__").inE(*args)
  @staticmethod
  def property(*args):
    return PythonGraphTraversal("__").property(*args)
  @staticmethod
  def both(*args):
    return PythonGraphTraversal("__").both(*args)
  @staticmethod
  def bothE(*args):
    return PythonGraphTraversal("__").bothE(*args)
  @staticmethod
  def inV(*args):
    return PythonGraphTraversal("__").inV(*args)
  @staticmethod
  def outV(*args):
    return PythonGraphTraversal("__").outV(*args)
  @staticmethod
  def bothV(*args):
    return PythonGraphTraversal("__").bothV(*args)
  @staticmethod
  def otherV(*args):
    return PythonGraphTraversal("__").otherV(*args)
  @staticmethod
  def valueMap(*args):
    return PythonGraphTraversal("__").valueMap(*args)
  @staticmethod
  def mapValues(*args):
    return PythonGraphTraversal("__").mapValues(*args)
  @staticmethod
  def mapKeys(*args):
    return PythonGraphTraversal("__").mapKeys(*args)
  @staticmethod
  def sack(*args):
    return PythonGraphTraversal("__").sack(*args)
  @staticmethod
  def loops(*args):
    return PythonGraphTraversal("__").loops(*args)
  @staticmethod
  def project(*args):
    return PythonGraphTraversal("__").project(*args)
  @staticmethod
  def unfold(*args):
    return PythonGraphTraversal("__").unfold(*args)
  @staticmethod
  def fold(*args):
    return PythonGraphTraversal("__").fold(*args)
  @staticmethod
  def mean(*args):
    return PythonGraphTraversal("__").mean(*args)
  @staticmethod
  def groupV3d0(*args):
    return PythonGraphTraversal("__").groupV3d0(*args)
  @staticmethod
  def addV(*args):
    return PythonGraphTraversal("__").addV(*args)
  @staticmethod
  def addE(*args):
    return PythonGraphTraversal("__").addE(*args)
  @staticmethod
  def addOutE(*args):
    return PythonGraphTraversal("__").addOutE(*args)
  @staticmethod
  def addInE(*args):
    return PythonGraphTraversal("__").addInE(*args)
  @staticmethod
  def dedup(*args):
    return PythonGraphTraversal("__").dedup(*args)
  @staticmethod
  def where(*args):
    return PythonGraphTraversal("__").where(*args)
  @staticmethod
  def hasNot(*args):
    return PythonGraphTraversal("__").hasNot(*args)
  @staticmethod
  def hasLabel(*args):
    return PythonGraphTraversal("__").hasLabel(*args)
  @staticmethod
  def hasId(*args):
    return PythonGraphTraversal("__").hasId(*args)
  @staticmethod
  def hasKey(*args):
    return PythonGraphTraversal("__").hasKey(*args)
  @staticmethod
  def hasValue(*args):
    return PythonGraphTraversal("__").hasValue(*args)
  @staticmethod
  def coin(*args):
    return PythonGraphTraversal("__").coin(*args)
  @staticmethod
  def timeLimit(*args):
    return PythonGraphTraversal("__").timeLimit(*args)
  @staticmethod
  def simplePath(*args):
    return PythonGraphTraversal("__").simplePath(*args)
  @staticmethod
  def cyclicPath(*args):
    return PythonGraphTraversal("__").cyclicPath(*args)
  @staticmethod
  def sample(*args):
    return PythonGraphTraversal("__").sample(*args)
  @staticmethod
  def sideEffect(*args):
    return PythonGraphTraversal("__").sideEffect(*args)
  @staticmethod
  def subgraph(*args):
    return PythonGraphTraversal("__").subgraph(*args)
  @staticmethod
  def aggregate(*args):
    return PythonGraphTraversal("__").aggregate(*args)
  @staticmethod
  def branch(*args):
    return PythonGraphTraversal("__").branch(*args)
  @staticmethod
  def optional(*args):
    return PythonGraphTraversal("__").optional(*args)
  @staticmethod
  def coalesce(*args):
    return PythonGraphTraversal("__").coalesce(*args)
  @staticmethod
  def repeat(*args):
    return PythonGraphTraversal("__").repeat(*args)
  @staticmethod
  def emit(*args):
    return PythonGraphTraversal("__").emit(*args)
  @staticmethod
  def local(*args):
    return PythonGraphTraversal("__").local(*args)
  @staticmethod
  def barrier(*args):
    return PythonGraphTraversal("__").barrier(*args)
  @staticmethod
  def choose(*args):
    return PythonGraphTraversal("__").choose(*args)
  @staticmethod
  def cap(*args):
    return PythonGraphTraversal("__").cap(*args)
  @staticmethod
  def inject(*args):
    return PythonGraphTraversal("__").inject(*args)
  @staticmethod
  def drop(*args):
    return PythonGraphTraversal("__").drop(*args)
  @staticmethod
  def times(*args):
    return PythonGraphTraversal("__").times(*args)
  @staticmethod
  def _as(*args):
    return PythonGraphTraversal("__")._as(*args)
  @staticmethod
  def label(*args):
    return PythonGraphTraversal("__").label(*args)
  @staticmethod
  def propertyMap(*args):
    return PythonGraphTraversal("__").propertyMap(*args)
  @staticmethod
  def constant(*args):
    return PythonGraphTraversal("__").constant(*args)
  @staticmethod
  def until(*args):
    return PythonGraphTraversal("__").until(*args)
  @staticmethod
  def __(*args):
    return PythonGraphTraversal("__").__(*args)
  @staticmethod
  def has(*args):
    return PythonGraphTraversal("__").has(*args)
  @staticmethod
  def union(*args):
    return PythonGraphTraversal("__").union(*args)
  @staticmethod
  def groupCount(*args):
    return PythonGraphTraversal("__").groupCount(*args)
  @staticmethod
  def flatMap(*args):
    return PythonGraphTraversal("__").flatMap(*args)
  @staticmethod
  def properties(*args):
    return PythonGraphTraversal("__").properties(*args)
  @staticmethod
  def to(*args):
    return PythonGraphTraversal("__").to(*args)
  @staticmethod
  def _not(*args):
    return PythonGraphTraversal("__")._not(*args)
  @staticmethod
  def _is(*args):
    return PythonGraphTraversal("__")._is(*args)
  @staticmethod
  def select(*args):
    return PythonGraphTraversal("__").select(*args)
  @staticmethod
  def wait(*args):
    return PythonGraphTraversal("__").wait(*args)
  @staticmethod
  def equals(*args):
    return PythonGraphTraversal("__").equals(*args)
  @staticmethod
  def toString(*args):
    return PythonGraphTraversal("__").toString(*args)
  @staticmethod
  def hashCode(*args):
    return PythonGraphTraversal("__").hashCode(*args)
  @staticmethod
  def getClass(*args):
    return PythonGraphTraversal("__").getClass(*args)
  @staticmethod
  def notify(*args):
    return PythonGraphTraversal("__").notify(*args)
  @staticmethod
  def notifyAll(*args):
    return PythonGraphTraversal("__").notifyAll(*args)


