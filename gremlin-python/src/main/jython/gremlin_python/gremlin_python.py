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
from aenum import Enum
statics = OrderedDict()


globalTranslator = None


class PythonGraphTraversalSource(object):
  def __init__(self, translator, remote_connection=None):
    global globalTranslator
    self.translator = translator
    globalTranslator = translator
    self.remote_connection = remote_connection
  def __repr__(self):
    return "graphtraversalsource[" + str(self.remote_connection) + ", " + self.translator.traversal_script + "]"
  def E(self, *args):
    traversal = PythonGraphTraversal(self.translator, self.remote_connection)
    traversal.translator.addSpawnStep(traversal, "E", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return traversal
  def V(self, *args):
    traversal = PythonGraphTraversal(self.translator, self.remote_connection)
    traversal.translator.addSpawnStep(traversal, "V", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return traversal
  def addV(self, *args):
    traversal = PythonGraphTraversal(self.translator, self.remote_connection)
    traversal.translator.addSpawnStep(traversal, "addV", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return traversal
  def inject(self, *args):
    traversal = PythonGraphTraversal(self.translator, self.remote_connection)
    traversal.translator.addSpawnStep(traversal, "inject", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return traversal
  def withBulk(self, *args):
    source = PythonGraphTraversalSource(self.translator, self.remote_connection)
    source.translator.addSource(source, "withBulk", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return source
  def withComputer(self, *args):
    source = PythonGraphTraversalSource(self.translator, self.remote_connection)
    source.translator.addSource(source, "withComputer", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return source
  def withPath(self, *args):
    source = PythonGraphTraversalSource(self.translator, self.remote_connection)
    source.translator.addSource(source, "withPath", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return source
  def withSack(self, *args):
    source = PythonGraphTraversalSource(self.translator, self.remote_connection)
    source.translator.addSource(source, "withSack", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return source
  def withSideEffect(self, *args):
    source = PythonGraphTraversalSource(self.translator, self.remote_connection)
    source.translator.addSource(source, "withSideEffect", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return source
  def withStrategies(self, *args):
    source = PythonGraphTraversalSource(self.translator, self.remote_connection)
    source.translator.addSource(source, "withStrategies", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return source
  def withTranslator(self, *args):
    source = PythonGraphTraversalSource(self.translator, self.remote_connection)
    source.translator.addSource(source, "withTranslator", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return source
  def withoutStrategies(self, *args):
    source = PythonGraphTraversalSource(self.translator, self.remote_connection)
    source.translator.addSource(source, "withoutStrategies", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return source


class PythonGraphTraversal(object):
  def __init__(self, translator, remote_connection=None):
    self.translator = translator
    self.remote_connection = remote_connection
    self.results = None
    self.last_traverser = None
    self.bindings = {}
  def __repr__(self):
    return self.translator.traversal_script
  def __getitem__(self,index):
    if isinstance(index,int):
      return self.range(index,index+1)
    elif isinstance(index,slice):
      return self.range(index.start,index.stop)
    else:
      raise TypeError("Index must be int or slice")
  def __getattr__(self,key):
    return self.values(key)
  def __iter__(self):
        return self
  def __next__(self):
     return self.next()
  def toList(self):
    return list(iter(self))
  def toSet(self):
    return set(iter(self))
  def next(self,amount):
    count = 0
    tempList = []
    while count < amount:
      count = count + 1
      temp = next(self,None)
      if None == temp:
        break
      tempList.append(temp)
    return tempList
  def next(self):
     if self.results is None:
        self.results = self.remote_connection.submit(self.translator.target_language, self.translator.traversal_script, self.bindings)
     if self.last_traverser is None:
         self.last_traverser = next(self.results)
     object = self.last_traverser.object
     self.last_traverser.bulk = self.last_traverser.bulk - 1
     if self.last_traverser.bulk <= 0:
         self.last_traverser = None
     return object
  def V(self, *args):
    self.translator.addStep(self, "V", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def _and(self, *args):
    self.translator.addStep(self, "_and", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def _as(self, *args):
    self.translator.addStep(self, "_as", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def _from(self, *args):
    self.translator.addStep(self, "_from", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def _in(self, *args):
    self.translator.addStep(self, "_in", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def _is(self, *args):
    self.translator.addStep(self, "_is", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def _not(self, *args):
    self.translator.addStep(self, "_not", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def _or(self, *args):
    self.translator.addStep(self, "_or", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def addE(self, *args):
    self.translator.addStep(self, "addE", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def addInE(self, *args):
    self.translator.addStep(self, "addInE", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def addOutE(self, *args):
    self.translator.addStep(self, "addOutE", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def addV(self, *args):
    self.translator.addStep(self, "addV", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def aggregate(self, *args):
    self.translator.addStep(self, "aggregate", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def asAdmin(self, *args):
    self.translator.addStep(self, "asAdmin", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def barrier(self, *args):
    self.translator.addStep(self, "barrier", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def both(self, *args):
    self.translator.addStep(self, "both", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def bothE(self, *args):
    self.translator.addStep(self, "bothE", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def bothV(self, *args):
    self.translator.addStep(self, "bothV", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def branch(self, *args):
    self.translator.addStep(self, "branch", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def by(self, *args):
    self.translator.addStep(self, "by", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def cap(self, *args):
    self.translator.addStep(self, "cap", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def choose(self, *args):
    self.translator.addStep(self, "choose", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def coalesce(self, *args):
    self.translator.addStep(self, "coalesce", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def coin(self, *args):
    self.translator.addStep(self, "coin", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def constant(self, *args):
    self.translator.addStep(self, "constant", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def count(self, *args):
    self.translator.addStep(self, "count", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def cyclicPath(self, *args):
    self.translator.addStep(self, "cyclicPath", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def dedup(self, *args):
    self.translator.addStep(self, "dedup", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def drop(self, *args):
    self.translator.addStep(self, "drop", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def emit(self, *args):
    self.translator.addStep(self, "emit", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def filter(self, *args):
    self.translator.addStep(self, "filter", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def flatMap(self, *args):
    self.translator.addStep(self, "flatMap", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def fold(self, *args):
    self.translator.addStep(self, "fold", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def group(self, *args):
    self.translator.addStep(self, "group", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def groupCount(self, *args):
    self.translator.addStep(self, "groupCount", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def groupV3d0(self, *args):
    self.translator.addStep(self, "groupV3d0", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def has(self, *args):
    self.translator.addStep(self, "has", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def hasId(self, *args):
    self.translator.addStep(self, "hasId", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def hasKey(self, *args):
    self.translator.addStep(self, "hasKey", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def hasLabel(self, *args):
    self.translator.addStep(self, "hasLabel", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def hasNot(self, *args):
    self.translator.addStep(self, "hasNot", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def hasValue(self, *args):
    self.translator.addStep(self, "hasValue", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def id(self, *args):
    self.translator.addStep(self, "id", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def identity(self, *args):
    self.translator.addStep(self, "identity", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def inE(self, *args):
    self.translator.addStep(self, "inE", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def inV(self, *args):
    self.translator.addStep(self, "inV", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def inject(self, *args):
    self.translator.addStep(self, "inject", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def iterate(self, *args):
    self.translator.addStep(self, "iterate", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def key(self, *args):
    self.translator.addStep(self, "key", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def label(self, *args):
    self.translator.addStep(self, "label", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def limit(self, *args):
    self.translator.addStep(self, "limit", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def local(self, *args):
    self.translator.addStep(self, "local", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def loops(self, *args):
    self.translator.addStep(self, "loops", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def map(self, *args):
    self.translator.addStep(self, "map", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def mapKeys(self, *args):
    self.translator.addStep(self, "mapKeys", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def mapValues(self, *args):
    self.translator.addStep(self, "mapValues", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def match(self, *args):
    self.translator.addStep(self, "match", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def max(self, *args):
    self.translator.addStep(self, "max", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def mean(self, *args):
    self.translator.addStep(self, "mean", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def min(self, *args):
    self.translator.addStep(self, "min", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def option(self, *args):
    self.translator.addStep(self, "option", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def optional(self, *args):
    self.translator.addStep(self, "optional", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def order(self, *args):
    self.translator.addStep(self, "order", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def otherV(self, *args):
    self.translator.addStep(self, "otherV", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def out(self, *args):
    self.translator.addStep(self, "out", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def outE(self, *args):
    self.translator.addStep(self, "outE", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def outV(self, *args):
    self.translator.addStep(self, "outV", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def pageRank(self, *args):
    self.translator.addStep(self, "pageRank", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def path(self, *args):
    self.translator.addStep(self, "path", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def peerPressure(self, *args):
    self.translator.addStep(self, "peerPressure", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def profile(self, *args):
    self.translator.addStep(self, "profile", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def program(self, *args):
    self.translator.addStep(self, "program", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def project(self, *args):
    self.translator.addStep(self, "project", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def properties(self, *args):
    self.translator.addStep(self, "properties", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def property(self, *args):
    self.translator.addStep(self, "property", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def propertyMap(self, *args):
    self.translator.addStep(self, "propertyMap", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def range(self, *args):
    self.translator.addStep(self, "range", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def repeat(self, *args):
    self.translator.addStep(self, "repeat", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def sack(self, *args):
    self.translator.addStep(self, "sack", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def sample(self, *args):
    self.translator.addStep(self, "sample", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def select(self, *args):
    self.translator.addStep(self, "select", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def sideEffect(self, *args):
    self.translator.addStep(self, "sideEffect", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def simplePath(self, *args):
    self.translator.addStep(self, "simplePath", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def store(self, *args):
    self.translator.addStep(self, "store", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def subgraph(self, *args):
    self.translator.addStep(self, "subgraph", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def sum(self, *args):
    self.translator.addStep(self, "sum", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def tail(self, *args):
    self.translator.addStep(self, "tail", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def timeLimit(self, *args):
    self.translator.addStep(self, "timeLimit", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def times(self, *args):
    self.translator.addStep(self, "times", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def to(self, *args):
    self.translator.addStep(self, "to", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def toE(self, *args):
    self.translator.addStep(self, "toE", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def toV(self, *args):
    self.translator.addStep(self, "toV", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def tree(self, *args):
    self.translator.addStep(self, "tree", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def unfold(self, *args):
    self.translator.addStep(self, "unfold", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def union(self, *args):
    self.translator.addStep(self, "union", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def until(self, *args):
    self.translator.addStep(self, "until", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def value(self, *args):
    self.translator.addStep(self, "value", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def valueMap(self, *args):
    self.translator.addStep(self, "valueMap", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def values(self, *args):
    self.translator.addStep(self, "values", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
  def where(self, *args):
    self.translator.addStep(self, "where", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self


class __(object):
  @staticmethod
  def V(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).V(*args)
  @staticmethod
  def __(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).__(*args)
  @staticmethod
  def _and(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator())._and(*args)
  @staticmethod
  def _as(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator())._as(*args)
  @staticmethod
  def _in(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator())._in(*args)
  @staticmethod
  def _is(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator())._is(*args)
  @staticmethod
  def _not(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator())._not(*args)
  @staticmethod
  def _or(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator())._or(*args)
  @staticmethod
  def addE(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).addE(*args)
  @staticmethod
  def addInE(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).addInE(*args)
  @staticmethod
  def addOutE(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).addOutE(*args)
  @staticmethod
  def addV(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).addV(*args)
  @staticmethod
  def aggregate(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).aggregate(*args)
  @staticmethod
  def barrier(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).barrier(*args)
  @staticmethod
  def both(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).both(*args)
  @staticmethod
  def bothE(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).bothE(*args)
  @staticmethod
  def bothV(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).bothV(*args)
  @staticmethod
  def branch(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).branch(*args)
  @staticmethod
  def cap(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).cap(*args)
  @staticmethod
  def choose(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).choose(*args)
  @staticmethod
  def coalesce(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).coalesce(*args)
  @staticmethod
  def coin(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).coin(*args)
  @staticmethod
  def constant(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).constant(*args)
  @staticmethod
  def count(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).count(*args)
  @staticmethod
  def cyclicPath(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).cyclicPath(*args)
  @staticmethod
  def dedup(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).dedup(*args)
  @staticmethod
  def drop(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).drop(*args)
  @staticmethod
  def emit(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).emit(*args)
  @staticmethod
  def filter(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).filter(*args)
  @staticmethod
  def flatMap(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).flatMap(*args)
  @staticmethod
  def fold(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).fold(*args)
  @staticmethod
  def group(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).group(*args)
  @staticmethod
  def groupCount(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).groupCount(*args)
  @staticmethod
  def groupV3d0(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).groupV3d0(*args)
  @staticmethod
  def has(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).has(*args)
  @staticmethod
  def hasId(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).hasId(*args)
  @staticmethod
  def hasKey(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).hasKey(*args)
  @staticmethod
  def hasLabel(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).hasLabel(*args)
  @staticmethod
  def hasNot(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).hasNot(*args)
  @staticmethod
  def hasValue(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).hasValue(*args)
  @staticmethod
  def id(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).id(*args)
  @staticmethod
  def identity(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).identity(*args)
  @staticmethod
  def inE(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).inE(*args)
  @staticmethod
  def inV(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).inV(*args)
  @staticmethod
  def inject(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).inject(*args)
  @staticmethod
  def key(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).key(*args)
  @staticmethod
  def label(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).label(*args)
  @staticmethod
  def limit(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).limit(*args)
  @staticmethod
  def local(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).local(*args)
  @staticmethod
  def loops(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).loops(*args)
  @staticmethod
  def map(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).map(*args)
  @staticmethod
  def mapKeys(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).mapKeys(*args)
  @staticmethod
  def mapValues(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).mapValues(*args)
  @staticmethod
  def match(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).match(*args)
  @staticmethod
  def max(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).max(*args)
  @staticmethod
  def mean(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).mean(*args)
  @staticmethod
  def min(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).min(*args)
  @staticmethod
  def optional(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).optional(*args)
  @staticmethod
  def order(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).order(*args)
  @staticmethod
  def otherV(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).otherV(*args)
  @staticmethod
  def out(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).out(*args)
  @staticmethod
  def outE(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).outE(*args)
  @staticmethod
  def outV(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).outV(*args)
  @staticmethod
  def path(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).path(*args)
  @staticmethod
  def project(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).project(*args)
  @staticmethod
  def properties(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).properties(*args)
  @staticmethod
  def property(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).property(*args)
  @staticmethod
  def propertyMap(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).propertyMap(*args)
  @staticmethod
  def range(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).range(*args)
  @staticmethod
  def repeat(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).repeat(*args)
  @staticmethod
  def sack(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).sack(*args)
  @staticmethod
  def sample(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).sample(*args)
  @staticmethod
  def select(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).select(*args)
  @staticmethod
  def sideEffect(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).sideEffect(*args)
  @staticmethod
  def simplePath(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).simplePath(*args)
  @staticmethod
  def start(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).start(*args)
  @staticmethod
  def store(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).store(*args)
  @staticmethod
  def subgraph(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).subgraph(*args)
  @staticmethod
  def sum(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).sum(*args)
  @staticmethod
  def tail(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).tail(*args)
  @staticmethod
  def timeLimit(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).timeLimit(*args)
  @staticmethod
  def times(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).times(*args)
  @staticmethod
  def to(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).to(*args)
  @staticmethod
  def toE(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).toE(*args)
  @staticmethod
  def toV(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).toV(*args)
  @staticmethod
  def tree(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).tree(*args)
  @staticmethod
  def unfold(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).unfold(*args)
  @staticmethod
  def union(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).union(*args)
  @staticmethod
  def until(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).until(*args)
  @staticmethod
  def value(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).value(*args)
  @staticmethod
  def valueMap(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).valueMap(*args)
  @staticmethod
  def values(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).values(*args)
  @staticmethod
  def where(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).where(*args)


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


Barrier = Enum('Barrier', 'normSack')

statics['normSack'] = Barrier.normSack

Cardinality = Enum('Cardinality', 'list set single')

statics['single'] = Cardinality.single
statics['list'] = Cardinality.list
statics['set'] = Cardinality.set

Column = Enum('Column', 'keys values')

statics['keys'] = Column.keys
statics['values'] = Column.values

Direction = Enum('Direction', 'BOTH IN OUT')

statics['OUT'] = Direction.OUT
statics['IN'] = Direction.IN
statics['BOTH'] = Direction.BOTH

Operator = Enum('Operator', 'addAll _and assign div max min minus mult _or sum sumLong')

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

Order = Enum('Order', 'decr incr keyDecr keyIncr shuffle valueDecr valueIncr')

statics['incr'] = Order.incr
statics['decr'] = Order.decr
statics['keyIncr'] = Order.keyIncr
statics['valueIncr'] = Order.valueIncr
statics['keyDecr'] = Order.keyDecr
statics['valueDecr'] = Order.valueDecr
statics['shuffle'] = Order.shuffle

Pop = Enum('Pop', 'all first last')

statics['first'] = Pop.first
statics['last'] = Pop.last
statics['all'] = Pop.all

Scope = Enum('Scope', '_global local')

statics['_global'] = Scope._global
statics['local'] = Scope.local

T = Enum('T', 'id key label value')

statics['label'] = T.label
statics['id'] = T.id
statics['key'] = T.key
statics['value'] = T.value

class P(object):
   def __init__(self, operator, value, other=None):
      self.operator = operator
      self.value = value
      self.other = other
   @staticmethod
   def _not(*args):
      return P("not", *args)
   @staticmethod
   def between(*args):
      return P("between", *args)
   @staticmethod
   def eq(*args):
      return P("eq", *args)
   @staticmethod
   def gt(*args):
      return P("gt", *args)
   @staticmethod
   def gte(*args):
      return P("gte", *args)
   @staticmethod
   def inside(*args):
      return P("inside", *args)
   @staticmethod
   def lt(*args):
      return P("lt", *args)
   @staticmethod
   def lte(*args):
      return P("lte", *args)
   @staticmethod
   def neq(*args):
      return P("neq", *args)
   @staticmethod
   def outside(*args):
      return P("outside", *args)
   @staticmethod
   def test(*args):
      return P("test", *args)
   @staticmethod
   def within(*args):
      return P("within", *args)
   @staticmethod
   def without(*args):
      return P("without", *args)
   def _and(self, arg):
      return P("_and", arg, self)
   def _or(self, arg):
      return P("_or", arg, self)

def _not(*args):
      return P._not(*args)

statics['_not'] = _not
def between(*args):
      return P.between(*args)

statics['between'] = between
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

class RawExpression(object):
   def __init__(self, *args):
      self.bindings = dict()
      self.parts = [self._process_arg(arg) for arg in args]

   def _process_arg(self, arg):
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
         self.bindings[arg[0]] = arg[1]
         return Raw(arg[0])
      else:
         return Raw(arg)

class Raw(object):
   def __init__(self, value):
      self.value = value

   def __str__(self):
      return str(self.value)

statics = OrderedDict(reversed(list(statics.items())))
