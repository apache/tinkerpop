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
from aenum import Enum
from traversal import RawExpression
from traversal import PythonTraversal
from statics import add_static
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


class PythonGraphTraversal(PythonTraversal):
  def __init__(self, translator, remote_connection=None):
    PythonTraversal.__init__(self, translator, remote_connection)
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

add_static('V', V)
def _and(*args):
      return __._and(*args)

add_static('_and', _and)
def _as(*args):
      return __._as(*args)

add_static('_as', _as)
def _in(*args):
      return __._in(*args)

add_static('_in', _in)
def _is(*args):
      return __._is(*args)

add_static('_is', _is)
def _not(*args):
      return __._not(*args)

add_static('_not', _not)
def _or(*args):
      return __._or(*args)

add_static('_or', _or)
def addE(*args):
      return __.addE(*args)

add_static('addE', addE)
def addInE(*args):
      return __.addInE(*args)

add_static('addInE', addInE)
def addOutE(*args):
      return __.addOutE(*args)

add_static('addOutE', addOutE)
def addV(*args):
      return __.addV(*args)

add_static('addV', addV)
def aggregate(*args):
      return __.aggregate(*args)

add_static('aggregate', aggregate)
def barrier(*args):
      return __.barrier(*args)

add_static('barrier', barrier)
def both(*args):
      return __.both(*args)

add_static('both', both)
def bothE(*args):
      return __.bothE(*args)

add_static('bothE', bothE)
def bothV(*args):
      return __.bothV(*args)

add_static('bothV', bothV)
def branch(*args):
      return __.branch(*args)

add_static('branch', branch)
def cap(*args):
      return __.cap(*args)

add_static('cap', cap)
def choose(*args):
      return __.choose(*args)

add_static('choose', choose)
def coalesce(*args):
      return __.coalesce(*args)

add_static('coalesce', coalesce)
def coin(*args):
      return __.coin(*args)

add_static('coin', coin)
def constant(*args):
      return __.constant(*args)

add_static('constant', constant)
def count(*args):
      return __.count(*args)

add_static('count', count)
def cyclicPath(*args):
      return __.cyclicPath(*args)

add_static('cyclicPath', cyclicPath)
def dedup(*args):
      return __.dedup(*args)

add_static('dedup', dedup)
def drop(*args):
      return __.drop(*args)

add_static('drop', drop)
def emit(*args):
      return __.emit(*args)

add_static('emit', emit)
def filter(*args):
      return __.filter(*args)

add_static('filter', filter)
def flatMap(*args):
      return __.flatMap(*args)

add_static('flatMap', flatMap)
def fold(*args):
      return __.fold(*args)

add_static('fold', fold)
def group(*args):
      return __.group(*args)

add_static('group', group)
def groupCount(*args):
      return __.groupCount(*args)

add_static('groupCount', groupCount)
def groupV3d0(*args):
      return __.groupV3d0(*args)

add_static('groupV3d0', groupV3d0)
def has(*args):
      return __.has(*args)

add_static('has', has)
def hasId(*args):
      return __.hasId(*args)

add_static('hasId', hasId)
def hasKey(*args):
      return __.hasKey(*args)

add_static('hasKey', hasKey)
def hasLabel(*args):
      return __.hasLabel(*args)

add_static('hasLabel', hasLabel)
def hasNot(*args):
      return __.hasNot(*args)

add_static('hasNot', hasNot)
def hasValue(*args):
      return __.hasValue(*args)

add_static('hasValue', hasValue)
def id(*args):
      return __.id(*args)

add_static('id', id)
def identity(*args):
      return __.identity(*args)

add_static('identity', identity)
def inE(*args):
      return __.inE(*args)

add_static('inE', inE)
def inV(*args):
      return __.inV(*args)

add_static('inV', inV)
def inject(*args):
      return __.inject(*args)

add_static('inject', inject)
def key(*args):
      return __.key(*args)

add_static('key', key)
def label(*args):
      return __.label(*args)

add_static('label', label)
def limit(*args):
      return __.limit(*args)

add_static('limit', limit)
def local(*args):
      return __.local(*args)

add_static('local', local)
def loops(*args):
      return __.loops(*args)

add_static('loops', loops)
def map(*args):
      return __.map(*args)

add_static('map', map)
def mapKeys(*args):
      return __.mapKeys(*args)

add_static('mapKeys', mapKeys)
def mapValues(*args):
      return __.mapValues(*args)

add_static('mapValues', mapValues)
def match(*args):
      return __.match(*args)

add_static('match', match)
def max(*args):
      return __.max(*args)

add_static('max', max)
def mean(*args):
      return __.mean(*args)

add_static('mean', mean)
def min(*args):
      return __.min(*args)

add_static('min', min)
def optional(*args):
      return __.optional(*args)

add_static('optional', optional)
def order(*args):
      return __.order(*args)

add_static('order', order)
def otherV(*args):
      return __.otherV(*args)

add_static('otherV', otherV)
def out(*args):
      return __.out(*args)

add_static('out', out)
def outE(*args):
      return __.outE(*args)

add_static('outE', outE)
def outV(*args):
      return __.outV(*args)

add_static('outV', outV)
def path(*args):
      return __.path(*args)

add_static('path', path)
def project(*args):
      return __.project(*args)

add_static('project', project)
def properties(*args):
      return __.properties(*args)

add_static('properties', properties)
def property(*args):
      return __.property(*args)

add_static('property', property)
def propertyMap(*args):
      return __.propertyMap(*args)

add_static('propertyMap', propertyMap)
def range(*args):
      return __.range(*args)

add_static('range', range)
def repeat(*args):
      return __.repeat(*args)

add_static('repeat', repeat)
def sack(*args):
      return __.sack(*args)

add_static('sack', sack)
def sample(*args):
      return __.sample(*args)

add_static('sample', sample)
def select(*args):
      return __.select(*args)

add_static('select', select)
def sideEffect(*args):
      return __.sideEffect(*args)

add_static('sideEffect', sideEffect)
def simplePath(*args):
      return __.simplePath(*args)

add_static('simplePath', simplePath)
def start(*args):
      return __.start(*args)

add_static('start', start)
def store(*args):
      return __.store(*args)

add_static('store', store)
def subgraph(*args):
      return __.subgraph(*args)

add_static('subgraph', subgraph)
def sum(*args):
      return __.sum(*args)

add_static('sum', sum)
def tail(*args):
      return __.tail(*args)

add_static('tail', tail)
def timeLimit(*args):
      return __.timeLimit(*args)

add_static('timeLimit', timeLimit)
def times(*args):
      return __.times(*args)

add_static('times', times)
def to(*args):
      return __.to(*args)

add_static('to', to)
def toE(*args):
      return __.toE(*args)

add_static('toE', toE)
def toV(*args):
      return __.toV(*args)

add_static('toV', toV)
def tree(*args):
      return __.tree(*args)

add_static('tree', tree)
def unfold(*args):
      return __.unfold(*args)

add_static('unfold', unfold)
def union(*args):
      return __.union(*args)

add_static('union', union)
def until(*args):
      return __.until(*args)

add_static('until', until)
def value(*args):
      return __.value(*args)

add_static('value', value)
def valueMap(*args):
      return __.valueMap(*args)

add_static('valueMap', valueMap)
def values(*args):
      return __.values(*args)

add_static('values', values)
def where(*args):
      return __.where(*args)

add_static('where', where)


