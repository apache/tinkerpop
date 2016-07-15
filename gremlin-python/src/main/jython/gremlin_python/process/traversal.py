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
from abc import abstractmethod
from aenum import Enum
from gremlin_python import statics

class Traversal(object):
    def __init__(self, graph, traversal_strategies, bytecode):
        self.graph = graph
        self.traversal_strategies = traversal_strategies
        self.bytecode = bytecode
        self.results = None
        self.last_traverser = None
        self.bindings = {}

    def __repr__(self):
        return str(self.bytecode)

    def __getitem__(self, index):
        if isinstance(index, int):
            return self.range(index, index + 1)
        elif isinstance(index, slice):
            return self.range(index.start, index.stop)
        else:
            raise TypeError("Index must be int or slice")

    def __getattr__(self, key):
        return self.values(key)

    def __iter__(self):
        return self

    def __next__(self):
        if self.results is None:
            self.traversal_strategies.apply_strategies(self)
        if self.last_traverser is None:
            self.last_traverser = next(self.results)
        object = self.last_traverser.object
        self.last_traverser.bulk = self.last_traverser.bulk - 1
        if self.last_traverser.bulk <= 0:
            self.last_traverser = None
        return object

    def toList(self):
        return list(iter(self))

    def toSet(self):
        return set(iter(self))

    def next(self, amount=None):
        if amount is None:
            return self.__next__()
        else:
            count = 0
            tempList = []
            while count < amount:
                count = count + 1
                try: temp = self.__next__()
                except StopIteration: return tempList
                tempList.append(temp)
            return tempList

Barrier = Enum('Barrier', 'normSack')

statics.add_static('normSack', Barrier.normSack)

Cardinality = Enum('Cardinality', 'list set single')

statics.add_static('single', Cardinality.single)
statics.add_static('list', Cardinality.list)
statics.add_static('set', Cardinality.set)

Column = Enum('Column', 'keys values')

statics.add_static('keys', Column.keys)
statics.add_static('values', Column.values)

Direction = Enum('Direction', 'BOTH IN OUT')

statics.add_static('OUT', Direction.OUT)
statics.add_static('IN', Direction.IN)
statics.add_static('BOTH', Direction.BOTH)

Operator = Enum('Operator', 'addAll _and assign div max min minus mult _or sum sumLong')

statics.add_static('sum', Operator.sum)
statics.add_static('minus', Operator.minus)
statics.add_static('mult', Operator.mult)
statics.add_static('div', Operator.div)
statics.add_static('min', Operator.min)
statics.add_static('max', Operator.max)
statics.add_static('assign', Operator.assign)
statics.add_static('_and', Operator._and)
statics.add_static('_or', Operator._or)
statics.add_static('addAll', Operator.addAll)
statics.add_static('sumLong', Operator.sumLong)

Order = Enum('Order', 'decr incr keyDecr keyIncr shuffle valueDecr valueIncr')

statics.add_static('incr', Order.incr)
statics.add_static('decr', Order.decr)
statics.add_static('keyIncr', Order.keyIncr)
statics.add_static('valueIncr', Order.valueIncr)
statics.add_static('keyDecr', Order.keyDecr)
statics.add_static('valueDecr', Order.valueDecr)
statics.add_static('shuffle', Order.shuffle)

Pop = Enum('Pop', 'all first last')

statics.add_static('first', Pop.first)
statics.add_static('last', Pop.last)
statics.add_static('all', Pop.all)

Scope = Enum('Scope', '_global local')

statics.add_static('_global', Scope._global)
statics.add_static('local', Scope.local)

T = Enum('T', 'id key label value')

statics.add_static('label', T.label)
statics.add_static('id', T.id)
statics.add_static('key', T.key)
statics.add_static('value', T.value)

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

statics.add_static('_not',_not)

def between(*args):
      return P.between(*args)

statics.add_static('between',between)

def eq(*args):
      return P.eq(*args)

statics.add_static('eq',eq)

def gt(*args):
      return P.gt(*args)

statics.add_static('gt',gt)

def gte(*args):
      return P.gte(*args)

statics.add_static('gte',gte)

def inside(*args):
      return P.inside(*args)

statics.add_static('inside',inside)

def lt(*args):
      return P.lt(*args)

statics.add_static('lt',lt)

def lte(*args):
      return P.lte(*args)

statics.add_static('lte',lte)

def neq(*args):
      return P.neq(*args)

statics.add_static('neq',neq)

def outside(*args):
      return P.outside(*args)

statics.add_static('outside',outside)

def test(*args):
      return P.test(*args)

statics.add_static('test',test)

def within(*args):
      return P.within(*args)

statics.add_static('within',within)

def without(*args):
      return P.without(*args)

statics.add_static('without',without)


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


'''
TRAVERSER
'''

class Traverser(object):
    def __init__(self, object, bulk):
        self.object = object
        self.bulk = bulk
    def __repr__(self):
        return str(self.object)

'''
TRAVERSAL STRATEGIES
'''

class TraversalStrategies(object):
    global_cache = {}

    def __init__(self, traversal_strategies):
        self.traversal_strategies = traversal_strategies
        return

    def apply_strategies(self, traversal):
        for traversal_strategy in self.traversal_strategies:
            traversal_strategy.apply(traversal)
        return


class TraversalStrategy(object):
    @abstractmethod
    def apply(self, traversal):
        return

'''
BYTECODE
'''

class Bytecode(object):
    def __init__(self, bytecode=None):
        self.source_instructions = []
        self.step_instructions = []
        if bytecode is not None:
            self.source_instructions = list(bytecode.source_instructions)
            self.step_instructions = list(bytecode.step_instructions)

    def add_source(self, source_name, *args):
        newArgs = ()
        for arg in args:
            newArgs = newArgs + (Bytecode.__convertArgument(arg),)
        self.source_instructions.append((source_name, newArgs))
        return

    def add_step(self, step_name, *args):
        newArgs = ()
        for arg in args:
            newArgs = newArgs + (Bytecode.__convertArgument(arg),)
        self.step_instructions.append((step_name, newArgs))
        return

    @staticmethod
    def __convertArgument(arg):
        if isinstance(arg, Traversal):
            return arg.bytecode
        else:
            return arg
