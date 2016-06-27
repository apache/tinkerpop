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
from statics import add_static

class PythonTraversal(object):
    def __init__(self, translator, remote_connection=None):
        self.translator = translator
        self.remote_connection = remote_connection
        self.results = None
        self.last_traverser = None
        self.bindings = {}

    def __repr__(self):
        return self.translator.traversal_script

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
            self.results = self.remote_connection.submit(self.translator.target_language,
                                                         self.translator.traversal_script, self.bindings)
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

add_static('normSack', Barrier.normSack)

Cardinality = Enum('Cardinality', 'list set single')

add_static('single', Cardinality.single)
add_static('list', Cardinality.list)
add_static('set', Cardinality.set)

Column = Enum('Column', 'keys values')

add_static('keys', Column.keys)
add_static('values', Column.values)

Direction = Enum('Direction', 'BOTH IN OUT')

add_static('OUT', Direction.OUT)
add_static('IN', Direction.IN)
add_static('BOTH', Direction.BOTH)

Operator = Enum('Operator', 'addAll _and assign div max min minus mult _or sum sumLong')

add_static('sum', Operator.sum)
add_static('minus', Operator.minus)
add_static('mult', Operator.mult)
add_static('div', Operator.div)
add_static('min', Operator.min)
add_static('max', Operator.max)
add_static('assign', Operator.assign)
add_static('_and', Operator._and)
add_static('_or', Operator._or)
add_static('addAll', Operator.addAll)
add_static('sumLong', Operator.sumLong)

Order = Enum('Order', 'decr incr keyDecr keyIncr shuffle valueDecr valueIncr')

add_static('incr', Order.incr)
add_static('decr', Order.decr)
add_static('keyIncr', Order.keyIncr)
add_static('valueIncr', Order.valueIncr)
add_static('keyDecr', Order.keyDecr)
add_static('valueDecr', Order.valueDecr)
add_static('shuffle', Order.shuffle)

Pop = Enum('Pop', 'all first last')

add_static('first', Pop.first)
add_static('last', Pop.last)
add_static('all', Pop.all)

Scope = Enum('Scope', '_global local')

add_static('_global', Scope._global)
add_static('local', Scope.local)

T = Enum('T', 'id key label value')

add_static('label', T.label)
add_static('id', T.id)
add_static('key', T.key)
add_static('value', T.value)

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

add_static('_not',_not)
def between(*args):
      return P.between(*args)

add_static('between',between)
def eq(*args):
      return P.eq(*args)

add_static('eq',eq)
def gt(*args):
      return P.gt(*args)

add_static('gt',gt)
def gte(*args):
      return P.gte(*args)

add_static('gte',gte)
def inside(*args):
      return P.inside(*args)

add_static('inside',inside)
def lt(*args):
      return P.lt(*args)

add_static('lt',lt)
def lte(*args):
      return P.lte(*args)

add_static('lte',lte)
def neq(*args):
      return P.neq(*args)

add_static('neq',neq)
def outside(*args):
      return P.outside(*args)

add_static('outside',outside)
def test(*args):
      return P.test(*args)

add_static('test',test)
def within(*args):
      return P.within(*args)

add_static('within',within)
def without(*args):
      return P.without(*args)

add_static('without',without)

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

