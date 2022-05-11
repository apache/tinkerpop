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

import copy
from aenum import Enum
from .. import statics
from ..statics import long

class Traversal(object):
    def __init__(self, graph, traversal_strategies, bytecode):
        self.graph = graph
        self.traversal_strategies = traversal_strategies
        self.bytecode = bytecode
        self.traversers = None
        self.last_traverser = None

    def __repr__(self):
        return str(self.bytecode)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.bytecode == other.bytecode
        else:
            return False

    def __iter__(self):
        return self

    def __next__(self):
        if self.traversers is None:
            self.traversal_strategies.apply_strategies(self)
        if self.last_traverser is None:
            self.last_traverser = next(self.traversers)
        object = self.last_traverser.object
        self.last_traverser.bulk = self.last_traverser.bulk - 1
        if self.last_traverser.bulk <= 0:
            self.last_traverser = None
        return object

    def toList(self):
        return list(iter(self))

    def toSet(self):
        return set(iter(self))

    def iterate(self):
        self.bytecode.add_step("none")
        while True:
            try: self.nextTraverser()
            except StopIteration: return self

    def nextTraverser(self):
        if self.traversers is None:
            self.traversal_strategies.apply_strategies(self)
        if self.last_traverser is None:
            return next(self.traversers)
        else:
            temp = self.last_traverser
            self.last_traverser = None
            return temp

    def hasNext(self):
        if self.traversers is None:
            self.traversal_strategies.apply_strategies(self)
        if self.last_traverser is None:
            try: self.last_traverser = next(self.traversers)
            except StopIteration: return False
        return not(self.last_traverser is None) and self.last_traverser.bulk > 0

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

    def promise(self, cb=None):
        self.traversal_strategies.apply_async_strategies(self)
        future_traversal = self.remote_results
        future = type(future_traversal)()
        def process(f):
            try:
                traversal = f.result()
            except Exception as e:
                future.set_exception(e)
            else:
                self.traversers = iter(traversal.traversers)
                if cb:
                    try:
                        result = cb(self)
                    except Exception as e:
                        future.set_exception(e)
                    else:
                        future.set_result(result)
                else:
                    future.set_result(self)
        future_traversal.add_done_callback(process)
        return future


Barrier = Enum('Barrier', ' normSack')

statics.add_static('normSack', Barrier.normSack)

Cardinality = Enum('Cardinality', ' list_ set_ single')

statics.add_static('single', Cardinality.single)
statics.add_static('list_', Cardinality.list_)
statics.add_static('set_', Cardinality.set_)

Column = Enum('Column', ' keys values')

statics.add_static('keys', Column.keys)
statics.add_static('values', Column.values)

Direction = Enum('Direction', ' BOTH IN OUT')

statics.add_static('OUT', Direction.OUT)
statics.add_static('IN', Direction.IN)
statics.add_static('BOTH', Direction.BOTH)

GraphSONVersion = Enum('GraphSONVersion', ' V1_0 V2_0 V3_0')

statics.add_static('V1_0', GraphSONVersion.V1_0)
statics.add_static('V2_0', GraphSONVersion.V2_0)
statics.add_static('V3_0', GraphSONVersion.V3_0)

GryoVersion = Enum('GryoVersion', ' V1_0 V3_0')

statics.add_static('V1_0', GryoVersion.V1_0)
statics.add_static('V3_0', GryoVersion.V3_0)

Order = Enum('Order', ' asc desc shuffle')

statics.add_static('shuffle', Order.shuffle)
statics.add_static('asc', Order.asc)
statics.add_static('desc', Order.desc)

Pick = Enum('Pick', ' any none')

statics.add_static('any', Pick.any)
statics.add_static('none', Pick.none)

Pop = Enum('Pop', ' all_ first last mixed')

statics.add_static('first', Pop.first)
statics.add_static('last', Pop.last)
statics.add_static('all_', Pop.all_)
statics.add_static('mixed', Pop.mixed)

Scope = Enum('Scope', ' global_ local')

statics.add_static('global_', Scope.global_)
statics.add_static('local', Scope.local)


T = Enum('T', ' id id_ key label value')

statics.add_static('id', T.id)
statics.add_static('label', T.label)
statics.add_static('id_', T.id_)
statics.add_static('key', T.key)
statics.add_static('value', T.value)


Operator = Enum('Operator', ' addAll and_ assign div max max_ min min_ minus mult or_ sum sum_ sumLong')

statics.add_static('sum_', Operator.sum_)
statics.add_static('sum', Operator.sum_)
statics.add_static('minus', Operator.minus)
statics.add_static('mult', Operator.mult)
statics.add_static('div', Operator.div)
statics.add_static('min', Operator.min_)
statics.add_static('min_', Operator.min_)
statics.add_static('max_', Operator.max_)
statics.add_static('assign', Operator.assign)
statics.add_static('and_', Operator.and_)
statics.add_static('or_', Operator.or_)
statics.add_static('addAll', Operator.addAll)
statics.add_static('sumLong', Operator.sumLong)


class P(object):
    def __init__(self, operator, value, other=None):
        self.operator = operator
        self.value = value
        self.other = other

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
    def not_(*args):
        return P("not", *args)

    @staticmethod
    def outside(*args):
        return P("outside", *args)

    @staticmethod
    def test(*args):
        return P("test", *args)

    @staticmethod
    def within(*args):
        if len(args) == 1 and type(args[0]) == list:
            return P("within", args[0])
        elif len(args) == 1 and type(args[0]) == set:
            return P("within", list(args[0]))
        else:
            return P("within", list(args))
        
    @staticmethod
    def without(*args):
        if len(args) == 1 and type(args[0]) == list:
            return P("without", args[0])
        elif len(args) == 1 and type(args[0]) == set:
            return P("without", list(args[0]))
        else:
            return P("without", list(args))

    def and_(self, arg):
        return P("and", self, arg)

    def or_(self, arg):
        return P("or", self, arg)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.operator == other.operator and self.value == other.value and self.other == other.other

    def __repr__(self):
        return self.operator + "(" + str(self.value) + ")" if self.other is None else self.operator + "(" + str(self.value) + "," + str(self.other) + ")"


def between(*args):
    return P.between(*args)


def eq(*args):
    return P.eq(*args)


def gt(*args):
    return P.gt(*args)


def gte(*args):
    return P.gte(*args)


def inside(*args):
    return P.inside(*args)


def lt(*args):
    return P.lt(*args)


def lte(*args):
    return P.lte(*args)


def neq(*args):
    return P.neq(*args)


def not_(*args):
    return P.not_(*args)


def outside(*args):
    return P.outside(*args)


def within(*args):
    return P.within(*args)


def without(*args):
    return P.without(*args)


statics.add_static('between', between)

statics.add_static('eq', eq)

statics.add_static('gt', gt)

statics.add_static('gte', gte)

statics.add_static('inside', inside)

statics.add_static('lt', lt)

statics.add_static('lte', lte)

statics.add_static('neq', neq)

statics.add_static('not_', not_)

statics.add_static('outside', outside)

statics.add_static('within', within)

statics.add_static('without', without)


class TextP(P):
    def __init__(self, operator, value, other=None):
        P.__init__(self, operator, value, other)

    @staticmethod
    def containing(*args):
        return TextP("containing", *args)

    @staticmethod
    def endingWith(*args):
        return TextP("endingWith", *args)

    @staticmethod
    def notContaining(*args):
        return TextP("notContaining", *args)

    @staticmethod
    def notEndingWith(*args):
        return TextP("notEndingWith", *args)

    @staticmethod
    def notStartingWith(*args):
        return TextP("notStartingWith", *args)

    @staticmethod
    def startingWith(*args):
        return TextP("startingWith", *args)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.operator == other.operator and self.value == other.value and self.other == other.other

    def __repr__(self):
        return self.operator + "(" + str(self.value) + ")" if self.other is None else self.operator + "(" + str(self.value) + "," + str(self.other) + ")"


def containing(*args):
    return TextP.containing(*args)


def endingWith(*args):
    return TextP.endingWith(*args)


def notContaining(*args):
    return TextP.notContaining(*args)


def notEndingWith(*args):
    return TextP.notEndingWith(*args)


def notStartingWith(*args):
    return TextP.notStartingWith(*args)


def startingWith(*args):
    return TextP.startingWith(*args)


statics.add_static('containing', containing)

statics.add_static('endingWith', endingWith)

statics.add_static('notContaining', notContaining)

statics.add_static('notEndingWith', notEndingWith)

statics.add_static('notStartingWith', notStartingWith)

statics.add_static('startingWith', startingWith)




'''
IO
'''


class IO(object):

    graphml = "graphml"

    graphson = "graphson"

    gryo = "gryo"

    reader = "~tinkerpop.io.reader"

    registry = "~tinkerpop.io.registry"

    writer = "~tinkerpop.io.writer"


'''
ConnectedComponent
'''


class ConnectedComponent(object):

    component = "gremlin.connectedComponentVertexProgram.component"

    edges = "~tinkerpop.connectedComponent.edges"

    propertyName = "~tinkerpop.connectedComponent.propertyName"


'''
ShortestPath
'''


class ShortestPath(object):

    distance = "~tinkerpop.shortestPath.distance"

    edges = "~tinkerpop.shortestPath.edges"

    includeEdges = "~tinkerpop.shortestPath.includeEdges"

    maxDistance = "~tinkerpop.shortestPath.maxDistance"

    target = "~tinkerpop.shortestPath.target"


'''
PageRank
'''


class PageRank(object):

    edges = "~tinkerpop.pageRank.edges"

    propertyName = "~tinkerpop.pageRank.propertyName"

    times = "~tinkerpop.pageRank.times"


'''
PeerPressure
'''


class PeerPressure(object):

    edges = "~tinkerpop.peerPressure.edges"

    propertyName = "~tinkerpop.peerPressure.propertyName"

    times = "~tinkerpop.peerPressure.times"


'''
TRAVERSER
'''


class Traverser(object):
    def __init__(self, object, bulk=None):
        if bulk is None:
            bulk = long(1)
        self.object = object
        self.bulk = bulk

    def __repr__(self):
        return str(self.object)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.object == other.object


'''
TRAVERSAL STRATEGIES
'''


class TraversalStrategies(object):
    global_cache = {}

    def __init__(self, traversal_strategies=None):
        self.traversal_strategies = traversal_strategies.traversal_strategies if traversal_strategies is not None else []

    def add_strategies(self, traversal_strategies):
        self.traversal_strategies = self.traversal_strategies + traversal_strategies

    def apply_strategies(self, traversal):
        for traversal_strategy in self.traversal_strategies:
            traversal_strategy.apply(traversal)

    def apply_async_strategies(self, traversal):
        for traversal_strategy in self.traversal_strategies:
            traversal_strategy.apply_async(traversal)

    def __repr__(self):
        return str(self.traversal_strategies)


class TraversalStrategy(object):
    def __init__(self, strategy_name=None, configuration=None, fqcn=None):
        self.fqcn = fqcn
        self.strategy_name = type(self).__name__ if strategy_name is None else strategy_name
        self.configuration = {} if configuration is None else configuration

    def apply(self, traversal):
        return

    def apply_async(self, traversal):
        return

    def __eq__(self, other):
        return isinstance(other, self.__class__)

    def __hash__(self):
        return hash(self.strategy_name)

    def __repr__(self):
        return self.strategy_name


'''
BYTECODE
'''


class Bytecode(object):
    def __init__(self, bytecode=None):
        self.source_instructions = []
        self.step_instructions = []
        self.bindings = {}
        if bytecode is not None:
            self.source_instructions = list(bytecode.source_instructions)
            self.step_instructions = list(bytecode.step_instructions)

    def add_source(self, source_name, *args):
        instruction = [source_name]
        for arg in args:
            instruction.append(self.__convertArgument(arg))
        self.source_instructions.append(instruction)

    def add_step(self, step_name, *args):
        instruction = [step_name]
        for arg in args:
            instruction.append(self.__convertArgument(arg))
        self.step_instructions.append(instruction)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.source_instructions == other.source_instructions and self.step_instructions == other.step_instructions
        else:
            return False

    def __copy__(self):
        bb = Bytecode()
        bb.source_instructions = self.source_instructions
        bb.step_instructions = self.step_instructions
        bb.bindings = self.bindings
        return bb

    def __deepcopy__(self, memo={}):
        bb = Bytecode()
        bb.source_instructions = copy.deepcopy(self.source_instructions, memo)
        bb.step_instructions = copy.deepcopy(self.step_instructions, memo)
        bb.bindings = copy.deepcopy(self.bindings, memo)
        return bb

    def __convertArgument(self, arg):
        if isinstance(arg, Traversal):
            if arg.graph is not None:
                raise TypeError("The child traversal of " + str(arg) + " was not spawned anonymously - use the __ class rather than a TraversalSource to construct the child traversal")
            self.bindings.update(arg.bytecode.bindings)
            return arg.bytecode
        elif isinstance(arg, dict):
            newDict = {}
            for key in arg:
                newDict[self.__convertArgument(key)] = self.__convertArgument(arg[key])
            return newDict
        elif isinstance(arg, list):
            newList = []
            for item in arg:
                newList.append(self.__convertArgument(item))
            return newList
        elif isinstance(arg, set):
            newSet = set()
            for item in arg:
                newSet.add(self.__convertArgument(item))
            return newSet
        elif isinstance(arg, Binding):
            self.bindings[arg.key] = arg.value
            return Binding(arg.key, self.__convertArgument(arg.value))
        else:
            return arg

    def __repr__(self):
        return (str(self.source_instructions) if len(self.source_instructions) > 0 else "") + \
               (str(self.step_instructions) if len(self.step_instructions) > 0 else "")

    @staticmethod
    def _create_graph_op(name, *values):
        bc = Bytecode()
        bc.add_source(name, *values)
        return bc

    @staticmethod
    class GraphOp:
        @staticmethod
        def commit():
            return Bytecode._create_graph_op("tx", "commit")

        @staticmethod
        def rollback():
            return Bytecode._create_graph_op("tx", "rollback")


'''
BINDINGS
'''


class Bindings(object):

    @staticmethod
    def of(key, value):
        if not isinstance(key, str):
            raise TypeError("Key must be str")
        return Binding(key, value)


class Binding(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.key == other.key and self.value == other.value

    def __hash__(self):
        return hash(self.key) + hash(self.value)

    def __repr__(self):
        return "binding[" + self.key + "=" + str(self.value) + "]"


'''
WITH OPTIONS
'''


class WithOptions(object):

    tokens = "~tinkerpop.valueMap.tokens"

    none = 0

    ids = 1

    labels = 2

    keys = 4

    values = 8

    all = 15

    indexer = "~tinkerpop.index.indexer"

    list = 0

    map = 1

