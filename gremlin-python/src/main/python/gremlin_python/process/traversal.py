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
import warnings
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
        warnings.warn(
            "gremlin_python.process.Traversal.toList will be replaced by "
            "gremlin_python.process.Traversal.to_list.",
            DeprecationWarning)
        return self.to_list()

    def to_list(self):
        return list(iter(self))

    def toSet(self):
        warnings.warn(
            "gremlin_python.process.Traversal.toSet will be replaced by "
            "gremlin_python.process.Traversal.to_set.",
            DeprecationWarning)
        return self.to_set()

    def to_set(self):
        return set(iter(self))

    def iterate(self):
        self.bytecode.add_step("discard")
        while True:
            try: self.next_traverser()
            except StopIteration: return self

    def nextTraverser(self):
        warnings.warn(
            "gremlin_python.process.Traversal.nextTraverser will be replaced by "
            "gremlin_python.process.Traversal.next_traverser.",
            DeprecationWarning)
        return self.next_traverser()

    def next_traverser(self):
        if self.traversers is None:
            self.traversal_strategies.apply_strategies(self)
        if self.last_traverser is None:
            return next(self.traversers)
        else:
            temp = self.last_traverser
            self.last_traverser = None
            return temp

    def hasNext(self):
        warnings.warn(
            "gremlin_python.process.Traversal.hasNext will be replaced by "
            "gremlin_python.process.Traversal.has_next.",
            DeprecationWarning)
        return self.has_next()

    def has_next(self):
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


Barrier = Enum('Barrier', ' normSack norm_sack')

statics.add_static('normSack', Barrier.normSack)
statics.add_static('norm_sack', Barrier.norm_sack)

Cardinality = Enum('Cardinality', ' list_ set_ single')

statics.add_static('single', Cardinality.single)
statics.add_static('list_', Cardinality.list_)
statics.add_static('set_', Cardinality.set_)

Column = Enum('Column', ' keys values')

statics.add_static('keys', Column.keys)
statics.add_static('values', Column.values)

# alias from_ and to
Direction = Enum(
    value='Direction',
    names=[
        ('BOTH', 'BOTH'),
        ('IN', 'IN'),
        ('OUT', 'OUT'),
        ('from_', "OUT"),
        ('to', 'IN'),
    ],
)

statics.add_static('OUT', Direction.OUT)
statics.add_static('IN', Direction.IN)
statics.add_static('BOTH', Direction.BOTH)
statics.add_static('from_', Direction.OUT)
statics.add_static('to', Direction.IN)

GraphSONVersion = Enum('GraphSONVersion', ' V1_0 V2_0 V3_0')

statics.add_static('V1_0', GraphSONVersion.V1_0)
statics.add_static('V2_0', GraphSONVersion.V2_0)
statics.add_static('V3_0', GraphSONVersion.V3_0)

GryoVersion = Enum('GryoVersion', ' V1_0 V3_0')

statics.add_static('V1_0', GryoVersion.V1_0)
statics.add_static('V3_0', GryoVersion.V3_0)

DT = Enum('DT', ' second minute hour day')

statics.add_static('second', DT.second)
statics.add_static('minute', DT.minute)
statics.add_static('hour', DT.hour)
statics.add_static('day', DT.day)

Merge = Enum('Merge', ' on_create on_match out_v in_v')

statics.add_static('on_create', Merge.on_create)
statics.add_static('on_match', Merge.on_match)
statics.add_static('in_v', Merge.in_v)
statics.add_static('out_v', Merge.out_v)

Order = Enum('Order', ' asc desc shuffle')

statics.add_static('shuffle', Order.shuffle)
statics.add_static('asc', Order.asc)
statics.add_static('desc', Order.desc)

Pick = Enum('Pick', ' any_ none unproductive')

statics.add_static('any_', Pick.any_)
statics.add_static('none', Pick.none)
statics.add_static('unproductive', Pick.unproductive)

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


Operator = Enum('Operator', ' addAll add_all and_ assign div max max_ min min_ minus mult or_ sum_ sumLong sum_long')

statics.add_static('sum_', Operator.sum_)
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
statics.add_static('add_all', Operator.add_all)
statics.add_static('sum_long', Operator.sum_long)


GType = Enum('GType', ' BIGDECIMAL BIGINT BINARY BOOLEAN BYTE CHAR DATETIME DOUBLE DURATION EDGE FLOAT GRAPH INT LIST LONG MAP NULL NUMBER PATH PROPERTY SET SHORT STRING TREE UUID VERTEX VPROPERTY')

statics.add_static('BIGDECIMAL', GType.BIGDECIMAL)
statics.add_static('BIGINT', GType.BIGINT)
statics.add_static('BINARY', GType.BINARY)
statics.add_static('BOOLEAN', GType.BOOLEAN)
statics.add_static('BYTE', GType.BYTE)
statics.add_static('CHAR', GType.CHAR)
statics.add_static('DATETIME', GType.DATETIME)
statics.add_static('DOUBLE', GType.DOUBLE)
statics.add_static('DURATION', GType.DURATION)
statics.add_static('EDGE', GType.EDGE)
statics.add_static('FLOAT', GType.FLOAT)
statics.add_static('GRAPH', GType.GRAPH)
statics.add_static('INT', GType.INT)
statics.add_static('LIST', GType.LIST)
statics.add_static('LONG', GType.LONG)
statics.add_static('MAP', GType.MAP)
statics.add_static('NULL', GType.NULL)
statics.add_static('NUMBER', GType.NUMBER)
statics.add_static('PATH', GType.PATH)
statics.add_static('PROPERTY', GType.PROPERTY)
statics.add_static('SET', GType.SET)
statics.add_static('SHORT', GType.SHORT)
statics.add_static('STRING', GType.STRING)
statics.add_static('TREE', GType.TREE)
statics.add_static('UUID', GType.UUID)
statics.add_static('VERTEX', GType.VERTEX)
statics.add_static('VPROPERTY', GType.VPROPERTY)


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
    def type_of(*args):
        return P("typeOf", *args)

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


def type_of(*args):
    return P.type_of(*args)


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

statics.add_static('typeOf', type_of)


class TextP(P):
    def __init__(self, operator, value, other=None):
        P.__init__(self, operator, value, other)

    @staticmethod
    def containing(*args):
        return TextP("containing", *args)

    @staticmethod
    def endingWith(*args):
        warnings.warn(
            "gremlin_python.process.TextP.endingWith will be replaced by "
            "gremlin_python.process.TextP.ending_with.",
            DeprecationWarning)
        return TextP("endingWith", *args)

    @staticmethod
    def ending_with(*args):
        return TextP("endingWith", *args)

    @staticmethod
    def notContaining(*args):
        warnings.warn(
            "gremlin_python.process.TextP.notContaining will be replaced by "
            "gremlin_python.process.TextP.not_containing.",
            DeprecationWarning)
        return TextP("notContaining", *args)

    @staticmethod
    def not_containing(*args):
        return TextP("notContaining", *args)

    @staticmethod
    def notEndingWith(*args):
        warnings.warn(
            "gremlin_python.process.TextP.notEndingWith will be replaced by "
            "gremlin_python.process.TextP.not_ending_with.",
            DeprecationWarning)
        return TextP("notEndingWith", *args)

    @staticmethod
    def not_ending_with(*args):
        return TextP("notEndingWith", *args)

    @staticmethod
    def notStartingWith(*args):
        warnings.warn(
            "gremlin_python.process.TextP.notStartingWith will be replaced by "
            "gremlin_python.process.TextP.not_starting_With.",
            DeprecationWarning)
        return TextP("notStartingWith", *args)

    @staticmethod
    def not_starting_with(*args):
        return TextP("notStartingWith", *args)

    @staticmethod
    def startingWith(*args):
        warnings.warn(
            "gremlin_python.process.TextP.startingWith will be replaced by "
            "gremlin_python.process.TextP.startingWith.",
            DeprecationWarning)
        return TextP("startingWith", *args)

    @staticmethod
    def starting_with(*args):
        return TextP("startingWith", *args)

    @staticmethod
    def regex(*args):
        return TextP("regex", *args)

    @staticmethod
    def not_regex(*args):
        return TextP("notRegex", *args)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.operator == other.operator and self.value == other.value and self.other == other.other

    def __repr__(self):
        return self.operator + "(" + str(self.value) + ")" if self.other is None else self.operator + "(" + str(self.value) + "," + str(self.other) + ")"


def containing(*args):
    return TextP.containing(*args)


def endingWith(*args):
    return TextP.ending_with(*args)


def ending_with(*args):
    return TextP.ending_with(*args)


def notContaining(*args):
    return TextP.not_containing(*args)


def not_containing(*args):
    return TextP.not_containing(*args)


def notEndingWith(*args):
    return TextP.not_ending_with(*args)


def not_ending_with(*args):
    return TextP.not_ending_with(*args)


def notStartingWith(*args):
    return TextP.not_starting_with(*args)


def not_starting_with(*args):
    return TextP.not_starting_with(*args)


def startingWith(*args):
    return TextP.starting_with(*args)


def starting_with(*args):
    return TextP.starting_with(*args)

def regex(*args):
    return TextP.regex(*args)

def not_regex(*args):
    return TextP.not_regex(*args)

statics.add_static('containing', containing)

statics.add_static('endingWith', endingWith)

statics.add_static('ending_with', ending_with)

statics.add_static('notContaining', notContaining)

statics.add_static('not_containing', not_containing)

statics.add_static('notEndingWith', notEndingWith)

statics.add_static('not_ending_with', not_ending_with)

statics.add_static('notStartingWith', notStartingWith)

statics.add_static('not_starting_with', not_starting_with)

statics.add_static('startingWith', startingWith)

statics.add_static('starting_with', starting_with)

statics.add_static('regex', regex)

statics.add_static('not_regex', not_regex)




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

    property_name = "~tinkerpop.connectedComponent.propertyName"


'''
ShortestPath
'''


class ShortestPath(object):

    distance = "~tinkerpop.shortestPath.distance"

    edges = "~tinkerpop.shortestPath.edges"

    includeEdges = "~tinkerpop.shortestPath.includeEdges"

    include_edges = "~tinkerpop.shortestPath.includeEdges"

    maxDistance = "~tinkerpop.shortestPath.maxDistance"

    max_distance = "~tinkerpop.shortestPath.maxDistance"

    target = "~tinkerpop.shortestPath.target"


'''
PageRank
'''


class PageRank(object):

    edges = "~tinkerpop.pageRank.edges"

    propertyName = "~tinkerpop.pageRank.propertyName"

    property_name = "~tinkerpop.pageRank.propertyName"

    times = "~tinkerpop.pageRank.times"


'''
PeerPressure
'''


class PeerPressure(object):

    edges = "~tinkerpop.peerPressure.edges"

    propertyName = "~tinkerpop.peerPressure.propertyName"

    property_name = "~tinkerpop.pageRank.propertyName"

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


class CardinalityValue(Bytecode):
    def __init__(self, cardinality, val):
        super().__init__()
        self.add_source("CardinalityValueTraversal", cardinality, val)

    @classmethod
    def single(cls, val):
        return CardinalityValue(Cardinality.single, val)

    @classmethod
    def list_(cls, val):
        return CardinalityValue(Cardinality.list_, val)

    @classmethod
    def set_(cls, val):
        return CardinalityValue(Cardinality.set_, val)


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

