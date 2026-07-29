# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""
gremlin_python.process.traversal.

These tests exercise the Traversal iteration protocol and the traversal.py
object model (Traverser, TraversalStrategies, TraversalStrategy, AtomicInteger,
and GremlinLang copy/convert helpers).

All assertions target the *actual* current behavior of the source. Where a
suspected bug is found, the test asserts the real behavior and flags the bug in
a comment rather than asserting idealized behavior or modifying the source.
"""
import copy

import pytest

from gremlin_python.statics import long
from gremlin_python.process.traversal import (
    Traversal,
    Traverser,
    TraversalStrategies,
    TraversalStrategy,
    AtomicInteger,
    GremlinLang,
    P,
    TextP,
)


class _FakeStrategies(object):
    """Minimal stand-in for TraversalStrategies.

    apply_strategies() populates traversal.traversers from a fixed list, which
    is exactly the wiring Traversal.__next__/next_traverser/has_next rely on.
    This lets us drive the iteration protocol with no server.
    """

    def __init__(self, traversers):
        self._traversers = list(traversers)
        self.applied = 0

    def apply_strategies(self, traversal):
        self.applied += 1
        traversal.traversers = iter(self._traversers)


def _make_traversal(traversers, graph=None):
    # Real __init__ signature is (graph, traversal_strategies, gremlin_lang).
    return Traversal(graph, _FakeStrategies(traversers), GremlinLang())


# ---------------------------------------------------------------------------
# 1. Traversal iteration protocol
# ---------------------------------------------------------------------------

class TestTraversalIteration(object):

    def test_next_bulk_expansion_of_traverser(self):
        # Traverser(obj, bulk=2) must yield obj twice before advancing;
        # a non-Traverser value yields exactly once.
        t = _make_traversal([Traverser('a', bulk=2), 'b'])
        assert t.__next__() == 'a'
        assert t.__next__() == 'a'
        assert t.__next__() == 'b'
        with pytest.raises(StopIteration):
            t.__next__()

    def test_next_no_amount_delegates_to_dunder_next(self):
        t = _make_traversal(['only'])
        assert t.next() == 'only'

    def test_next_with_amount_returns_up_to_n_and_stops_early(self):
        # Requesting more than available returns what's there without raising.
        t = _make_traversal(['x', 'y'])
        assert t.next(5) == ['x', 'y']

    def test_next_with_amount_returns_exactly_n_when_available(self):
        t = _make_traversal(['x', 'y', 'z'])
        assert t.next(2) == ['x', 'y']

    def test_to_list_drains_all(self):
        t = _make_traversal(['x', 'y', 'z'])
        assert t.to_list() == ['x', 'y', 'z']

    def test_to_set_dedups(self):
        t = _make_traversal(['a', 'a', 'b'])
        assert t.to_set() == {'a', 'b'}

    def test_iterate_drains_and_returns_self(self):
        t = _make_traversal(['a', 'b', 'c'])
        result = t.iterate()
        assert result is t
        # iterate() appends a 'discard' step to the gremlin bytecode.
        assert 'discard' in t.gremlin_lang.get_gremlin()

    def test_next_traverser_returns_raw_traverser(self):
        # next_traverser must return the Traverser itself, not the unwrapped object.
        t = _make_traversal([Traverser('a', bulk=2)])
        raw = t.next_traverser()
        assert isinstance(raw, Traverser)
        assert raw.object == 'a'
        assert raw.bulk == 2

    def test_iter_returns_self(self):
        t = _make_traversal(['x'])
        assert iter(t) is t

    def test_eq_equal_gremlin_strings_are_equal(self):
        t1 = _make_traversal([])
        t1.gremlin_lang.add_step('V')
        t2 = _make_traversal([])
        t2.gremlin_lang.add_step('V')
        assert t1 == t2

    def test_eq_different_gremlin_strings_not_equal(self):
        t1 = _make_traversal([])
        t1.gremlin_lang.add_step('V')
        t2 = _make_traversal([])
        t2.gremlin_lang.add_step('E')
        assert t1 != t2

    def test_eq_non_traversal_is_not_equal(self):
        t = _make_traversal([])
        assert (t == 'not a traversal') is False

    def test_repr_is_gremlin_string(self):
        t = _make_traversal([])
        t.gremlin_lang.add_step('V')
        assert repr(t) == t.gremlin_lang.get_gremlin()
        assert repr(t) == 'g.V()'

    def test_has_next_false_when_exhausted_true_when_items_remain(self):
        # has_next() pulls one traverser and caches it; on an empty traversal
        # the underlying next() raises StopIteration and has_next() returns
        # False, while a non-empty traversal returns True.
        assert _make_traversal([]).has_next() is False
        assert _make_traversal([Traverser('a')]).has_next() is True

    def test_has_next_caches_traverser_returned_by_next_traverser(self):
        # has_next() stores the pulled traverser in last_traverser; the very
        # next next_traverser() must return that SAME cached instance instead of
        # advancing the iterator again.
        tr = Traverser('a', bulk=2)
        t = _make_traversal([tr])
        assert t.has_next() is True
        assert t.next_traverser() is tr


# ---------------------------------------------------------------------------
# 2. Traverser
# ---------------------------------------------------------------------------

class TestTraverser(object):

    def test_bulk_defaults_to_long_one(self):
        tr = Traverser('x')
        assert tr.bulk == long(1)

    def test_explicit_bulk_is_kept(self):
        tr = Traverser('x', bulk=7)
        assert tr.bulk == 7

    def test_eq_compares_only_object_ignoring_bulk(self):
        assert Traverser('x', bulk=2) == Traverser('x', bulk=99)
        assert Traverser('x') != Traverser('y')

    def test_eq_non_traverser_is_not_equal(self):
        assert (Traverser('x') == 'x') is False

    def test_repr_equals_str_of_object(self):
        assert repr(Traverser(42)) == str(42)
        assert repr(Traverser('abc')) == 'abc'


# ---------------------------------------------------------------------------
# 3. TraversalStrategies
# ---------------------------------------------------------------------------

class TestTraversalStrategies(object):

    def test_default_init_is_empty(self):
        ts = TraversalStrategies()
        assert ts.traversal_strategies == []

    def test_copy_constructor_takes_source_list(self):
        source = TraversalStrategies()
        s1, s2 = TraversalStrategy(), TraversalStrategy()
        source.traversal_strategies = [s1, s2]
        copied = TraversalStrategies(source)
        assert copied.traversal_strategies == [s1, s2]

    def test_add_strategies_concatenates(self):
        ts = TraversalStrategies()
        a, b, c = TraversalStrategy(), TraversalStrategy(), TraversalStrategy()
        ts.traversal_strategies = [a]
        ts.add_strategies([b, c])
        assert ts.traversal_strategies == [a, b, c]


# ---------------------------------------------------------------------------
# 4. TraversalStrategy
# ---------------------------------------------------------------------------

class _MyStrategy(TraversalStrategy):
    pass


class TestTraversalStrategy(object):

    def test_strategy_name_defaults_to_class_name(self):
        assert TraversalStrategy().strategy_name == 'TraversalStrategy'
        assert _MyStrategy().strategy_name == '_MyStrategy'

    def test_explicit_strategy_name_is_kept(self):
        assert TraversalStrategy(strategy_name='Custom').strategy_name == 'Custom'

    def test_kwargs_merged_into_configuration(self):
        s = TraversalStrategy(a=1, b=2)
        assert s.configuration == {'a': 1, 'b': 2}

    def test_configuration_takes_precedence_over_kwargs(self):
        # Source merges as {**kwargs, **self.configuration}, so an explicit
        # configuration value wins over a same-named kwarg.
        s = TraversalStrategy(configuration={'x': 'cfg'}, x='kwarg', y='kwarg')
        assert s.configuration == {'x': 'cfg', 'y': 'kwarg'}

    def test_eq_true_for_same_class_regardless_of_config(self):
        assert TraversalStrategy(a=1) == TraversalStrategy(b=2)

    def test_eq_false_for_different_class(self):
        assert (TraversalStrategy() == _MyStrategy()) is False

    def test_hash_uses_strategy_name(self):
        assert hash(TraversalStrategy()) == hash('TraversalStrategy')
        assert hash(_MyStrategy()) == hash('_MyStrategy')


# ---------------------------------------------------------------------------
# 5. AtomicInteger
# ---------------------------------------------------------------------------

class TestAtomicInteger(object):

    def test_get_and_increment_returns_pre_increment_values(self):
        ai = AtomicInteger()
        assert ai.get_and_increment() == 0
        assert ai.get_and_increment() == 1
        assert ai.get_and_increment() == 2
        assert ai.value == 3

    def test_set_returns_new_value(self):
        ai = AtomicInteger()
        assert ai.set(10) == 10
        assert ai.value == 10

    def test_value_property_reflects_state(self):
        ai = AtomicInteger()
        assert ai.value == 0
        ai.get_and_increment()
        assert ai.value == 1


# ---------------------------------------------------------------------------
# 6. GremlinLang.__copy__ / __deepcopy__
# ---------------------------------------------------------------------------

class TestGremlinLangCopy(object):

    def test_copy_shares_parameters_and_gremlin_references(self):
        gl = GremlinLang()
        gl.add_step('V')
        gl.parameters['k'] = 'v'
        shallow = copy.copy(gl)
        # __copy__ assigns the same underlying list/dict objects.
        assert shallow.gremlin is gl.gremlin
        assert shallow.parameters is gl.parameters

    def test_deepcopy_is_independent(self):
        gl = GremlinLang()
        gl.add_step('V')
        gl.parameters['k'] = 'v'
        deep = copy.deepcopy(gl)
        assert deep.gremlin is not gl.gremlin
        assert deep.parameters is not gl.parameters
        assert deep.gremlin == gl.gremlin
        assert deep.parameters == gl.parameters
        # Mutating the deep copy must not affect the original.
        deep.gremlin.append('.mutated')
        deep.parameters['new'] = 'x'
        assert '.mutated' not in gl.gremlin
        assert 'new' not in gl.parameters


# ---------------------------------------------------------------------------
# 7. GremlinLang._convert_argument
# ---------------------------------------------------------------------------

class TestConvertArgument(object):

    def test_non_anonymous_child_traversal_raises_type_error(self):
        gl = GremlinLang()
        # A child traversal spawned from a source (graph is not None) is illegal.
        child = Traversal('some-graph', _FakeStrategies([]), GremlinLang())
        with pytest.raises(TypeError) as excinfo:
            gl._convert_argument(child)
        assert 'spawned anonymously' in str(excinfo.value)

    def test_anonymous_child_traversal_converted_to_gremlin_lang(self):
        gl = GremlinLang()
        anon = Traversal(None, _FakeStrategies([]), GremlinLang())
        assert gl._convert_argument(anon) is anon.gremlin_lang

    def test_nested_dict_list_set_of_plain_values_unchanged(self):
        gl = GremlinLang()
        assert gl._convert_argument({1: [2, 3]}) == {1: [2, 3]}
        assert gl._convert_argument([1, [2, 3]]) == [1, [2, 3]]
        assert gl._convert_argument({1, 2, 3}) == {1, 2, 3}

    def test_nested_anonymous_traversal_inside_container_converted(self):
        gl = GremlinLang()
        anon = Traversal(None, _FakeStrategies([]), GremlinLang())
        converted = gl._convert_argument([anon])
        assert converted == [anon.gremlin_lang]



# ---------------------------------------------------------------------------
# 8. P / TextP predicate object model
# ---------------------------------------------------------------------------

class TestPRepr(object):
    def test_single_arg_repr(self):
        # other is None -> "operator(value)"
        assert repr(P.eq(2)) == "eq(2)"
        assert repr(P.gt(5)) == "gt(5)"

    def test_two_arg_repr(self):
        # other is not None -> "operator(value,other)" (no space after comma)
        assert repr(P.between(1, 2)) == "between(1,2)"
        assert repr(P.inside(1, 10)) == "inside(1,10)"

    def test_repr_uses_str_of_operands(self):
        # value/other are stringified via str()
        assert repr(P.within([1, 2, 3])) == "within([1, 2, 3])"


class TestPEquality(object):
    def test_equal_same_fields(self):
        assert P.eq(2) == P.eq(2)
        assert P.between(1, 2) == P.between(1, 2)

    def test_not_equal_different_value(self):
        assert P.eq(2) != P.eq(3)

    def test_not_equal_different_operator(self):
        assert P.eq(2) != P.gt(2)

    def test_not_equal_different_other(self):
        assert P.between(1, 2) != P.between(1, 3)

    def test_not_equal_non_p(self):
        # isinstance guard makes comparison against a plain value False
        assert P.eq(2) != 2


class TestPTextPSubclassEquality(object):
    def test_p_equals_textp_is_false_this_direction(self):
        p = P.eq(2)
        t = TextP("eq", 2)
        # reflected priority calls TextP.__eq__(p); isinstance(p, TextP) is False -> False.
        assert (p == t) is False

    def test_textp_equals_p_is_false_reverse_direction(self):
        p = P.eq(2)
        t = TextP("eq", 2)
        # self is TextP; isinstance(p, TextP) is False -> False.
        assert (t == p) is False


class TestPBooleanComposition(object):
    def test_and_builds_nested_p(self):
        left = P.gt(0)
        right = P.lt(10)
        combined = left.and_(right)
        # and_ -> P("and", self, arg): value holds self, other holds arg
        assert combined.operator == "and"
        assert combined.value is left
        assert combined.other is right

    def test_or_builds_nested_p(self):
        left = P.gt(0)
        right = P.lt(10)
        combined = left.or_(right)
        assert combined.operator == "or"
        assert combined.value is left
        assert combined.other is right


class TestPWithinWithoutBranches(object):
    def test_within_single_list_kept_as_is(self):
        source = [1, 2, 3]
        p = P.within(source)
        assert p.operator == "within"
        # the exact list object is retained (not copied)
        assert p.value is source

    def test_within_single_set_converted_to_list(self):
        p = P.within({1, 2, 3})
        assert p.operator == "within"
        assert isinstance(p.value, list)
        assert set(p.value) == {1, 2, 3}

    def test_within_single_traversal_passthrough(self):
        trav = Traversal(None, None, None)
        p = P.within(trav)
        assert p.operator == "within"
        # a lone Traversal is passed through untouched (not wrapped in a list)
        assert p.value is trav

    def test_within_varargs_becomes_list(self):
        p = P.within(1, 2, 3)
        assert p.operator == "within"
        assert p.value == [1, 2, 3]

    def test_without_single_list_kept_as_is(self):
        source = [1, 2, 3]
        p = P.without(source)
        assert p.operator == "without"
        assert p.value is source

    def test_without_single_set_converted_to_list(self):
        p = P.without({1, 2, 3})
        assert p.operator == "without"
        assert isinstance(p.value, list)
        assert set(p.value) == {1, 2, 3}

    def test_without_single_traversal_passthrough(self):
        trav = Traversal(None, None, None)
        p = P.without(trav)
        assert p.operator == "without"
        assert p.value is trav

    def test_without_varargs_becomes_list(self):
        p = P.without(1, 2, 3)
        assert p.operator == "without"
        assert p.value == [1, 2, 3]


class TestPUnhashable(object):
    # P (and TextP) define __eq__ but not __hash__, so Python sets __hash__ to
    # None, making instances unhashable. Documented, not fixed.
    def test_p_is_unhashable(self):
        with pytest.raises(TypeError):
            hash(P.eq(1))

    def test_textp_is_unhashable(self):
        with pytest.raises(TypeError):
            hash(TextP.containing("x"))


class TestTextPObjectModel(object):
    def test_textp_repr_single_arg(self):
        assert repr(TextP.containing("x")) == "containing(x)"

    def test_textp_equality_same_class(self):
        assert TextP.containing("x") == TextP.containing("x")
        assert TextP.containing("x") != TextP.containing("y")

    def test_textp_operator_and_value(self):
        t = TextP.starting_with("ab")
        assert t.operator == "startingWith"
        assert t.value == "ab"
        assert t.other is None
