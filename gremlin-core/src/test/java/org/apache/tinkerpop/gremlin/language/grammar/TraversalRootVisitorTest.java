/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.language.grammar;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.function.Function;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.tinkerpop.gremlin.process.traversal.DT;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.CollectionUtil;
import org.apache.tinkerpop.gremlin.util.DatetimeHelper;
import org.junit.Before;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.apache.tinkerpop.gremlin.process.traversal.P.gt;
import static org.apache.tinkerpop.gremlin.process.traversal.SackFunctions.Barrier.normSack;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.global;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.E;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.V;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.addE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.as;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.constant;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.is;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.min;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;
import static org.apache.tinkerpop.gremlin.structure.T.label;
import static org.junit.Assert.assertEquals;

public class TraversalRootVisitorTest {
    private GraphTraversalSource g;
    private GremlinAntlrToJava antlrToLanguage;

    @Before
    public void setup() {
        g = new GraphTraversalSource(EmptyGraph.instance());
        antlrToLanguage = createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("foo", "bar")));
    }

    @Test
    public void shouldParseTraversalMethod_discard()  {
        compare(g.V().discard(), eval("g.V().discard()"));
        compare(g.V().union(__.identity().discard()), eval("g.V().union(__.identity().discard())"));
    }

    @Test
    public void shouldParseChainedTraversal() {
        // a random chain traversal.
        compare(g.V().map(__.addE("person")), eval("g.V().map(__.addE('person'))"));
    }

    @Test
    public void shouldParseTraversalMethod_addE_String() {
        // same with chained traversal but uses double quotes
        compare(g.V().map(__.addE("person")), eval("g.V().map(__.addE(\"person\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_addE_Traversal() {
        // same with chained traversal but uses double quotes
        compare(g.V().map(__.addE(V().hasLabel("person").label())), eval("g.V().map(__.addE(V().hasLabel(\"person\").label()))"));
    }

    @Test
    public void shouldParseTraversalMethod_addE_GValue() {
        compare(g.V().map(__.addE(GValue.of("foo", "bar"))), eval("g.V().map(__.addE(foo))"));
    }

    @Test
    public void shouldParseTraversalMethod_mergeE()  {
        compare(g.V().map(__.mergeE()), eval("g.V().map(__.mergeE())"));
    }

    @Test
    public void shouldParseTraversalMethod_mergeE_GValue()  {
        antlrToLanguage = new GremlinAntlrToJava("g", EmptyGraph.instance(), __::start, g,
                new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("foo", Collections.singletonMap("x", "y"))));
        compare(g.V().map(__.mergeE(GValue.of("foo", Collections.singletonMap("x", "y")))), eval("g.V().map(__.mergeE(foo))"));
    }

    @Test
    public void shouldParseTraversalMethod_mergeE_Traversal()  {
        compare(g.V().map(__.mergeE(__.hasLabel("person"))), eval("g.V().map(__.mergeE(__.hasLabel(\"person\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_mergeE_Map()  {
        compare(g.V().map(__.mergeE(CollectionUtil.asMap("x", "y"))), eval("g.V().map(__.mergeE([\"x\":\"y\"]))"));
    }

    @Test
    public void shouldParseTraversalMethod_addV_Empty() {
        compare(g.V().map(__.addV()), eval("g.V().map(__.addV())"));
    }

    @Test
    public void shouldParseTraversalMethod_addV_String() {
        compare(g.V().map(__.addV("test")), eval("g.V().map(__.addV(\"test\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_addV_Traversal() {
        compare(g.V().map(__.addV(V().hasLabel("person").label())), eval("g.V().map(__.addV(V().hasLabel(\"person\").label()))"));
    }

    @Test
    public void shouldParseTraversalMethod_addV_GValue() {
        compare(g.V().map(__.addV(GValue.of("foo", "bar"))), eval("g.V().map(__.addV(foo))"));
    }

    @Test
    public void shouldParseTraversalMethod_mergeV()  {
        compare(g.V().map(__.mergeV()), eval("g.V().map(__.mergeV())"));
    }

    @Test
    public void shouldParseTraversalMethod_mergeV_GValue()  {
        antlrToLanguage = new GremlinAntlrToJava("g", EmptyGraph.instance(), __::start, g,
                new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("foo", Collections.singletonMap("x", "y"))));
        compare(g.V().map(__.mergeV(GValue.of("foo", Collections.singletonMap("x", "y")))), eval("g.V().map(__.mergeV(foo))"));
    }

    @Test
    public void shouldParseTraversalMethod_mergeV_Traversal()  {
        compare(g.V().map(__.mergeV(__.hasLabel("person"))), eval("g.V().map(__.mergeV(__.hasLabel(\"person\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_mergeV_Map()  {
        compare(g.V().map(__.mergeV(CollectionUtil.asMap("x", "y"))), eval("g.V().map(__.mergeV([\"x\": \"y\"]))"));
    }

    @Test
    public void shouldParseTraversalMethod_aggregate() {
        compare(g.V().map(__.aggregate("test")), eval("g.V().map(__.aggregate('test'))"));
    }

    @Test
    public void shouldParseTraversalMethod_aggregate_Scope() {
        compare(g.V().map(__.aggregate(global, "test")), eval("g.V().map(__.aggregate(global, 'test'))"));
        compare(g.V().map(__.aggregate(Scope.local, "test")), eval("g.V().map(__.aggregate(Scope.local, 'test'))"));
    }

    @Test
    public void shouldParseTraversalMethod_all_P() {
        compare(g.V().map(__.id().fold().all(gt(1))), eval("g.V().map(__.id().fold().all(gt(1)))"));
    }

    @Test
    public void shouldParseTraversalMethod_and() {
        compare(g.V().map(__.and(outE("knows"))), eval("g.V().map(__.and(outE('knows')))"));
    }

    @Test
    public void shouldParseTraversalMethod_any_P() {
        compare(g.V().map(__.id().fold().any(gt(1))), eval("g.V().map(__.id().fold().any(gt(1)))"));
    }
    @Test
    public void shouldParseTraversalMethod_as() {
        compare(g.V().map(__.as("test")), eval("g.V().map(__.as('test'))"));
    }

    @Test
    public void shouldParseTraversalMethod_barrier_Consumer() {
        compare(g.V().map(__.barrier(normSack)), eval("g.V().map(__.barrier(normSack))"));
    }

    @Test
    public void shouldParseTraversalMethod_barrier_Empty() {
        compare(g.V().map(__.barrier()), eval("g.V().map(__.barrier())"));
    }

    @Test
    public void shouldParseTraversalMethod_barrier_int() {
        compare(g.V().map(__.barrier(4)), eval("g.V().map(__.barrier(4))"));
    }

    @Test
    public void shouldParseTraversalMethod_both_Empty() {
        compare(g.V().map(__.both()), eval("g.V().map(__.both())"));
    }

    @Test
    public void shouldParseTraversalMethod_both_SingleString() {
        compare(g.V().map(__.both("test")), eval("g.V().map(__.both('test'))"));
    }

    @Test
    public void shouldParseTraversalMethod_both_MultiString() {
        compare(g.V().map(__.both("a", "b")), eval("g.V().map(__.both('a', 'b'))"));
    }

    @Test
    public void shouldParseTraversalMethod_both_GValue() {
        compare(g.V().map(__.both(GValue.ofString("foo", "bar"))), eval("g.V().map(__.both(foo))"));
    }

    @Test
    public void shouldParseTraversalMethod_bothE() {
        compare(g.V().map(__.bothE("test")), eval("g.V().map(__.bothE('test'))"));
    }

    @Test
    public void shouldParseTraversalMethod_bothE_GValue() {
        compare(g.V().map(__.bothE(GValue.ofString("foo", "bar"))), eval("g.V().map(__.bothE(foo))"));
    }

    @Test
    public void shouldParseTraversalMethod_bothV() {
        compare(g.V().map(__.bothV()), eval("g.V().map(__.bothV())"));
    }

    @Test
    public void shouldParseTraversalMethod_branch_Traversal() {
        compare(g.V().map(__.branch(values("name"))), eval("g.V().map(__.branch(values('name')))"));
    }

    @Test
    public void shouldParseTraversalMethod_by_Comparator() {
        compare(g.V().map(__.order().by(Order.asc)), eval("g.V().map(__.order().by(asc))"));
    }

    @Test
    public void shouldParseTraversalMethod_by_Empty() {
        compare(g.V().map(__.cyclicPath().by()), eval("g.V().map(__.cyclicPath().by())"));
    }

    @Test
    public void shouldParseTraversalMethod_by_Function() {
        compare(g.V().map(__.order().by(T.id)), eval("g.V().map(__.order().by(id))"));
    }

    @Test
    public void shouldParseTraversalMethod_by_Function_Comparator() {
        compare(g.V().map(__.order().by(Column.keys, Order.asc)), eval("g.V().map(__.order().by(keys, asc))"));
    }

    @Test
    public void shouldParseTraversalMethod_by_Order() {
        compare(g.V().map(__.order().by(Order.shuffle)), eval("g.V().map(__.order().by(shuffle))"));
    }

    @Test
    public void shouldParseTraversalMethod_by_String() {
        compare(g.V().map(__.order().by("name")), eval("g.V().map(__.order().by('name'))"));
    }

    @Test
    public void shouldParseTraversalMethod_by_String_Comparator() {
        compare(g.V().map(__.order().by("name", Order.asc)), eval("g.V().map(__.order().by('name', asc))"));
    }

    @Test
    public void shouldParseTraversalMethod_by_T() {
        compare(g.V().map(__.order().by(T.id)), eval("g.V().map(__.order().by(id))"));
    }

    @Test
    public void shouldParseTraversalMethod_by_Traversal() {
        compare(g.V().map(__.group().by(bothE().count())), eval("g.V().map(__.group().by(bothE().count()))"));
    }

    @Test
    public void shouldParseTraversalMethod_by_Traversal_Comparator() {
        compare(g.V().map(__.order().by(bothE().count(), Order.asc)), eval("g.V().map(__.order().by(bothE().count(), asc))"));
    }

    @Test
    public void shouldParseTraversalMethod_cap() {
        compare(g.V().map(__.cap("test")), eval("g.V().map(__.cap('test'))"));
    }

    @Test
    public void shouldParseTraversalMethod_choose_Function() {
        compare(g.V().map(__.choose((Function) label)), eval("g.V().map(__.choose(label))"));
    }

    @Test
    public void shouldParseTraversalMethod_choose_Predicate() {
        compare(g.V().map(__.choose(P.eq("test"), out("knows"))), eval("g.V().map(__.choose(P.eq(\"test\"), out(\"knows\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_choose_Predicate_Traversal() {
        compare(g.V().map(__.choose(is(12), values("age"))), eval("g.V().map(__.choose(is(12), values(\"age\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_choose_Predicate_Traversal_Traversal() {
        compare(g.V().map(__.choose(is(12), values("age"), values("count"))),
                eval("g.V().map(__.choose(is(12), values(\"age\"), values(\"count\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_choose_Traversal() {
        compare(g.V().map(__.choose(values("age"))), eval("g.V().map(__.choose(values('age')))"));
    }

    @Test
    public void shouldParseTraversalMethod_choose_Traversal_Traversal() {
        compare(g.V().map(__.choose(values("age"), bothE())), eval("g.V().map(__.choose(values('age'), bothE()))"));
    }

    @Test
    public void shouldParseTraversalMethod_choose_Traversal_Traversal_Traversal() {
        compare(g.V().map(__.choose(values("age"), bothE(), bothE())), eval("g.V().map(__.choose(values('age'), bothE(), bothE()))"));
    }

    @Test
    public void shouldParseTraversalMethod_coalesce() {
        compare(g.V().map(__.coalesce(outE("knows"))), eval("g.V().map(__.coalesce(outE('knows')))"));
    }

    @Test
    public void shouldParseTraversalMethod_coin() {
        compare(g.V().map(__.coin(2.5)), eval("g.V().map(__.coin(2.5))"));
    }

    @Test
    public void shouldParseTraversalMethod_coin_GValue() {
        antlrToLanguage = createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("probability", 2.5)));
        compare(g.V().map(__.coin(GValue.of("probability", 2.5))), eval("g.V().map(__.coin(probability))"));
    }

    @Test
    public void shouldParseTraversalMethod_combine_Object() {
        final ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        compare(g.V().map(__.values("name").fold().combine(list)),
                eval("g.V().map(__.values(\"name\").fold().combine([1,2]))"));
        compare(g.V().map(__.values("name").fold().combine(V().fold())),
                eval("g.V().map(__.values(\"name\").fold().combine(__.V().fold()))"));
    }

    @Test
    public void shouldParseTraversalMethod_constant() {
        compare(g.V().map(__.constant("yigit")), eval("g.V().map(__.constant('yigit'))"));
    }

    @Test
    public void shouldParseTraversalMethod_constant_gValue()  {
        compare(g.V().map(__.constant(GValue.of("foo", "bar"))), eval("g.V().map(__.constant(foo))"));
    }

    @Test
    public void shouldParseTraversalMethod_count_Empty() {
        compare(g.V().map(__.count()), eval("g.V().map(__.count())"));
    }

    @Test
    public void shouldParseTraversalMethod_count_Scope() {
        compare(g.V().map(__.count(global)), eval("g.V().map(__.count(global))"));
    }

    @Test
    public void shouldParseTraversalMethod_cyclicPath() {
        compare(g.V().map(__.cyclicPath()), eval("g.V().map(__.cyclicPath())"));
    }

    @Test
    public void shouldParseTraversalMethod_dedup_Scope_String() {
        compare(g.V().map(__.dedup(Scope.local, "age")), eval("g.V().map(__.dedup(local, 'age'))"));
    }

    @Test
    public void shouldParseTraversalMethod_dedup_String() {
        compare(g.V().map(__.dedup()), eval("g.V().map(__.dedup())"));
    }

    @Test
    public void shouldParseTraversalMethod_difference_Object() {
        final ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        compare(g.V().map(__.values("name").fold().difference(list)),
                eval("g.V().map(__.values(\"name\").fold().difference([1,2]))"));
        compare(g.V().map(__.values("name").fold().difference(V().fold())),
                eval("g.V().map(__.values(\"name\").fold().difference(__.V().fold()))"));
    }

    @Test
    public void shouldParseTraversalMethod_disjunct_Object() {
        final ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        compare(g.V().map(__.values("name").fold().disjunct(list)),
                eval("g.V().map(__.values(\"name\").fold().disjunct([1,2]))"));
        compare(g.V().map(__.values("name").fold().disjunct(V().fold())),
                eval("g.V().map(__.values(\"name\").fold().disjunct(__.V().fold()))"));
    }

    @Test
    public void shouldParseTraversalMethod_drop() {
        compare(g.V().map(__.drop()), eval("g.V().map(__.drop())"));
    }

    @Test
    public void shouldParseTraversalMethod_emit_Empty() {
        compare(g.V().map(__.emit()), eval("g.V().map(__.emit())"));
    }

    @Test
    public void shouldParseTraversalMethod_emit_Predicate() {
        compare(g.V().map(__.repeat(out()).emit(is("asd"))), eval("g.V().map(__.repeat(out()).emit(is(\"asd\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_emit_Traversal() {
        compare(g.V().map(__.emit(has("name"))), eval("g.V().map(__.emit(has('name')))"));
    }

    @Test
    public void shouldParseTraversalMethod_filter_Predicate() {
        compare(g.V().map(__.repeat(out()).filter(is("2"))), eval("g.V().map(__.repeat(out()).filter(is(\"2\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_filter_Traversal() {
        compare(g.V().map(__.filter(has("name"))), eval("g.V().map(__.filter(has('name')))"));
    }

    @Test
    public void shouldParseTraversalMethod_flatMap_Traversal() {
        compare(g.V().map(__.flatMap(has("name"))), eval("g.V().map(__.flatMap(has('name')))"));
    }

    @Test
    public void shouldParseTraversalMethod_fold_Empty() {
        compare(g.V().map(__.fold()), eval("g.V().map(__.fold())"));
    }

    @Test
    public void shouldParseTraversalMethod_fold_Function() {
        compare(g.V().values("age").map(__.fold(0, Operator.max)), eval("g.V().values('age').map(__.fold(0, max))"));
    }

    @Test
    public void shouldParseTraversalMethod_fold_Object_BiFunction() {
        compare(g.V().map(__.values("age").fold(0, Operator.max)), eval("g.V().map(__.values('age').fold(0, max))"));
    }

    @Test
    public void shouldParseTraversalMethod_from_String() {
        compare(g.V().map(__.cyclicPath().from("name")), eval("g.V().map(__.cyclicPath().from('name'))"));
    }

    @Test
    public void shouldParseTraversalMethod_from_Traversal() {
        compare(g.V().map(__.addE("as").from(V())), eval("g.V().map(__.addE('as').from(V()))"));
    }

    @Test
    public void shouldParseTraversalMethod_group_Empty() {
        compare(g.V().map(__.group()), eval("g.V().map(__.group())"));
    }

    @Test
    public void shouldParseTraversalMethod_group_String() {
        compare(g.V().map(__.group("age")), eval("g.V().map(__.group('age'))"));
    }

    @Test
    public void shouldParseTraversalMethod_groupCount_Empty() {
        compare(g.V().map(__.groupCount()), eval("g.V().map(__.groupCount())"));
    }

    @Test
    public void shouldParseTraversalMethod_groupCount_String() {
        compare(g.V().map(__.groupCount("age")), eval("g.V().map(__.groupCount('age'))"));
    }

    @Test
    public void shouldParseTraversalMethod_has_String() {
        compare(g.V().map(__.has("age")), eval("g.V().map(__.has('age'))"));
    }

    @Test
    public void shouldParseTraversalMethod_has_String_Object() {
        compare(g.V().map(__.has("age", 132)), eval("g.V().map(__.has('age', 132))"));
    }

    @Test
    public void shouldParseTraversalMethod_has_String_P() {
        compare(g.V().map(__.has("a", eq("b"))), eval("g.V().map(__.has(\"a\", eq(\"b\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_has_String_String_Object() {
        compare(g.V().map(__.has("a", "b", 3)), eval("g.V().map(__.has(\"a\", \"b\", 3))"));
    }

    @Test
    public void shouldParseTraversalMethod_has_String_String_P() {
        compare(g.V().map(__.has("a", "b", eq("c"))), eval("g.V().map(__.has(\"a\", \"b\", eq(\"c\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_has_String_Traversal() {
        compare(g.V().map(__.has("age", bothE())), eval("g.V().map(__.has('age', bothE()))"));
    }

    @Test
    public void shouldParseTraversalMethod_has_T_Object() {
        compare(g.V().map(__.has(T.id, 6)), eval("g.V().map(__.has(id, 6))"));
    }

    @Test
    public void shouldParseTraversalMethod_has_T_P() {
        compare(g.V().map(__.has(T.id, eq("asd"))), eval("g.V().map(__.has(id, eq('asd')))"));
    }

    @Test
    public void shouldParseTraversalMethod_has_T_Traversal() {
        compare(g.V().map(__.has(T.id, bothE())), eval("g.V().map(__.has(id, bothE()))"));
    }

    @Test
    public void shouldParseTraversalMethod_has_GValue() {
        compare(g.V().map(__.has(GValue.of("foo", "bar"), "b", eq("c"))), eval("g.V().map(__.has(foo, \"b\", eq(\"c\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_has_GValue_String_String() {
        compare(g.V().map(__.has(GValue.of("foo", "bar"), "b", "c")), eval("g.V().map(__.has(foo, \"b\", \"c\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_hasId_Object_Object() {
        compare(g.V().map(__.hasId(3, 4)), eval("g.V().map(__.hasId(3, 4))"));
    }

    @Test
    public void shouldParseTraversalMethod_hasId_P() {
        compare(g.V().map(__.hasId(gt(4))), eval("g.V().map(__.hasId(gt(4)))"));
    }

    @Test
    public void shouldParseTraversalMethod_hasKey_P() {
        compare(g.V().map(__.hasKey(eq("asd"))), eval("g.V().map(__.hasKey(eq(\"asd\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_hasKey_String_String() {
        compare(g.V().map(__.hasKey("age")), eval("g.V().map(__.hasKey('age'))"));
        compare(g.V().map(__.hasKey("age", "3")), eval("g.V().map(__.hasKey('age', '3'))"));
    }

    @Test
    public void shouldParseTraversalMethod_hasLabel_P() {
        compare(g.V().map(__.hasLabel(eq("asd"))), eval("g.V().map(__.hasLabel(eq(\"asd\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_hasLabel_String_String() {
        compare(g.V().map(__.hasLabel("age")), eval("g.V().map(__.hasLabel('age'))"));
        compare(g.V().map(__.hasLabel("age", "3")), eval("g.V().map(__.hasLabel('age', '3'))"));
    }

    @Test
    public void shouldParseTraversalMethod_hasLabel_GValue() {
        compare(g.V().map(__.hasLabel(GValue.of("foo", "bar"))), eval("g.V().map(__.hasLabel(foo))"));
    }

    @Test
    public void shouldParseTraversalMethod_hasNot() {
        compare(g.V().map(__.hasNot("know")), eval("g.V().map(__.hasNot('know'))"));
    }

    @Test
    public void shouldParseTraversalMethod_hasValue_Object_Object() {
        compare(g.V().map(__.hasValue(3, 4)), eval("g.V().map(__.hasValue(3, 4))"));
    }

    @Test
    public void shouldParseTraversalMethod_hasValue_P() {
        compare(g.V().map(__.hasValue(eq(2))), eval("g.V().map(__.hasValue(eq(2)))"));
    }

    @Test
    public void shouldParseTraversalMethod_id() {
        compare(g.V().map(__.id()), eval("g.V().map(__.id())"));
    }

    @Test
    public void shouldParseTraversalMethod_identity() {
        compare(g.V().map(__.identity()), eval("g.V().map(__.identity())"));
    }

    @Test
    public void shouldParseTraversalMethod_in_String() {
        compare(g.V().map(__.in("created")), eval("g.V().map(__.in('created'))"));
    }

    @Test
    public void shouldParseTraversalMethodl_in_GValue()  {
        compare(g.V().map(__.in(GValue.ofString("foo", "bar"))), eval("g.V().map(__.in(foo))"));
    }

    @Test
    public void shouldParseTraversalMethod_in()  {
        compare(g.V().map(__.in()), eval("g.V().map(__.in())"));
    }

    @Test
    public void shouldParseTraversalMethod_index() {
        compare(g.V().map(__.hasLabel("software").index()), eval("g.V().map(__.hasLabel('software').index())"));
    }

    @Test
    public void shouldParseTraversalMethod_inE() {
        compare(g.V().map(__.inE()), eval("g.V().map(__.inE())"));    }

    @Test
    public void shouldParseTraversalMethod_inE_String() {
        compare(g.V().map(__.inE("created")), eval("g.V().map(__.inE('created'))"));
    }

    @Test
    public void shouldParseTraversalMethod_inE_GValue() {
        compare(g.V().map(__.inE(GValue.ofString("foo", "bar"))), eval("g.V().map(__.inE(foo))"));
    }

    @Test
    public void shouldParseTraversalMethod_inV() {
        compare(g.V().map(__.inV()), eval("g.V().map(__.inV())"));
    }

    @Test
    public void shouldParseTraversalMethod_inject() {
        compare(g.V(4).out().values("name").map(__.inject("daniel")),
                eval("g.V(4).out().values(\"name\").map(__.inject(\"daniel\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_intersect_Object() {
        final ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        compare(g.V().map(__.values("name").fold().intersect(list)),
                eval("g.V().map(__.values(\"name\").fold().intersect([1,2]))"));
        compare(g.V().map(__.values("name").fold().intersect(V().fold())),
                eval("g.V().map(__.values(\"name\").fold().intersect(__.V().fold()))"));
    }

    @Test
    public void shouldParseTraversalMethod_is_Object() {
        compare(g.V().map(__.is(4)), eval("g.V().map(__.is(4))"));
    }

    @Test
    public void shouldParseTraversalMethod_is_P() {
        compare(g.V().map(__.is(gt(4))), eval("g.V().map(__.is(gt(4)))"));
    }

    @Test
    public void shouldParseTraversalMethod_conjoin_Object() {
        compare(g.V().map(__.values("name").fold().conjoin(";")),
                eval("g.V().map(__.values(\"name\").fold().conjoin(\";\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_key() {
        compare(g.V().map(__.key()), eval("g.V().map(__.key())"));
    }

    @Test
    public void shouldParseTraversalMethod_label() {
        compare(g.V().map(__.label()), eval("g.V().map(__.label())"));
    }

    @Test
    public void shouldParseTraversalMethod_limit_Scope_long() {
        compare(g.V().map(__.limit(global, 3)), eval("g.V().map(__.limit(global, 3))"));
    }

    @Test
    public void shouldParseTraversalMethod_limit_long() {
        compare(g.V().map(__.limit(2)), eval("g.V().map(__.limit(2))"));
    }

    @Test
    public void shouldParseTraversalMethod_local() {
        compare(g.V().map(__.local(bothE())), eval("g.V().map(__.local(bothE()))"));
    }

    @Test
    public void shouldParseTraversalMethod_loops() {
        compare(g.V().map(__.loops()), eval("g.V().map(__.loops())"));
    }

    @Test
    public void shouldParseTraversalMethod_loops_String() {
        compare(g.V().map(__.loops("test")), eval("g.V().map(__.loops(\"test\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_map_Traversal() {
        compare(g.V().map(__.map(bothE())), eval("g.V().map(__.map(bothE()))"));
    }

    @Test
    public void shouldParseTraversalMethod_flatMap() {
        compare(g.V().map(__.flatMap(bothE())), eval("g.V().map(__.flatMap(bothE()))"));    }

    @Test
    public void shouldParseTraversalMethod_match() {
        compare(g.V().map(__.match(as("a"), as("b"))), eval("g.V().map(__.match(as(\"a\"), as(\"b\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_max_Empty() {
        compare(g.V().map(__.max()), eval("g.V().map(__.max())"));
    }

    @Test
    public void shouldParseTraversalMethod_max_Scope() {
        compare(g.V().map(__.max(Scope.local)), eval("g.V().map(__.max(local))"));
    }

    @Test
    public void shouldParseTraversalMethod_math() {
        compare(g.V().count().map(__.math("_ + 10")), eval("g.V().count().map(__.math('_ + 10'))"));
    }

    @Test
    public void shouldParseTraversalMethod_mean_Empty() {
        compare(g.V().map(__.mean()), eval("g.V().map(__.mean())"));
    }

    @Test
    public void shouldParseTraversalMethod_mean_Scope() {
        compare(g.V().map(__.mean(global)), eval("g.V().map(__.mean(global))"));
    }

    @Test
    public void shouldParseTraversalMethod_merge_Object() {
        final ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        compare(g.V().map(__.values("name").fold().merge(list)),
                eval("g.V().map(__.values(\"name\").fold().merge([1,2]))"));
        compare(g.V().map(__.values("name").fold().merge(V().fold())),
                eval("g.V().map(__.values(\"name\").fold().merge(__.V().fold()))"));
    }

    @Test
    public void shouldParseTraversalMethod_min_Empty() {
        compare(g.V().map(__.min()), eval("g.V().map(__.min())"));
    }

    @Test
    public void shouldParseTraversalMethod_min_Scope() {
        compare(g.V().map(__.min(Scope.local)), eval("g.V().map(__.min(local))"));
    }

    @Test
    public void shouldParseTraversalMethod_not() {
        compare(g.V().map(__.not(both())), eval("g.V().map(__.not(both()))"));
    }

    @Test
    public void shouldParseTraversalMethod_option_Object_Traversal() {
        compare(g.V().map(__.branch(values("name")).option(2, bothE())),
                eval("g.V().map(__.branch(values(\"name\")).option(2, bothE()))"));
    }

    @Test
    public void shouldParseTraversalMethod_option_Traversal() {
        compare(g.V().map(__.branch(values("name")).option(both())), eval("g.V().map(__.branch(values(\"name\")).option(both()))"));
    }

    @Test
    public void shouldParseTraversalMethod_optional() {
        compare(g.V().map(__.optional(min())), eval("g.V().map(__.optional(min()))"));
    }

    @Test
    public void shouldParseTraversalMethod_or() {
        compare(g.V().map(__.or(as("a"), as("b"))), eval("g.V().map(__.or(as(\"a\"), as(\"b\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_order_Empty() {
        compare(g.V().map(__.order()), eval("g.V().map(__.order())"));
    }

    @Test
    public void shouldParseTraversalMethod_order_Scope() {
        compare(g.V().map(__.order(global)), eval("g.V().map(__.order(global))"));
    }

    @Test
    public void shouldParseTraversalMethod_otherV() {
        compare(g.V().map(__.otherV()), eval("g.V().map(__.otherV())"));
    }

    @Test
    public void shouldParseTraversalMethod_out_String() {
        compare(g.V().map(__.out("a", "b")), eval("g.V().map(__.out(\"a\", \"b\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_out_GValue()  {
        compare(g.V().map(__.out(GValue.ofString("foo", "bar"))), eval("g.V().map(__.out(foo))"));
    }

    @Test
    public void shouldParseTraversalMethod_out()  {
        compare(g.V().map(__.out()), eval("g.V().map(__.out())"));
    }

    @Test
    public void shouldParseTraversalMethod_outE() {
        compare(g.V().map(__.outE()), eval("g.V().map(__.outE())"));
    }

    @Test
    public void shouldParseTraversalMethod_outE_String() {
        compare(g.V().map(__.outE("a", "b")), eval("g.V().map(__.outE(\"a\", \"b\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_outE_GValue() {
        compare(g.V().map(__.outE(GValue.ofString("foo", "bar"))), eval("g.V().map(__.outE(foo))"));
    }

    @Test
    public void shouldParseTraversalMethod_outV() {
        compare(g.V().map(__.outV()), eval("g.V().map(__.outV())"));
    }

    @Test
    public void shouldParseTraversalMethod_path() {
        compare(g.V().map(__.path()), eval("g.V().map(__.path())"));
    }

    @Test
    public void shouldParseTraversalMethod_product_Object() {
        final ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        compare(g.V().map(__.values("name").fold().product(list)),
                eval("g.V().map(__.values(\"name\").fold().product([1,2]))"));
        compare(g.V().map(__.values("name").fold().product(V().fold())),
                eval("g.V().map(__.values(\"name\").fold().product(__.V().fold()))"));
    }

    @Test
    public void shouldParseTraversalMethod_project() {
        compare(g.V().map(__.project("neptune")), eval("g.V().map(__.project('neptune'))"));
        compare(g.V().map(__.project("neptune", "uranus")), eval("g.V().map(__.project('neptune', 'uranus'))"));
    }

    @Test
    public void shouldParseTraversalMethod_properties() {
        compare(g.V().map(__.properties("venus", "mars")), eval("g.V().map(__.properties('venus', 'mars'))"));
    }

    @Test
    public void shouldParseTraversalMethod_property_Cardinality_Object_Object_Object() {
        compare(g.V().map(__.property(Cardinality.list,1,2,"key", 4)),
                eval("g.V().map(__.property(list, 1,2,'key',4))"));
    }

    @Test
    public void shouldParseTraversalMethod_property_Object_Object_Object() {
        compare(g.V().map(__.property(1,2,"key", 4)), eval("g.V().map(__.property(1,2,'key',4))"));
    }

    @Test
    public void shouldParseTraversalMethod_property_Object() {
        final LinkedHashMap<Object, Object> map = new LinkedHashMap<>();
        map.put("key", "foo");
        map.put("key1", "bar");
        compare(g.V().map(__.property(map)), eval("g.V().map(__.property(['key': 'foo', 'key1': 'bar']))"));
        map.clear();
        map.put("name", "foo");
        map.put("age", 42);
        compare(g.V().map(__.addV().property(map)), eval("g.V().map(__.addV().property([\"name\": \"foo\", \"age\": 42 ]))"));
        map.clear();
        map.put(label, "foo");
        map.put("age", 42);
        compare(g.V().map(__.addV().property(map)), eval("g.V().map(__.addV().property([T.label: \"foo\", \"age\": 42 ]))"));
    }

    @Test
    public void shouldParseTraversalMethod_property_Cardinality_Object() {
        final LinkedHashMap<Object, Object> map = new LinkedHashMap<>();
        map.put("key", "foo");
        map.put("key1", "bar");
        compare(g.V().map(__.property(Cardinality.list, map)), eval("g.V().map(__.property(list, ['key': 'foo', 'key1': 'bar']))"));
    }

    @Test
    public void shouldParseTraversalMethod_propertyMap() {
        compare(g.V().map(__.propertyMap("venus", "mars")), eval("g.V().map(__.propertyMap('venus', 'mars'))"));
    }

    @Test
    public void shouldParseTraversalMethod_elementMap()  {
        compare(g.V().map(__.elementMap("test")), eval("g.V().map(__.elementMap(\"test\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_range_Scope_long_long() {
        compare(g.V().map(__.range(global, 3,5)), eval("g.V().map(__.range(global, 3,5))"));
    }

    @Test
    public void shouldParseTraversalMethod_range_long_long() {
        compare(g.V().map(__.range(3,5)), eval("g.V().map(__.range(3,5))"));
    }

    @Test
    public void shouldParseTraversalMethod_repeat() {
        compare(g.V().map(__.repeat(both())), eval("g.V().map(__.repeat(both()))"));
    }

    @Test
    public void shouldParseTraversalMethod_repeat_String_Traversal() {
        compare(g.V().map(__.repeat("test", both())), eval("g.V().map(__.repeat(\"test\", both()))"));
    }

    @Test
    public void shouldParseTraversalMethod_sack_BiFunction() {
        compare(g.V().map(__.sack()), eval("g.V().map(__.sack())"));
        compare(g.V().map(__.sack(Operator.addAll)), eval("g.V().map(__.sack(addAll))"));
        compare(g.V().map(__.sack(Operator.and)), eval("g.V().map(__.sack(and))"));
        compare(g.V().map(__.sack(Operator.assign)), eval("g.V().map(__.sack(assign))"));
        compare(g.V().map(__.sack(Operator.div)), eval("g.V().map(__.sack(div))"));
        compare(g.V().map(__.sack(Operator.max)), eval("g.V().map(__.sack(max))"));
        compare(g.V().map(__.sack(Operator.min)), eval("g.V().map(__.sack(min))"));
        compare(g.V().map(__.sack(Operator.minus)), eval("g.V().map(__.sack(minus))"));
        compare(g.V().map(__.sack(Operator.mult)), eval("g.V().map(__.sack(mult))"));
        compare(g.V().map(__.sack(Operator.or)), eval("g.V().map(__.sack(or))"));
        compare(g.V().map(__.sack(Operator.sum)), eval("g.V().map(__.sack(sum))"));
        compare(g.V().map(__.sack(Operator.sumLong)), eval("g.V().map(__.sack(sumLong))"));
    }

    @Test
    public void shouldParseTraversalMethod_sack_Empty() {
        compare(g.V().map(__.sack()), eval("g.V().map(__.sack())"));
    }

    @Test
    public void shouldParseTraversalMethod_sample_Scope_int() {
        compare(g.V().map(__.sample(global, 2)), eval("g.V().map(__.sample(global, 2))"));
    }

    @Test
    public void shouldParseTraversalMethod_sample_int() {
        compare(g.V().map(__.sample(4)), eval("g.V().map(__.sample(4))"));
    }

    @Test
    public void shouldParseTraversalMethod_select_Column() {
        compare(g.V().map(__.select(Column.keys)), eval("g.V().map(__.select(keys))"));
    }

    @Test
    public void shouldParseTraversalMethod_select_Pop_String() {
        compare(g.V().map(__.select(Pop.first, "asd")), eval("g.V().map(__.select(first, 'asd'))"));
    }

    @Test
    public void shouldParseTraversalMethod_select_Pop_String_String_String() {
        compare(g.V().map(__.select(Pop.all, "a", "b", "c", "d")), eval("g.V().map(__.select(all, \"a\", \"b\", \"c\", \"d\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_select_Pop_Traversal() {
        compare(g.V().map(__.select(Pop.all, out().properties("a"))), eval("g.V().map(__.select(all, out().properties(\"a\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_select_String() {
        compare(g.V().map(__.select("yigit")), eval("g.V().map(__.select(\"yigit\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_select_String_String_String() {
        compare(g.V().map(__.select("a", "b", "c", "d")), eval("g.V().map(__.select(\"a\", \"b\", \"c\", \"d\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_select_Traversal() {
        compare(g.V().map(__.select(out().properties("a"))), eval("g.V().map(__.select(out().properties(\"a\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_sideEffect() {
        compare(g.V().map(__.sideEffect(bothE())), eval("g.V().map(__.sideEffect(bothE()))"));
    }

    @Test
    public void shouldParseTraversalMethod_simplePath() {
        compare(g.V().map(__.simplePath()), eval("g.V().map(__.simplePath())"));
    }

    @Test
    public void shouldParseTraversalMethod_skip_Scope_long() {
        compare(g.V().map(__.skip(global, 8)), eval("g.V().map(__.skip(global, 8))"));
    }

    @Test
    public void shouldParseTraversalMethod_skip_long() {
        compare(g.V().map(__.skip(8)), eval("g.V().map(__.skip(8))"));
    }

    @Test
    public void shouldParseTraversalMethod_store() {
        compare(g.V().map(__.store("asd")), eval("g.V().map(__.store(\"asd\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_subgraph() {
        compare(g.V().map(__.subgraph("asd")), eval("g.V().map(__.subgraph('asd'))"));
    }

    @Test
    public void shouldParseTraversalMethod_sum_Empty() {
        compare(g.V().map(__.sum()), eval("g.V().map(__.sum())"));
    }

    @Test
    public void shouldParseTraversalMethod_sum_Scope() {
        compare(g.V().map(__.sum(Scope.local)), eval("g.V().map(__.sum(local))"));
    }

    @Test
    public void shouldParseTraversalMethod_tail_Empty() {
        compare(g.V().map(__.tail()), eval("g.V().map(__.tail())"));
    }

    @Test
    public void shouldParseTraversalMethod_tail_Scope() {
        compare(g.V().map(__.tail(Scope.local)), eval("g.V().map(__.tail(local))"));
    }

    @Test
    public void shouldParseTraversalMethod_tail_Scope_long() {
        compare(g.V().map(__.tail(Scope.local, 3)), eval("g.V().map(__.tail(local, 3))"));
    }

    @Test
    public void shouldParseTraversalMethod_tail_long() {
        compare(g.V().map(__.tail(4)), eval("g.V().map(__.tail(4))"));
    }

    @Test
    public void shouldParseTraversalMethod_timeLimit() {
        compare(g.V().map(__.timeLimit(5)), eval("g.V().map(__.timeLimit(5))"));
    }

    @Test
    public void shouldParseTraversalMethod_times() {
        compare(g.V().map(__.times(6)), eval("g.V().map(__.times(6))"));
    }

    @Test
    public void shouldParseTraversalMethod_to_Direction_String() {
        compare(g.V().map(__.to(Direction.IN, "asd")), eval("g.V().map(__.to(IN, 'asd'))"));
    }

    @Test
    public void shouldParseTraversalMethod_GValue()  {
        compare(g.V().map(__.to(Direction.OUT, GValue.ofString("foo", "bar"))), eval("g.V().map(__.to(Direction.OUT,foo))"));
    }

    @Test
    public void shouldParseTraversalMethod_Direction()  {
        compare(g.V().map(__.to(Direction.OUT)), eval("g.V().map(__.to(Direction.OUT))"));
    }

    @Test
    public void shouldParseTraversalMethod_to_String() {
        compare(g.V().map(__.path().to("home")), eval("g.V().map(__.path().to(\"home\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_to_Traversal() {
        compare(g.V().map(__.addE("as").to(V())), eval("g.V().map(__.addE('as').to(V()))"));
    }

    @Test
    public void shouldParseTraversalMethod_toE() {
        compare(g.V().map(__.toE(Direction.IN, "asd")), eval("g.V().map(__.toE(IN, 'asd'))"));
    }

    @Test
    public void shouldParseTraversalMethod_toE_Direction() {
        compare(g.V().map(__.toE(Direction.OUT)), eval("g.V().map(__.toE(Direction.OUT))"));
    }

    @Test
    public void shouldParseTraversalMethod_toE_Direction_GValue() {
        compare(g.V().map(__.toE(Direction.OUT, GValue.ofString("foo", "bar"))), eval("g.V().map(__.toE(Direction.OUT,foo))"));
    }

    @Test
    public void shouldParseTraversalMethod_toV() {
        compare(g.V().map(__.toV(Direction.IN)), eval("g.V().map(__.toV(IN))"));
    }

    @Test
    public void shouldParseTraversalMethod_tree_Empty() {
        compare(g.V().map(__.tree()), eval("g.V().map(__.tree())"));
    }

    @Test
    public void shouldParseTraversalMethod_tree_String() {
        compare(g.V().map(__.tree("hello")), eval("g.V().map(__.tree(\"hello\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_unfold() {
        compare(g.V().map(__.unfold()), eval("g.V().map(__.unfold())"));
    }

    @Test
    public void shouldParseTraversalMethod_union() {
        compare(g.V().map(__.union(in(), out())), eval("g.V().map(__.union(in(), out()))"));
    }

    @Test
    public void shouldParseTraversalMethod_until_Predicate() {
        compare(g.V().map(__.until(is("123"))), eval("g.V().map(__.until(is(\"123\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_until_Traversal() {
        compare(g.V().map(__.until(has("ripple"))), eval("g.V().map(__.until(has(\"ripple\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_value() {
        compare(g.V().map(__.value()), eval("g.V().map(__.value())"));
    }

    @Test
    public void shouldParseTraversalMethod_valueMap_String() {
        compare(g.V().map(__.valueMap("yigit")), eval("g.V().map(__.valueMap(\"yigit\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_valueMap_boolean_String1() {
        compare(g.V().map(__.valueMap(true)), eval("g.V().map(__.valueMap(true))"));
    }

    @Test
    public void shouldParseTraversalMethod_valueMap_boolean_String2() {
        compare(g.V().map(__.valueMap(true, "that")), eval("g.V().map(__.valueMap(true, \"that\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_valueMap_withOption() {
        compare(g.V().map(__.valueMap().with(WithOptions.tokens, WithOptions.labels)),
                eval("g.V().map(__.valueMap().with(WithOptions.tokens, WithOptions.labels))"));
    }

    @Test
    public void shouldParseTraversalMethod_values() {
        compare(g.V().map(__.values("earth", "mars")), eval("g.V().map(__.values(\"earth\", \"mars\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_where_P() {
        compare(g.V().map(__.where(eq("123"))), eval("g.V().map(__.where(eq(\"123\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_where_String_P() {
        compare(g.V().map(__.where("age", eq("123"))), eval("g.V().map(__.where('age', eq(\"123\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_where_Traversal() {
        compare(g.V().map(__.where(both())), eval("g.V().map(__.where(both()))"));
    }

    @Test
    public void visitTraversalMethod_with_String() {
        compare(g.V().map(__.valueMap().with("hakuna")), eval("g.V().map(__.valueMap().with('hakuna'))"));
    }

    @Test
    public void visitTraversalMethod_with_String_Object() {
        compare(g.V().map(__.index().with(WithOptions.indexer, WithOptions.map)),
                eval("g.V().map(__.index().with(WithOptions.indexer, WithOptions.map))"));
    }

    @Test
    public void visitTraversalMethod_withOptionsTokensAll() {
        compare(g.V().map(__.has("code","AUS").valueMap().with(WithOptions.tokens,WithOptions.all).unfold()),
                eval("g.V().map(__.has('code','AUS').valueMap().with(WithOptions.tokens,WithOptions.all).unfold())"));
    }

    @Test
    public void visitTraversalMethod_withOptionsTokensNone() {
        compare(g.V().map(__.has("code","AUS").valueMap().with(WithOptions.tokens,WithOptions.none)),
                eval("g.V().map(__.has('code','AUS').valueMap().with(WithOptions.tokens,WithOptions.none))"));
    }

    @Test
    public void visitTraversalMethod_withOptionsTokensIds() {
        compare(g.V().map(__.has("code","AUS").valueMap().with(WithOptions.tokens,WithOptions.ids)),
                eval("g.V().map(__.has('code','AUS').valueMap().with(WithOptions.tokens,WithOptions.ids))"));
    }

    @Test
    public void visitTraversalMethod_withOptionsTokensLabels() {
        compare(g.V().map(__.has("code","AUS").valueMap().with(WithOptions.tokens,WithOptions.labels)),
                eval("g.V().map(__.has('code','AUS').valueMap().with(WithOptions.tokens,WithOptions.labels))"));
    }

    @Test
    public void visitTraversalMethod_withOptionsTokensKeys() {
        compare(g.V().map(__.has("code","AUS").valueMap().with(WithOptions.tokens,WithOptions.keys)),
                eval("g.V().map(__.has('code','AUS').valueMap().with(WithOptions.tokens,WithOptions.keys))"));
    }

    @Test
    public void visitTraversalMethod_withOptionsTokensValues() {
        compare(g.V().map(__.has("code","AUS").valueMap().with(WithOptions.tokens,WithOptions.values)),
                eval("g.V().map(__.has('code','AUS').valueMap().with(WithOptions.tokens,WithOptions.values))"));
    }

    @Test
    public void visitTraversalMethod_withOptionsIndexerList() {
        compare(g.V().map(__.has("code","AUS").valueMap().with(WithOptions.indexer,WithOptions.list)),
                eval("g.V().map(__.has('code','AUS').valueMap().with(WithOptions.indexer,WithOptions.list))"));
    }
    @Test
    public void visitTraversalMethod_withOptionsIndexerMap() {
        compare(g.V().map(__.has("code","AUS").valueMap().with(WithOptions.indexer,WithOptions.map)),
                eval("g.V().map(__.has('code','AUS').valueMap().with(WithOptions.indexer,WithOptions.map))"));
    }

    @Test
    public void shouldParseTraversalMethod_midTraversal_E() {
        compare(g.V().map(__.inject(1).E()), eval("g.V().map(__.inject(1).E())"));
    }

    @Test
    public void shouldParseTraversalMethod_midTraversal_E_multipleArgs() {
        compare(g.V().map(__.inject(1).E(2,null)), eval("g.V().map(__.inject(1).E(2,null))"));
    }

    @Test
    public void shouldParseTraversalMethod_midTraversal_E_spawning() {
        compare(g.V().map(__.coalesce(E(),addE("person"))), eval("g.V().map(__.coalesce(__.E(),__.addE('person')))"));
    }

    @Test
    public void shouldParseTraversalMethod_midTraversal_E_multipleArgs_spawning() {
        compare(g.V().map(__.coalesce(E(1,2),addE("person"))),
                eval("g.V().map(__.coalesce(__.E(1,2),__.addE('person')))"));
    }

    @Test
    public void shouldParseTraversalMethod_concat_Empty() {
        compare(g.V().map(__.concat()), eval("g.V().map(__.concat())"));
    }

    @Test
    public void shouldParseTraversalMethod_concat_multipleStringArgs() {
        compare(g.V().map(__.concat("hello", "world")), eval("g.V().map(__.concat('hello', 'world'))"));
    }

    @Test
    public void shouldParseTraversalMethod_concat_traversal() {
        compare(g.V().map(__.concat(constant("hello"))),
                eval("g.V().map(__.concat(__.constant('hello')))"));
    }

    @Test
    public void shouldParseTraversalMethod_concat_multipleTraversalArgs() {
        compare(g.V().map(__.concat(constant("hello"), constant("world"))),
                eval("g.V().map(__.concat(__.constant('hello'), __.constant('world')))"));
    }

    @Test
    public void shouldParseTraversalMethod_concat_ArgsWithNulls() {
        compare(g.V().map(__.concat(null, "hello")),
                eval("g.V().map(__.concat(null, 'hello'))"));
    }

    @Test
    public void shouldParseTraversalMethod_asString_Empty() {
        compare(g.V().map(__.asString()), eval("g.V().map(__.asString())"));
    }

    @Test
    public void shouldParseTraversalMethod_asString__Scope() {
        compare(g.V().map(__.asString(Scope.global)), eval("g.V().map(__.asString(Scope.global))"));
    }

    @Test
    public void shouldParseTraversalMethod_format_String() {
        compare(g.V().map(__.format("Hello %{name}")), eval("g.V().map(__.format(\"Hello %{name}\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_toLower_Empty() {
        compare(g.V().map(__.toLower()), eval("g.V().map(__.toLower())"));
    }

    @Test
    public void shouldParseTraversalMethod_toLower_Scope() {
        compare(g.V().map(__.toLower(Scope.global)), eval("g.V().map(__.toLower(Scope.global))"));
    }

    @Test
    public void shouldParseTraversalMethod_toUpper_Empty() {
        compare(g.V().map(__.toUpper()), eval("g.V().map(__.toUpper())"));
    }

    @Test
    public void shouldParseTraversalMethod_toUpper_Scope() {
        compare(g.V().map(__.toUpper(Scope.global)), eval("g.V().map(__.toUpper(Scope.global))"));
    }

    @Test
    public void shouldParseTraversalMethod_trim()  {
        compare(g.V().map(__.trim()), eval("g.V().map(__.trim())"));
    }

    @Test
    public void shouldParseTraversalMethod_trim_Scope()  {
        compare(g.V().map(__.trim(Scope.global)), eval("g.V().map(__.trim(Scope.global))"));
    }

    @Test
    public void shouldParseTraversalMethod_lTrim()  {
        compare(g.V().map(__.lTrim()), eval("g.V().map(__.lTrim())"));
    }

    @Test
    public void shouldParseTraversalMethod_lTrim_Scope()  {
        compare(g.V().map(__.lTrim(Scope.global)), eval("g.V().map(__.lTrim(Scope.global))"));
    }

    @Test
    public void shouldParseTraversalMethod_rTrim()  {
        compare(g.V().map(__.rTrim()), eval("g.V().map(__.rTrim())"));
    }

    @Test
    public void shouldParseTraversalMethod_rTrim_Scope()  {
        compare(g.V().map(__.rTrim(Scope.global)), eval("g.V().map(__.rTrim(Scope.global))"));
    }

    @Test
    public void shouldParseTraversalMethod_reverse()  {
        compare(g.V().map(__.reverse()), eval("g.V().map(__.reverse())"));
    }

    @Test
    public void shouldParseTraversalMethod_length_Empty() {
        compare(g.V().map(__.length()), eval("g.V().map(__.length())"));
    }

    @Test
    public void shouldParseTraversalMethod_length_Scope() {
        compare(g.V().map(__.length(Scope.global)), eval("g.V().map(__.length(Scope.global))"));
    }

    @Test
    public void shouldParseTraversalMethod_replace_string_string() {
        compare(g.V().map(__.replace("a", "b")), eval("g.V().map(__.replace('a', 'b'))"));
    }

    @Test
    public void shouldParseTraversalMethod_replace_Scope() {
        compare(g.V().map(__.replace(Scope.global, "hello", "world")), eval("g.V().map(__.replace(Scope.global, \"hello\", \"world\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_split_string() {
        compare(g.V().map(__.split("a")), eval("g.V().map(__.split('a'))"));
    }

    @Test
    public void shouldParseTraversalMethod_split_Scope() {
        compare(g.V().map(__.split(Scope.global, ",")), eval("g.V().map(__.split(Scope.global, \",\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_substring_long() {
        compare(g.V().map(__.substring(1)), eval("g.V().map(__.substring(1))"));
    }

    @Test
    public void shouldParseTraversalMethod_substring_long_long() {
        compare(g.V().map(__.substring(1, 3)), eval("g.V().map(__.substring(1, 3))"));
    }

    @Test
    public void shouldParseTraversalMethod_substring_long_Scope() {
        compare(g.V().map(__.substring(Scope.global, 5)), eval("g.V().map(__.substring(Scope.global, 5))"));
    }

    @Test
    public void shouldParseTraversalMethod_substring_long_long_Scope() {
        compare(g.V().map(__.substring(Scope.global, 5, 9)), eval("g.V().map(__.substring(Scope.global, 5, 9))"));
    }

    @Test
    public void shouldParseTraversalMethod_asDate() {
        compare(g.V().map(__.asDate()), eval("g.V().map(__.asDate())"));
    }

    @Test
    public void shouldParseTraversalMethod_dateAdd() {
        compare(g.V().map(__.dateAdd(DT.day, 2)), eval("g.V().map(__.dateAdd(DT.day, 2))"));
    }

    @Test
    public void shouldParseTraversalMethod_dateDiff() {
        compare(g.V().map(__.dateDiff(DatetimeHelper.datetime("2024-11-26"))), eval("g.V().map(__.dateDiff(datetime(\"2024-11-26\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_dateDiff_Traversal() {
        compare(g.V().map(__.dateDiff(__.constant(DatetimeHelper.datetime("2024-11-26")))), eval("g.V().map(__.dateDiff(__.constant(datetime(\"2024-11-26\"))))"));
    }

    @Test
    public void shouldParseTraversalMethod_difference() {
        compare(g.V().map(__.difference("test")), eval("g.V().map(__.difference(\"test\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_difference_GValue() {
        compare(g.V().map(__.difference(GValue.of("foo", "bar"))), eval("g.V().map(__.difference(foo))"));
    }

    @Test
    public void shouldParseTraversalMethod_disjunct() {
        compare(g.V().map(__.disjunct("test")), eval("g.V().map(__.disjunct(\"test\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_disjunct_GValue() {
        compare(g.V().map(__.disjunct(GValue.of("foo", "bar"))), eval("g.V().map(__.disjunct(foo))"));
    }

    @Test
    public void shouldParseTraversalMethod_intersect() {
        compare(g.V().map(__.intersect("test")), eval("g.V().map(__.intersect(\"test\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_intersect_GValue() {
        compare(g.V().map(__.intersect(GValue.of("foo", "bar"))), eval("g.V().map(__.intersect(foo))"));
    }

    @Test
    public void shouldParseTraversalMethod_conjoin() {
        compare(g.V().map(__.conjoin("test")), eval("g.V().map(__.conjoin(\"test\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_conjoin_GValue() {
        compare(g.V().map(__.conjoin(GValue.of("foo", "bar"))), eval("g.V().map(__.conjoin(foo))"));
    }

    @Test
    public void shouldParseTraversalMethod_merge() {
        compare(g.V().map(__.merge("test")), eval("g.V().map(__.merge(\"test\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_merge_GValue() {
        compare(g.V().map(__.merge(GValue.of("foo", "bar"))), eval("g.V().map(__.merge(foo))"));
    }

    @Test
    public void shouldParseTraversalMethod_combine() {
        compare(g.V().map(__.combine("test")), eval("g.V().map(__.combine(\"test\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_combine_GValue() {
        compare(g.V().map(__.combine(GValue.of("foo", "bar"))), eval("g.V().map(__.combine(foo))"));
    }

    @Test
    public void shouldParseTraversalMethod_product() {
        compare(g.V().map(__.product("test")), eval("g.V().map(__.product(\"test\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_product_GValue() {
        compare(g.V().map(__.product(GValue.of("foo", "bar"))), eval("g.V().map(__.product(foo))"));
    }

    @Test
    public void shouldParseTraversalMethod_all() {
        compare(g.V().map(__.all(P.eq("test"))), eval("g.V().map(__.all(P.eq(\"test\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_any() {
        compare(g.V().map(__.any(P.eq("test"))), eval("g.V().map(__.any(P.eq(\"test\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_none() {
        compare(g.V().map(__.none(P.eq("test"))), eval("g.V().map(__.none(P.eq(\"test\")))"));
    }

    @Test
    public void shouldParseTraversalMethod_fail() {
        compare(g.V().map(__.fail()), eval("g.V().map(__.fail())"));
    }

    @Test
    public void shouldParseTraversalMethod_fail_String() {
        compare(g.V().map(__.fail("test")), eval("g.V().map(__.fail(\"test\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_element() {
        compare(g.V().map(__.element()), eval("g.V().map(__.element())"));
    }

    @Test
    public void shouldParseTraversalMethod_call() {
        compare(g.V().map(__.call("test")), eval("g.V().map(__.call(\"test\"))"));
    }

    @Test
    public void shouldParseTraversalMethod_call_String_Traversal() {
        compare(g.V().map(__.call("test", __.inject(CollectionUtil.asMap("a", "b")))), eval("g.V().map(__.call(\"test\", __.inject([\"a\":\"b\"])))"));
    }

    @Test
    public void shouldParseTraversalMethod_call_Map() {
        compare(g.V().map(__.call("test", CollectionUtil.asMap("x", "y"))), eval("g.V().map(__.call(\"test\", [\"x\":\"y\"]))"));
    }

    @Test
    public void shouldParseTraversalMethod_call_Map_Traversal() {
        compare(g.V().map(__.call("test", CollectionUtil.asMap("x", "y"), __.inject(CollectionUtil.asMap("a", "b")))), eval("g.V().map(__.call(\"test\", [\"x\":\"y\"], __.inject([\"a\":\"b\"])))"));
    }

    @Test
    public void shouldParseTraversalMethod_call_GValue() {
        antlrToLanguage = createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("test", CollectionUtil.asMap("foo", "bar"))));
        compare(g.V().map(__.call("svc", GValue.ofMap("test", CollectionUtil.asMap("foo", "bar")))), eval("g.V().map(__.call(\"svc\", test))"));
    }

    @Test
    public void shouldParseTraversalMethod_call_GValue_Traversal() {
        antlrToLanguage = createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("test", CollectionUtil.asMap("foo", "bar"))));
        compare(g.V().map(__.call("svc", GValue.ofMap("test", CollectionUtil.asMap("foo", "bar")), __.inject(CollectionUtil.asMap("a", "b")))), eval("g.V().map(__.call(\"svc\", test, __.inject([\"a\":\"b\"])))"));
    }

    private void compare(Object expected, Object actual) {
        assertEquals(((DefaultGraphTraversal) expected).asAdmin().getGremlinLang(),
                ((DefaultGraphTraversal) actual).asAdmin().getGremlinLang());
    }

    private Object eval(String query) {
        final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(query));
        final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
        return antlrToLanguage.visit(parser.queryList());
    }

    private GremlinAntlrToJava createAntlr(final VariableResolver resolver) {
        return new GremlinAntlrToJava("g", EmptyGraph.instance(), __::start, g, resolver);
    }
}
