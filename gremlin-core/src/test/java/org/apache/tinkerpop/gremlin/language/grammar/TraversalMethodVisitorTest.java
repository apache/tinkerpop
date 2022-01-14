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

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRank;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PeerPressure;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ShortestPath;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.apache.tinkerpop.gremlin.process.traversal.P.gt;
import static org.apache.tinkerpop.gremlin.process.traversal.SackFunctions.Barrier.normSack;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.global;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.apache.tinkerpop.gremlin.structure.T.label;
import static org.junit.Assert.assertEquals;

public class TraversalMethodVisitorTest {

    private final GraphTraversalSource g = traversal().withEmbedded(EmptyGraph.instance());
    private GremlinAntlrToJava antlrToLaunguage;

    private Object eval(String query) {
        final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(query));
        final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
        return antlrToLaunguage.visit(parser.queryList());
    }

    @Before
    public void setup() throws Exception {
        antlrToLaunguage = new GremlinAntlrToJava();
    }
    
    private void compare(Object expected, Object actual) {
        assertEquals(((DefaultGraphTraversal) expected).asAdmin().getBytecode(),
                ((DefaultGraphTraversal) actual).asAdmin().getBytecode());
    }

    @Test
    public void testChainedTraversal() throws Exception {
        // a random chain traversal.
        compare(g.V().addE("person"), eval("g.V().addE('person')"));
    }

    @Test
    public void testTraversalMethod_addE_String() throws Exception {
        // same with chained traversal but uses double quotes
        compare(g.V().addE("person"), eval("g.V().addE(\"person\")"));
    }

    @Test
    public void testTraversalMethod_addE_Traversal() throws Exception {
        // same with chained traversal but uses double quotes
        compare(g.V().addE(V().hasLabel("person").label()), eval("g.V().addE(V().hasLabel(\"person\").label())"));
    }

    @Test
    public void testTraversalMethod_addV_Empty() throws Exception {
        compare(g.V().addV(), eval("g.V().addV()"));
    }

    @Test
    public void testTraversalMethod_addV_String() throws Exception {
        compare(g.V().addV("test"), eval("g.V().addV(\"test\")"));
    }

    @Test
    public void testTraversalMethod_addV_Traversal() throws Exception {
        compare(g.addV(V().hasLabel("person").label()), eval("g.addV(V().hasLabel(\"person\").label())"));
    }

    @Test
    public void testTraversalMethod_aggregate() throws Exception {
        compare(g.V().aggregate("test"), eval("g.V().aggregate('test')"));
    }

    @Test
    public void testTraversalMethod_aggregate_Scope() throws Exception {
        compare(g.V().aggregate(global, "test"), eval("g.V().aggregate(global, 'test')"));
        compare(g.V().aggregate(Scope.local, "test"), eval("g.V().aggregate(Scope.local, 'test')"));
    }

    @Test
    public void testTraversalMethod_and() throws Exception {
        compare(g.V().and(outE("knows")), eval("g.V().and(outE('knows'))"));
    }

    @Test
    public void testTraversalMethod_as() throws Exception {
        compare(g.V().as("test"), eval("g.V().as('test')"));
    }

    @Test
    public void testTraversalMethod_barrier_Consumer() throws Exception {
        compare(g.V().barrier(normSack), eval("g.V().barrier(normSack)"));
    }

    @Test
    public void testTraversalMethod_barrier_Empty() throws Exception {
        compare(g.V().barrier(), eval("g.V().barrier()"));
    }

    @Test
    public void testTraversalMethod_barrier_int() throws Exception {
        compare(g.V().barrier(4), eval("g.V().barrier(4)"));
    }

    @Test
    public void testTraversalMethod_both_Empty() throws Exception {
        compare(g.V().both(), eval("g.V().both()"));
    }

    @Test
    public void testTraversalMethod_both_SingleString() throws Exception {
        compare(g.V().both("test"), eval("g.V().both('test')"));
    }

    @Test
    public void testTraversalMethod_both_MultiString() throws Exception {
        compare(g.V().both(new String[]{"a", "b"}), eval("g.V().both('a', 'b')"));
    }

    @Test
    public void testTraversalMethod_bothE() throws Exception {
        compare(g.V().bothE("test"), eval("g.V().bothE('test')"));
    }

    @Test
    public void testTraversalMethod_bothV() throws Exception {
        compare(g.V().bothV(), eval("g.V().bothV()"));
    }

    @Test
    public void testTraversalMethod_branch_Traversal() throws Exception {
        compare(g.V().branch(values("name")), eval("g.V().branch(values('name'))"));
    }

    @Test
    public void testTraversalMethod_by_Comparator() throws Exception {
        compare(g.V().order().by(Order.asc), eval("g.V().order().by(asc)"));
    }

    @Test
    public void testTraversalMethod_by_Empty() throws Exception {
        compare(g.V().cyclicPath().by(), eval("g.V().cyclicPath().by()"));
    }

    @Test
    public void testTraversalMethod_by_Function() throws Exception {
        compare(g.V().order().by(T.id), eval("g.V().order().by(id)"));
    }

    @Test
    public void testTraversalMethod_by_Function_Comparator() throws Exception {
        compare(g.V().order().by(Column.keys, Order.asc), eval("g.V().order().by(keys, asc)"));
    }

    @Test
    public void testTraversalMethod_by_Order() throws Exception {
        compare(g.V().order().by(Order.shuffle), eval("g.V().order().by(shuffle)"));
    }

    @Test
    public void testTraversalMethod_by_String() throws Exception {
        compare(g.V().order().by("name"), eval("g.V().order().by('name')"));
    }

    @Test
    public void testTraversalMethod_by_String_Comparator() throws Exception {
        compare(g.V().order().by("name", Order.asc), eval("g.V().order().by('name', asc)"));
    }

    @Test
    public void testTraversalMethod_by_T() throws Exception {
        compare(g.V().order().by(T.id), eval("g.V().order().by(id)"));
    }

    @Test
    public void testTraversalMethod_by_Traversal() throws Exception {
        compare(g.V().group().by(bothE().count()), eval("g.V().group().by(bothE().count())"));
    }

    @Test
    public void testTraversalMethod_by_Traversal_Comparator() throws Exception {
        compare(g.V().order().by(bothE().count(), Order.asc), eval("g.V().order().by(bothE().count(), asc)"));
    }

    @Test
    public void testTraversalMethod_cap() throws Exception {
        compare(g.V().cap("test"), eval("g.V().cap('test')"));
    }

    @Test
    public void testTraversalMethod_choose_Function() throws Exception {
        compare(g.V().choose((Function) label), eval("g.V().choose(label)"));
    }

    @Test
    public void testTraversalMethod_choose_Predicate_Traversal() throws Exception {
        compare(g.V().choose(is(12), values("age")), eval("g.V().choose(is(12), values(\"age\"))"));
    }

    @Test
    public void testTraversalMethod_choose_Predicate_Traversal_Traversal() throws Exception {
        compare(g.V().choose(is(12), values("age"), values("count")),
                eval("g.V().choose(is(12), values(\"age\"), values(\"count\"))"));
    }

    @Test
    public void testTraversalMethod_choose_Traversal() throws Exception {
        compare(g.V().choose(values("age")), eval("g.V().choose(values('age'))"));
    }

    @Test
    public void testTraversalMethod_choose_Traversal_Traversal() throws Exception {
        compare(g.V().choose(values("age"), bothE()), eval("g.V().choose(values('age'), bothE())"));
    }

    @Test
    public void testTraversalMethod_choose_Traversal_Traversal_Traversal() throws Exception {
        compare(g.V().choose(values("age"), bothE(), bothE()), eval("g.V().choose(values('age'), bothE(), bothE())"));
    }

    @Test
    public void testTraversalMethod_coalesce() throws Exception {
        compare(g.V().coalesce(outE("knows")), eval("g.V().coalesce(outE('knows'))"));
    }

    @Test
    public void testTraversalMethod_coin() throws Exception {
        compare(g.V().coin(2.5), eval("g.V().coin(2.5)"));
    }

    @Test
    public void testTraversalMethod_constant() throws Exception {
        compare(g.V().constant("yigit"), eval("g.V().constant('yigit')"));
    }

    @Test
    public void testTraversalMethod_count_Empty() throws Exception {
        compare(g.V().count(), eval("g.V().count()"));
    }

    @Test
    public void testTraversalMethod_count_Scope() throws Exception {
        compare(g.V().count(global), eval("g.V().count(global)"));
    }

    @Test
    public void testTraversalMethod_cyclicPath() throws Exception {
        compare(g.V().cyclicPath(), eval("g.V().cyclicPath()"));
    }

    @Test
    public void testTraversalMethod_dedup_Scope_String() throws Exception {
        compare(g.V().dedup(Scope.local, "age"), eval("g.V().dedup(local, 'age')"));
    }

    @Test
    public void testTraversalMethod_dedup_String() throws Exception {
        compare(g.V().dedup(), eval("g.V().dedup()"));
    }

    @Test
    public void testTraversalMethod_drop() throws Exception {
        compare(g.V().drop(), eval("g.V().drop()"));
    }

    @Test
    public void testTraversalMethod_emit_Empty() throws Exception {
        compare(g.V().emit(), eval("g.V().emit()"));
    }

    @Test
    public void testTraversalMethod_emit_Predicate() throws Exception {
        compare(g.V().repeat(out()).emit(is("asd")), eval("g.V().repeat(out()).emit(is(\"asd\"))"));
    }

    @Test
    public void testTraversalMethod_emit_Traversal() throws Exception {
        compare(g.V().emit(has("name")), eval("g.V().emit(has('name'))"));
    }

    @Test
    public void testTraversalMethod_filter_Predicate() throws Exception {
        compare(g.V().repeat(out()).filter(is("2")), eval("g.V().repeat(out()).filter(is(\"2\"))"));
    }

    @Test
    public void testTraversalMethod_filter_Traversal() throws Exception {
        compare(g.V().filter(has("name")), eval("g.V().filter(has('name'))"));
    }

    @Test
    public void testTraversalMethod_flatMap_Traversal() throws Exception {
        compare(g.V().flatMap(has("name")), eval("g.V().flatMap(has('name'))"));
    }

    @Test
    public void testTraversalMethod_fold_Empty() throws Exception {
        compare(g.V().fold(), eval("g.V().fold()"));
    }

    @Test
    public void testTraversalMethod_fold_Object_BiFunction() throws Exception {
        compare(g.V().values("age").fold(0, Operator.max), eval("g.V().values('age').fold(0, max)"));
    }

    @Test
    public void testTraversalMethod_from_String() throws Exception {
        compare(g.V().cyclicPath().from("name"), eval("g.V().cyclicPath().from('name')"));
    }

    @Test
    public void testTraversalMethod_from_Traversal() throws Exception {
        compare(g.V().addE("as").from(V()), eval("g.V().addE('as').from(V())"));
    }

    @Test
    public void testTraversalMethod_group_Empty() throws Exception {
        compare(g.V().group(), eval("g.V().group()"));
    }

    @Test
    public void testTraversalMethod_group_String() throws Exception {
        compare(g.V().group("age"), eval("g.V().group('age')"));
    }

    @Test
    public void testTraversalMethod_groupCount_Empty() throws Exception {
        compare(g.V().groupCount(), eval("g.V().groupCount()"));
    }

    @Test
    public void testTraversalMethod_groupCount_String() throws Exception {
        compare(g.V().groupCount("age"), eval("g.V().groupCount('age')"));
    }

    @Test
    public void testTraversalMethod_has_String() throws Exception {
        compare(g.V().has("age"), eval("g.V().has('age')"));
    }

    @Test
    public void testTraversalMethod_has_String_Object() throws Exception {
        compare(g.V().has("age", 132), eval("g.V().has('age', 132)"));
    }

    @Test
    public void testTraversalMethod_has_String_P() throws Exception {
        compare(g.V().has("a", eq("b")), eval("g.V().has(\"a\", eq(\"b\"))"));
    }

    @Test
    public void testTraversalMethod_has_String_String_Object() throws Exception {
        compare(g.V().has("a", "b", 3), eval("g.V().has(\"a\", \"b\", 3)"));
    }

    @Test
    public void testTraversalMethod_has_String_String_P() throws Exception {
        compare(g.V().has("a", "b", eq("c")), eval("g.V().has(\"a\", \"b\", eq(\"c\"))"));
    }

    @Test
    public void testTraversalMethod_has_String_Traversal() throws Exception {
        compare(g.V().has("age", bothE()), eval("g.V().has('age', bothE())"));
    }

    @Test
    public void testTraversalMethod_has_T_Object() throws Exception {
        compare(g.V().has(T.id, 6), eval("g.V().has(id, 6)"));
    }

    @Test
    public void testTraversalMethod_has_T_P() throws Exception {
        compare(g.V().has(T.id, eq("asd")), eval("g.V().has(id, eq('asd'))"));
    }

    @Test
    public void testTraversalMethod_has_T_Traversal() throws Exception {
        compare(g.V().has(T.id, bothE()), eval("g.V().has(id, bothE())"));
    }

    @Test
    public void testTraversalMethod_hasId_Object_Object() throws Exception {
        compare(g.V().hasId(3, 4), eval("g.V().hasId(3, 4)"));
    }

    @Test
    public void testTraversalMethod_hasId_P() throws Exception {
        compare(g.V().hasId(gt(4)), eval("g.V().hasId(gt(4))"));
    }

    @Test
    public void testTraversalMethod_hasKey_P() throws Exception {
        compare(g.V().hasKey(eq("asd")), eval("g.V().hasKey(eq(\"asd\"))"));
    }

    @Test
    public void testTraversalMethod_hasKey_String_String() throws Exception {
        compare(g.V().hasKey("age"), eval("g.V().hasKey('age')"));
        compare(g.V().hasKey("age", "3"), eval("g.V().hasKey('age', '3')"));
    }

    @Test
    public void testTraversalMethod_hasLabel_P() throws Exception {
        compare(g.V().hasLabel(eq("asd")), eval("g.V().hasLabel(eq(\"asd\"))"));
    }

    @Test
    public void testTraversalMethod_hasLabel_String_String() throws Exception {
        compare(g.V().hasLabel("age"), eval("g.V().hasLabel('age')"));
        compare(g.V().hasLabel("age", "3"), eval("g.V().hasLabel('age', '3')"));
    }

    @Test
    public void testTraversalMethod_hasNot() throws Exception {
        compare(g.V().hasNot("know"), eval("g.V().hasNot('know')"));
    }

    @Test
    public void testTraversalMethod_hasValue_Object_Object() throws Exception {
        compare(g.V().hasValue(3, 4), eval("g.V().hasValue(3, 4)"));
    }

    @Test
    public void testTraversalMethod_hasValue_P() throws Exception {
        compare(g.V().hasValue(eq(2)), eval("g.V().hasValue(eq(2))"));
    }

    @Test
    public void testTraversalMethod_id() throws Exception {
        compare(g.V().id(), eval("g.V().id()"));
    }

    @Test
    public void testTraversalMethod_identity() throws Exception {
        compare(g.V().identity(), eval("g.V().identity()"));
    }

    @Test
    public void testTraversalMethod_in() throws Exception {
        compare(g.V().in("created"), eval("g.V().in('created')"));
    }

    @Test
    public void testTraversalMethod_index() throws Exception {
        compare(g.V().hasLabel("software").index(), eval("g.V().hasLabel('software').index()"));
    }

    @Test
    public void testTraversalMethod_inE() throws Exception {
        compare(g.V().inE("created"), eval("g.V().inE('created')"));
    }

    @Test
    public void testTraversalMethod_inV() throws Exception {
        compare(g.V().inV(), eval("g.V().inV()"));
    }

    @Test
    public void testTraversalMethod_inject() throws Exception {
        compare(g.V(4).out().values("name").inject("daniel"),
                eval("g.V(4).out().values(\"name\").inject(\"daniel\")"));
    }

    @Test
    public void testTraversalMethod_is_Object() throws Exception {
        compare(g.V().is(4), eval("g.V().is(4)"));
    }

    @Test
    public void testTraversalMethod_is_P() throws Exception {
        compare(g.V().is(gt(4)), eval("g.V().is(gt(4))"));
    }

    @Test
    public void testTraversalMethod_iterate() throws Exception {
        compare(g.V().iterate(), eval("g.V().iterate()"));
    }

    @Test
    public void testTraversalMethod_key() throws Exception {
        compare(g.V().key(), eval("g.V().key()"));
    }

    @Test
    public void testTraversalMethod_label() throws Exception {
        compare(g.V().label(), eval("g.V().label()"));
    }

    @Test
    public void testTraversalMethod_limit_Scope_long() throws Exception {
        compare(g.V().limit(global, 3), eval("g.V().limit(global, 3)"));
    }

    @Test
    public void testTraversalMethod_limit_long() throws Exception {
        compare(g.V().limit(2), eval("g.V().limit(2)"));
    }

    @Test
    public void testTraversalMethod_local() throws Exception {
        compare(g.V().local(bothE()), eval("g.V().local(bothE())"));
    }

    @Test
    public void testTraversalMethod_loops() throws Exception {
        compare(g.V().loops(), eval("g.V().loops()"));
    }

    @Test
    public void testTraversalMethod_map_Traversal() throws Exception {
        compare(g.V().map(bothE()), eval("g.V().map(bothE())"));
    }

    @Test
    public void testTraversalMethod_match() throws Exception {
        compare(g.V().match(as("a"), as("b")), eval("g.V().match(as(\"a\"), as(\"b\"))"));
    }

    @Test
    public void testTraversalMethod_max_Empty() throws Exception {
        compare(g.V().max(), eval("g.V().max()"));
    }

    @Test
    public void testTraversalMethod_max_Scope() throws Exception {
        compare(g.V().max(Scope.local), eval("g.V().max(local)"));
    }

    @Test
    public void testTraversalMethod_math() throws Exception {
        compare(g.V().count().math("_ + 10"), eval("g.V().count().math('_ + 10')"));
    }

    @Test
    public void testTraversalMethod_mean_Empty() throws Exception {
        compare(g.V().mean(), eval("g.V().mean()"));
    }

    @Test
    public void testTraversalMethod_mean_Scope() throws Exception {
        compare(g.V().mean(global), eval("g.V().mean(global)"));
    }

    @Test
    public void testTraversalMethod_min_Empty() throws Exception {
        compare(g.V().min(), eval("g.V().min()"));
    }

    @Test
    public void testTraversalMethod_min_Scope() throws Exception {
        compare(g.V().min(Scope.local), eval("g.V().min(local)"));
    }

    @Test
    public void testTraversalMethod_not() throws Exception {
        compare(g.V().not(both()), eval("g.V().not(both())"));
    }

    @Test
    public void testTraversalMethod_option_Object_Traversal() throws Exception {
        compare(g.V().branch(values("name")).option(2, bothE()),
                eval("g.V().branch(values(\"name\")).option(2, bothE())"));
    }

    @Test
    public void testTraversalMethod_option_Traversal() throws Exception {
        compare(g.V().branch(values("name")).option(both()), eval("g.V().branch(values(\"name\")).option(both())"));
    }

    @Test
    public void testTraversalMethod_optional() throws Exception {
        compare(g.V().optional(min()), eval("g.V().optional(min())"));
    }

    @Test
    public void testTraversalMethod_or() throws Exception {
        compare(g.V().or(as("a"), as("b")), eval("g.V().or(as(\"a\"), as(\"b\"))"));
    }

    @Test
    public void testTraversalMethod_order_Empty() throws Exception {
        compare(g.V().order(), eval("g.V().order()"));
    }

    @Test
    public void testTraversalMethod_order_Scope() throws Exception {
        compare(g.V().order(global), eval("g.V().order(global)"));
    }

    @Test
    public void testTraversalMethod_otherV() throws Exception {
        compare(g.V().otherV(), eval("g.V().otherV()"));
    }

    @Test
    public void testTraversalMethod_out() throws Exception {
        compare(g.V().out("a", "b"), eval("g.V().out(\"a\", \"b\")"));
    }

    @Test
    public void testTraversalMethod_outE() throws Exception {
        compare(g.V().outE("a", "b"), eval("g.V().outE(\"a\", \"b\")"));
    }

    @Test
    public void testTraversalMethod_outV() throws Exception {
        compare(g.V().outV(), eval("g.V().outV()"));
    }

    @Test
    public void testTraversalMethod_pageRank_Empty() throws Exception {
        compare(g.V().pageRank(), eval("g.V().pageRank()"));
    }

    @Test
    public void testTraversalMethod_pageRank_double() throws Exception {
        compare(g.V().pageRank(2.6), eval("g.V().pageRank(2.6)"));
    }

    @Test
    public void testTraversalMethod_path() throws Exception {
        compare(g.V().path(), eval("g.V().path()"));
    }

    @Test
    public void testTraversalMethod_peerPressure() throws Exception {
        compare(g.V().peerPressure(), eval("g.V().peerPressure()"));
    }

    @Test
    public void testTraversalMethod_profile_Empty() throws Exception {
        compare(g.V().profile(), eval("g.V().profile()"));
    }

    @Test
    public void testTraversalMethod_profile_String() throws Exception {
        compare(g.V().profile("neptune"), eval("g.V().profile('neptune')"));
    }

    @Test
    public void testTraversalMethod_project() throws Exception {
        compare(g.V().project("neptune"), eval("g.V().project('neptune')"));
        compare(g.V().project("neptune", "uranus"), eval("g.V().project('neptune', 'uranus')"));
    }

    @Test
    public void testTraversalMethod_properties() throws Exception {
        compare(g.V().properties("venus", "mars"), eval("g.V().properties('venus', 'mars')"));
    }

    @Test
    public void testTraversalMethod_property_Cardinality_Object_Object_Object() throws Exception {
        compare(g.V().property(VertexProperty.Cardinality.list,1,2,"key", 4),
                eval("g.V().property(list, 1,2,'key',4)"));
    }

    @Test
    public void testTraversalMethod_property_Object_Object_Object() throws Exception {
        compare(g.V().property(1,2,"key", 4), eval("g.V().property(1,2,'key',4)"));
    }

    @Test
    public void testTraversalMethod_property_Object() throws Exception {
        final LinkedHashMap<Object, Object> map = new LinkedHashMap<>();
        map.put("key", "foo");
        map.put("key1", "bar");
        compare(g.V().property(map), eval("g.V().property(['key': 'foo', 'key1': 'bar'])"));
        map.clear();
        map.put("name", "foo");
        map.put("age", 42);
        compare(g.addV().property(map), eval("g.addV().property([\"name\": \"foo\", \"age\": 42 ])"));
        map.clear();
        map.put(label, "foo");
        map.put("age", 42);
        compare(g.addV().property(map), eval("g.addV().property([T.label: \"foo\", \"age\": 42 ])"));
    }

    @Test
    public void testTraversalMethod_property_Cardinality_Object() throws Exception {
        final LinkedHashMap<Object, Object> map = new LinkedHashMap<>();
        map.put("key", "foo");
        map.put("key1", "bar");
        compare(g.V().property(Cardinality.list, map), eval("g.V().property(list, ['key': 'foo', 'key1': 'bar'])"));
    }

    @Test
    public void testTraversalMethod_propertyMap() throws Exception {
        compare(g.V().propertyMap("venus", "mars"), eval("g.V().propertyMap('venus', 'mars')"));
    }

    @Test
    public void testTraversalMethod_range_Scope_long_long() throws Exception {
        compare(g.V().range(global, 3,5), eval("g.V().range(global, 3,5)"));
    }

    @Test
    public void testTraversalMethod_range_long_long() throws Exception {
        compare(g.V().range(3,5), eval("g.V().range(3,5)"));
    }

    @Test
    public void testTraversalMethod_repeat() throws Exception {
        compare(g.V().repeat(both()), eval("g.V().repeat(both())"));
    }

    @Test
    public void testTraversalMethod_sack_BiFunction() throws Exception {
        compare(g.V().sack(), eval("g.V().sack()"));
        compare(g.V().sack(Operator.addAll), eval("g.V().sack(addAll)"));
        compare(g.V().sack(Operator.and), eval("g.V().sack(and)"));
        compare(g.V().sack(Operator.assign), eval("g.V().sack(assign)"));
        compare(g.V().sack(Operator.div), eval("g.V().sack(div)"));
        compare(g.V().sack(Operator.max), eval("g.V().sack(max)"));
        compare(g.V().sack(Operator.min), eval("g.V().sack(min)"));
        compare(g.V().sack(Operator.minus), eval("g.V().sack(minus)"));
        compare(g.V().sack(Operator.mult), eval("g.V().sack(mult)"));
        compare(g.V().sack(Operator.or), eval("g.V().sack(or)"));
        compare(g.V().sack(Operator.sum), eval("g.V().sack(sum)"));
        compare(g.V().sack(Operator.sumLong), eval("g.V().sack(sumLong)"));
    }

    @Test
    public void testTraversalMethod_sack_Empty() throws Exception {
        compare(g.V().sack(), eval("g.V().sack()"));
    }

    @Test
    public void testTraversalMethod_sample_Scope_int() throws Exception {
        compare(g.V().sample(global, 2), eval("g.V().sample(global, 2)"));
    }

    @Test
    public void testTraversalMethod_sample_int() throws Exception {
        compare(g.V().sample(4), eval("g.V().sample(4)"));
    }

    @Test
    public void testTraversalMethod_select_Column() throws Exception {
        compare(g.V().select(Column.keys), eval("g.V().select(keys)"));
    }

    @Test
    public void testTraversalMethod_select_Pop_String() throws Exception {
        compare(g.V().select(Pop.first, "asd"), eval("g.V().select(first, 'asd')"));
    }

    @Test
    public void testTraversalMethod_select_Pop_String_String_String() throws Exception {
        compare(g.V().select(Pop.all, "a", "b", "c", "d"), eval("g.V().select(all, \"a\", \"b\", \"c\", \"d\")"));
    }

    @Test
    public void testTraversalMethod_select_Pop_Traversal() throws Exception {
        compare(g.V().select(Pop.all, out().properties("a")), eval("g.V().select(all, out().properties(\"a\"))"));
    }

    @Test
    public void testTraversalMethod_select_String() throws Exception {
        compare(g.V().select("yigit"), eval("g.V().select(\"yigit\")"));
    }

    @Test
    public void testTraversalMethod_select_String_String_String() throws Exception {
        compare(g.V().select("a", "b", "c", "d"), eval("g.V().select(\"a\", \"b\", \"c\", \"d\")"));
    }

    @Test
    public void testTraversalMethod_select_Traversal() throws Exception {
        compare(g.V().select(out().properties("a")), eval("g.V().select(out().properties(\"a\"))"));
    }

    @Test
    public void testTraversalMethod_sideEffect() throws Exception {
        compare(g.V().sideEffect(bothE()), eval("g.V().sideEffect(bothE())"));
    }

    @Test
    public void testTraversalMethod_simplePath() throws Exception {
        compare(g.V().simplePath(), eval("g.V().simplePath()"));
    }

    @Test
    public void testTraversalMethod_skip_Scope_long() throws Exception {
        compare(g.V().skip(global, 8), eval("g.V().skip(global, 8)"));
    }

    @Test
    public void testTraversalMethod_skip_long() throws Exception {
        compare(g.V().skip(8), eval("g.V().skip(8)"));
    }

    @Test
    public void testTraversalMethod_store() throws Exception {
        compare(g.V().store("asd"), eval("g.V().store(\"asd\")"));
    }

    @Test
    public void testTraversalMethod_subgraph() throws Exception {
        compare(g.V().subgraph("asd"), eval("g.V().subgraph('asd')"));
    }

    @Test
    public void testTraversalMethod_sum_Empty() throws Exception {
        compare(g.V().sum(), eval("g.V().sum()"));
    }

    @Test
    public void testTraversalMethod_sum_Scope() throws Exception {
        compare(g.V().sum(Scope.local), eval("g.V().sum(local)"));
    }

    @Test
    public void testTraversalMethod_tail_Empty() throws Exception {
        compare(g.V().tail(), eval("g.V().tail()"));
    }

    @Test
    public void testTraversalMethod_tail_Scope() throws Exception {
        compare(g.V().tail(Scope.local), eval("g.V().tail(local)"));
    }

    @Test
    public void testTraversalMethod_tail_Scope_long() throws Exception {
        compare(g.V().tail(Scope.local, 3), eval("g.V().tail(local, 3)"));
    }

    @Test
    public void testTraversalMethod_tail_long() throws Exception {
        compare(g.V().tail(4), eval("g.V().tail(4)"));
    }

    @Test
    public void testTraversalMethod_timeLimit() throws Exception {
        compare(g.V().timeLimit(5), eval("g.V().timeLimit(5)"));
    }

    @Test
    public void testTraversalMethod_times() throws Exception {
        compare(g.V().times(6), eval("g.V().times(6)"));
    }

    @Test
    public void testTraversalMethod_to_Direction_String() throws Exception {
        compare(g.V().to(Direction.IN, "asd"), eval("g.V().to(IN, 'asd')"));
    }

    @Test
    public void testTraversalMethod_to_String() throws Exception {
        compare(g.V().path().to("home"), eval("g.V().path().to(\"home\")"));
    }

    @Test
    public void testTraversalMethod_to_Traversal() throws Exception {
        compare(g.V().addE("as").to(V()), eval("g.V().addE('as').to(V())"));
    }

    @Test
    public void testTraversalMethod_toE() throws Exception {
        compare(g.V().toE(Direction.IN, "asd"), eval("g.V().toE(IN, 'asd')"));
    }

    @Test
    public void testTraversalMethod_toV() throws Exception {
        compare(g.V().toV(Direction.IN), eval("g.V().toV(IN)"));
    }

    @Test
    public void testTraversalMethod_tree_Empty() throws Exception {
        compare(g.V().tree(), eval("g.V().tree()"));
    }

    @Test
    public void testTraversalMethod_tree_String() throws Exception {
        compare(g.V().tree("hello"), eval("g.V().tree(\"hello\")"));
    }

    @Test
    public void testTraversalMethod_unfold() throws Exception {
        compare(g.V().unfold(), eval("g.V().unfold()"));
    }

    @Test
    public void testTraversalMethod_union() throws Exception {
        compare(g.V().union(in(), out()), eval("g.V().union(in(), out())"));
    }

    @Test
    public void testTraversalMethod_until_Predicate() throws Exception {
        compare(g.V().until(is("123")), eval("g.V().until(is(\"123\"))"));
    }

    @Test
    public void testTraversalMethod_until_Traversal() throws Exception {
        compare(g.V().until(has("ripple")), eval("g.V().until(has(\"ripple\"))"));
    }

    @Test
    public void testTraversalMethod_value() throws Exception {
        compare(g.V().value(), eval("g.V().value()"));
    }

    @Test
    public void testTraversalMethod_valueMap_String() throws Exception {
        compare(g.V().valueMap("yigit"), eval("g.V().valueMap(\"yigit\")"));
    }

    @Test
    public void testTraversalMethod_valueMap_boolean_String1() throws Exception {
        compare(g.V().valueMap(true), eval("g.V().valueMap(true)"));
    }

    @Test
    public void testTraversalMethod_valueMap_boolean_String2() throws Exception {
        compare(g.V().valueMap(true, "that"), eval("g.V().valueMap(true, \"that\")"));
    }

    @Test
    public void testTraversalMethod_valueMap_withOption() throws Exception {
        compare(g.V().valueMap().with(WithOptions.tokens, WithOptions.labels),
                eval("g.V().valueMap().with(WithOptions.tokens, WithOptions.labels)"));
    }

    @Test
    public void testTraversalMethod_values() throws Exception {
        compare(g.V().values("earth", "mars"), eval("g.V().values(\"earth\", \"mars\")"));
    }

    @Test
    public void testTraversalMethod_where_P() throws Exception {
        compare(g.V().where(eq("123")), eval("g.V().where(eq(\"123\"))"));
    }

    @Test
    public void testTraversalMethod_where_String_P() throws Exception {
        compare(g.V().where("age", eq("123")), eval("g.V().where('age', eq(\"123\"))"));
    }

    @Test
    public void testTraversalMethod_where_Traversal() throws Exception {
        compare(g.V().where(both()), eval("g.V().where(both())"));
    }

    @Test
    public void visitTraversalMethod_with_String() throws Exception {
        compare(g.V().valueMap().with("hakuna"), eval("g.V().valueMap().with('hakuna')"));
    }

    @Test
    public void visitTraversalMethod_with_String_Object() throws Exception {
        compare(g.V().index().with(WithOptions.indexer, WithOptions.map),
                eval("g.V().index().with(WithOptions.indexer, WithOptions.map)"));
    }

    @Test
    public void visitTraversalMethod_withOptionsTokensAll() throws Exception {
        compare(g.V().has("code","AUS").valueMap().with(WithOptions.tokens,WithOptions.all).unfold(),
                eval("g.V().has('code','AUS').valueMap().with(WithOptions.tokens,WithOptions.all).unfold()"));
    }

    @Test
    public void visitTraversalMethod_withOptionsTokensNone() throws Exception {
        compare(g.V().has("code","AUS").valueMap().with(WithOptions.tokens,WithOptions.none),
                eval("g.V().has('code','AUS').valueMap().with(WithOptions.tokens,WithOptions.none)"));
    }

    @Test
    public void visitTraversalMethod_withOptionsTokensIds() throws Exception {
        compare(g.V().has("code","AUS").valueMap().with(WithOptions.tokens,WithOptions.ids),
                eval("g.V().has('code','AUS').valueMap().with(WithOptions.tokens,WithOptions.ids)"));
    }

    @Test
    public void visitTraversalMethod_withOptionsTokensLabels() throws Exception {
        compare(g.V().has("code","AUS").valueMap().with(WithOptions.tokens,WithOptions.labels),
                eval("g.V().has('code','AUS').valueMap().with(WithOptions.tokens,WithOptions.labels)"));
    }

    @Test
    public void visitTraversalMethod_withOptionsTokensKeys() throws Exception {
        compare(g.V().has("code","AUS").valueMap().with(WithOptions.tokens,WithOptions.keys),
                eval("g.V().has('code','AUS').valueMap().with(WithOptions.tokens,WithOptions.keys)"));
    }

    @Test
    public void visitTraversalMethod_withOptionsTokensValues() throws Exception {
        compare(g.V().has("code","AUS").valueMap().with(WithOptions.tokens,WithOptions.values),
                eval("g.V().has('code','AUS').valueMap().with(WithOptions.tokens,WithOptions.values)"));
    }

    @Test
    public void visitTraversalMethod_withOptionsIndexerList() throws Exception {
        compare(g.V().has("code","AUS").valueMap().with(WithOptions.indexer,WithOptions.list),
                eval("g.V().has('code','AUS').valueMap().with(WithOptions.indexer,WithOptions.list)"));
    }
    @Test
    public void visitTraversalMethod_withOptionsIndexerMap() throws Exception {
        compare(g.V().has("code","AUS").valueMap().with(WithOptions.indexer,WithOptions.map),
                eval("g.V().has('code','AUS').valueMap().with(WithOptions.indexer,WithOptions.map)"));
    }

    @Test
    public void testTraversalMethod_peerPressure_withPropertyName() throws Exception {
        compare(g.V().peerPressure().with(PeerPressure.propertyName, "cluster"),
                eval("g.V().peerPressure().with(PeerPressure.propertyName, 'cluster')"));
    }

    @Test
    public void testTraversalMethod_peerPressure_withEdges() throws Exception {
        compare(g.V().peerPressure().with(PeerPressure.edges, outE("knows")),
                eval("g.V().peerPressure().with(PeerPressure.edges, __.outE('knows'))"));
    }

    @Test
    public void testTraversalMethod_peerPressure_withTimes() throws Exception {
        compare(g.V().peerPressure().with(PeerPressure.times, 2),
                eval("g.V().peerPressure().with(PeerPressure.times, 2)"));
    }

    @Test
    public void testTraversalMethod_pageRank_withOutEdges() throws Exception {
        compare(g.V().pageRank(2.6).with(PageRank.edges, outE("knows")),
                eval("g.V().pageRank(2.6).with(PageRank.edges, __.outE('knows'))"));
    }

    @Test
    public void testTraversalMethod_pageRank_withTimes() throws Exception {
        compare(g.V().pageRank(2.6).with(PageRank.times, 2),
                eval("g.V().pageRank(2.6).with(PageRank.times, 2)"));
    }

    @Test
    public void testTraversalMethod_pageRank_withPropertyName() throws Exception {
        compare(g.V().pageRank(2.6).with(PageRank.propertyName, "blah"),
                eval("g.V().pageRank(2.6).with(PageRank.propertyName, 'blah')"));
    }

    @Test
    public void testTraversalMethod_shortestPath_withEdges() throws Exception {
        compare(g.V().shortestPath().with(ShortestPath.edges, outE("knows")),
                eval("g.V().shortestPath().with(ShortestPath.edges, __.outE('knows'))"));
    }

    @Test
    public void testTraversalMethod_shortestPath_withIncludeEdges() throws Exception {
        compare(g.V().shortestPath().with(ShortestPath.includeEdges, true),
                eval("g.V().shortestPath().with(ShortestPath.includeEdges, true)"));
    }

    @Test
    public void testTraversalMethod_shortestPath_withDistance() throws Exception {
        compare(g.V().shortestPath().with(ShortestPath.distance, "asd"),
                eval("g.V().shortestPath().with(ShortestPath.distance, 'asd')"));
    }

    @Test
    public void testTraversalMethod_shortestPath_withMaxDistance() throws Exception {
        compare(g.V().shortestPath().with(ShortestPath.maxDistance, 2),
                eval("g.V().shortestPath().with(ShortestPath.maxDistance, 2)"));
    }

    @Test
    public void testTraversalMethod_shortestPath_withTarget() throws Exception {
        compare(g.V().shortestPath().with(ShortestPath.target, has("name", "peter")),
                eval("g.V().shortestPath().with(ShortestPath.target, __.has('name','peter'))"));
    }

    @Test
    public void testTraversalMethod_shortestPath_withEdgesWithTarget() throws Exception {
        compare(g.V().shortestPath().with(ShortestPath.edges, Direction.IN).with(ShortestPath.target, has("name", "josh")),
                eval("g.V().shortestPath().\n" +
                        "                 with(ShortestPath.edges, IN).\n" +
                        "                 with(ShortestPath.target, __.has('name','josh'))"));
    }

    @Test
    public void testTraversalMethod_with() throws Exception {
        compare(g.V().with("blah"),
                eval("g.V().with('blah')"));
    }

    @Test
    public void testTraversalMethod_with_multipleArgs() throws Exception {
        compare(g.V().with("blah", "bleh"),
                eval("g.V().with('blah', 'bleh')"));
    }
}
