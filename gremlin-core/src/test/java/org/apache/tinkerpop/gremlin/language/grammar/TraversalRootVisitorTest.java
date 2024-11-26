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

import java.util.Collections;
import java.util.Map;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.CollectionUtil;
import org.junit.Before;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.as;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.constant;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
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
    public void shouldParseAnonymousTraversal_map() {
        compare(g.V().map(__.map(__.hasLabel("person").label())), eval("g.V().map(__.map(__.hasLabel(\"person\").label()))"));
        // TODO map with function
    }

    @Test
    public void shouldParseAnonymousTraversal_flatMap() {
        compare(g.V().map(__.flatMap(__.hasLabel("person").label())), eval("g.V().map(__.flatMap(__.hasLabel(\"person\").label()))"));
        // TODO flatMap with function
    }

    @Test
    public void shouldParseAnonymousTraversal_identity()  {
        compare(g.V().map(__.identity()), eval("g.V().map(__.identity())"));
    }

    @Test
    public void shouldParseAnonymousTraversal_constant()  {
        compare(g.V().map(__.constant("test")), eval("g.V().map(__.constant(\"test\"))"));
        compare(g.V().map(__.constant(GValue.of("foo", "bar"))), eval("g.V().map(__.constant(foo))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_label()  {
        compare(g.V().map(__.label()), eval("g.V().map(__.label())"));
    }

    @Test
    public void shouldParseAnonymousTraversal_id()  {
        compare(g.V().map(__.id()), eval("g.V().map(__.id())"));
    }

    @Test
    public void shouldParseAnonymousTraversal_V()  {
        compare(g.V().map(__.V()), eval("g.V().map(__.V())"));
    }

    @Test
    public void shouldParseAnonymousTraversal_E()  {
        compare(g.V().map(__.E()), eval("g.V().map(__.E())"));
    }

    @Test
    public void shouldParseAnonymousTraversal_to()  {
        compare(g.V().map(__.to(Direction.OUT)), eval("g.V().map(__.to(Direction.OUT))"));
        compare(g.V().map(__.to(Direction.OUT, "knows")), eval("g.V().map(__.to(Direction.OUT,\"knows\"))"));
        compare(g.V().map(__.to(Direction.OUT, GValue.ofString("foo", "bar"))), eval("g.V().map(__.to(Direction.OUT,foo))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_out()  {
        compare(g.V().map(__.out()), eval("g.V().map(__.out())"));
        compare(g.V().map(__.out("knows")), eval("g.V().map(__.out(\"knows\"))"));
        compare(g.V().map(__.out(GValue.ofString("foo", "bar"))), eval("g.V().map(__.out(foo))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_in()  {
        compare(g.V().map(__.in()), eval("g.V().map(__.in())"));
        compare(g.V().map(__.in("knows")), eval("g.V().map(__.in(\"knows\"))"));
        compare(g.V().map(__.in(GValue.ofString("foo", "bar"))), eval("g.V().map(__.in(foo))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_both()  {
        compare(g.V().map(__.both()), eval("g.V().map(__.both())"));
        compare(g.V().map(__.both("knows")), eval("g.V().map(__.both(\"knows\"))"));
        compare(g.V().map(__.both(GValue.ofString("foo", "bar"))), eval("g.V().map(__.both(foo))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_toE()  {
        compare(g.V().map(__.toE(Direction.OUT)), eval("g.V().map(__.toE(Direction.OUT))"));
        compare(g.V().map(__.toE(Direction.OUT, "knows")), eval("g.V().map(__.toE(Direction.OUT,\"knows\"))"));
        compare(g.V().map(__.toE(Direction.OUT, GValue.ofString("foo", "bar"))), eval("g.V().map(__.toE(Direction.OUT,foo))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_outE()  {
        compare(g.V().map(__.outE()), eval("g.V().map(__.outE())"));
        compare(g.V().map(__.outE("knows")), eval("g.V().map(__.outE(\"knows\"))"));
        compare(g.V().map(__.outE(GValue.ofString("foo", "bar"))), eval("g.V().map(__.outE(foo))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_inE()  {
        compare(g.V().map(__.inE()), eval("g.V().map(__.inE())"));
        compare(g.V().map(__.inE("knows")), eval("g.V().map(__.inE(\"knows\"))"));
        compare(g.V().map(__.inE(GValue.ofString("foo", "bar"))), eval("g.V().map(__.inE(foo))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_bothE()  {
        compare(g.V().map(__.bothE()), eval("g.V().map(__.bothE())"));
        compare(g.V().map(__.bothE("knows")), eval("g.V().map(__.bothE(\"knows\"))"));
        compare(g.V().map(__.bothE(GValue.ofString("foo", "bar"))), eval("g.V().map(__.bothE(foo))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_toV()  {
        compare(g.V().map(__.toV(Direction.OUT)), eval("g.V().map(__.toV(Direction.OUT)))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_inV()  {
        compare(g.V().map(__.inV()), eval("g.V().map(__.inV()))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_outV()  {
        compare(g.V().map(__.outV()), eval("g.V().map(__.outV()))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_bothV()  {
        compare(g.V().map(__.bothV()), eval("g.V().map(__.bothV()))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_otherV()  {
        compare(g.V().map(__.otherV()), eval("g.V().map(__.otherV()))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_order()  {
        compare(g.V().map(__.order()), eval("g.V().map(__.order()))"));
        compare(g.V().map(__.order(Scope.global)), eval("g.V().map(__.order(Scope.global)))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_properties()  {
        compare(g.V().map(__.properties("test")), eval("g.V().map(__.properties(\"test\"))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_values()  {
        compare(g.V().map(__.values("test")), eval("g.V().map(__.values(\"test\"))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_propertyMap()  {
        compare(g.V().map(__.propertyMap("test")), eval("g.V().map(__.propertyMap(\"test\"))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_elementMap()  {
        compare(g.V().map(__.elementMap("test")), eval("g.V().map(__.elementMap(\"test\"))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_valueMap()  {
        compare(g.V().map(__.valueMap("test")), eval("g.V().map(__.valueMap(\"test\"))"));
        compare(g.V().map(__.valueMap(true, "test")), eval("g.V().map(__.valueMap(true, \"test\"))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_project()  {
        compare(g.V().map(__.project("test1", "test2")), eval("g.V().map(__.project(\"test1\", \"test2\"))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_selectColumn()  {
        compare(g.V().map(__.select(Column.keys)), eval("g.V().map(__.select(Column.keys))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_selectKeyStrings()  {
        compare(g.V().map(__.select("test")), eval("g.V().map(__.select(\"test\"))"));
        compare(g.V().map(__.select("test1", "test2", "test3")), eval("g.V().map(__.select(\"test1\", \"test2\", \"test3\"))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_selectPop()  {
        compare(g.V().map(__.select(Pop.all, "test")), eval("g.V().map(__.select(Pop.all, \"test\"))"));
        compare(g.V().map(__.select(Pop.all, "test1", "test2", "test3")), eval("g.V().map(__.select(Pop.all, \"test1\", \"test2\", \"test3\"))"));
        compare(g.V().map(__.select(Pop.all, out().properties("a"))), eval("g.V().map(__.select(all, out().properties(\"a\")))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_selectTraversal()  {
        compare(g.V().map(__.select(out().properties("a"))), eval("g.V().map(__.select(out().properties(\"a\")))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_key()  {
        compare(g.V().map(__.key()), eval("g.V().map(__.key())"));
    }

    @Test
    public void shouldParseAnonymousTraversal_value()  {
        compare(g.V().map(__.value()), eval("g.V().map(__.value())"));
    }

    @Test
    public void shouldParseAnonymousTraversal_path()  {
        compare(g.V().map(__.path()), eval("g.V().map(__.path())"));
    }

    @Test
    public void shouldParseAnonymousTraversal_match() {
        compare(g.V().map(__.match(as("a"), as("b"))), eval("g.V().map(__.match(as(\"a\"), as(\"b\")))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_sack()  {
        compare(g.V().map(__.sack()), eval("g.V().map(__.sack())"));
    }

    @Test
    public void shouldParseAnonymousTraversal_loops()  {
        compare(g.V().map(__.loops()), eval("g.V().map(__.loops())"));
        compare(g.V().map(__.loops("test")), eval("g.V().map(__.loops(\"test\"))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_unfold()  {
        compare(g.V().map(__.unfold()), eval("g.V().map(__.unfold())"));
    }

    @Test
    public void shouldParseAnonymousTraversal_fold()  {
        compare(g.V().map(__.fold()), eval("g.V().map(__.fold())"));
        compare(g.V().values("age").map(__.fold(0, Operator.max)), eval("g.V().values('age').map(__.fold(0, max))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_count()  {
        compare(g.V().map(__.count()), eval("g.V().map(__.count())"));
        compare(g.V().map(__.count(Scope.global)), eval("g.V().map(__.count(Scope.global))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_sum()  {
        compare(g.V().map(__.sum()), eval("g.V().map(__.sum())"));
        compare(g.V().map(__.sum(Scope.global)), eval("g.V().map(__.sum(Scope.global))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_min()  {
        compare(g.V().map(__.min()), eval("g.V().map(__.min())"));
        compare(g.V().map(__.min(Scope.global)), eval("g.V().map(__.min(Scope.global))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_max()  {
        compare(g.V().map(__.max()), eval("g.V().map(__.max())"));
        compare(g.V().map(__.max(Scope.global)), eval("g.V().map(__.max(Scope.global))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_mean()  {
        compare(g.V().map(__.mean()), eval("g.V().map(__.mean())"));
        compare(g.V().map(__.mean(Scope.global)), eval("g.V().map(__.mean(Scope.global))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_group()  {
        compare(g.V().map(__.group()), eval("g.V().map(__.group())"));
    }

    @Test
    public void shouldParseAnonymousTraversal_groupCount()  {
        compare(g.V().map(__.groupCount()), eval("g.V().map(__.groupCount())"));
    }

    @Test
    public void shouldParseAnonymousTraversal_tree()  {
        compare(g.V().map(__.tree()), eval("g.V().map(__.tree())"));
    }

    @Test
    public void shouldParseAnonymousTraversal_addV()  {
        compare(g.V().map(__.addV("person")), eval("g.V().map(__.addV(\"person\"))"));
        compare(g.V().map(__.addV(GValue.of("foo", "bar"))), eval("g.V().map(__.addV(foo))"));
        compare(g.V().map(__.addV(__.hasLabel("person").label())), eval("g.V().map(__.addV(__.hasLabel(\"person\").label()))"));
        compare(g.V().map(__.addV()), eval("g.V().map(__.addV())"));
    }

    @Test
    public void shouldParseAnonymousTraversal_mergeV()  {
        antlrToLanguage = createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("foo", Collections.singletonMap("x", "y"))));
        compare(g.V().map(__.mergeV()), eval("g.V().map(__.mergeV())"));
        compare(g.V().map(__.mergeV(GValue.of("foo", Collections.singletonMap("x", "y")))), eval("g.V().map(__.mergeV(foo))"));
        compare(g.V().map(__.mergeV(__.hasLabel("person"))), eval("g.V().map(__.mergeV(__.hasLabel(\"person\")))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_addE()  {
        compare(g.V().map(__.addE("person")), eval("g.V().map(__.addE(\"person\"))"));
        compare(g.V().map(__.addE(GValue.of("foo", "bar"))), eval("g.V().map(__.addE(foo))"));
        compare(g.V().map(__.addE(__.hasLabel("person").label())), eval("g.V().map(__.addE(__.hasLabel(\"person\").label()))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_mergeE()  {
        antlrToLanguage = createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("foo", Collections.singletonMap("x", "y"))));
        compare(g.V().map(__.mergeE()), eval("g.V().map(__.mergeE())"));
        compare(g.V().map(__.mergeE(GValue.of("foo", Collections.singletonMap("x", "y")))), eval("g.V().map(__.mergeE(foo))"));
        compare(g.V().map(__.mergeE(__.hasLabel("person"))), eval("g.V().map(__.mergeE(__.hasLabel(\"person\")))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_math()  {
        compare(g.V().map(__.math("1+1")), eval("g.V().map(__.math(\"1+1\"))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_concat()  {
        compare(g.V().map(__.concat("test")), eval("g.V().map(__.concat(\"test\"))"));
        compare(g.V().map(__.concat(constant("hello"))), eval("g.V().map(__.concat(__.constant('hello')))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_asString()  {
        compare(g.V().map(__.asString()), eval("g.V().map(__.asString())"));
        compare(g.V().map(__.asString(Scope.global)), eval("g.V().map(__.asString(Scope.global))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_length()  {
        compare(g.V().map(__.length()), eval("g.V().map(__.length())"));
        compare(g.V().map(__.length(Scope.global)), eval("g.V().map(__.length(Scope.global))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_toLower()  {
        compare(g.V().map(__.toLower()), eval("g.V().map(__.toLower())"));
        compare(g.V().map(__.toLower(Scope.global)), eval("g.V().map(__.toLower(Scope.global))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_toUpper()  {
        compare(g.V().map(__.toUpper()), eval("g.V().map(__.toUpper())"));
        compare(g.V().map(__.toUpper(Scope.global)), eval("g.V().map(__.toUpper(Scope.global))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_trim()  {
        compare(g.V().map(__.trim()), eval("g.V().map(__.trim())"));
        compare(g.V().map(__.trim(Scope.global)), eval("g.V().map(__.trim(Scope.global))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_lTrim()  {
        compare(g.V().map(__.lTrim()), eval("g.V().map(__.lTrim())"));
        compare(g.V().map(__.lTrim(Scope.global)), eval("g.V().map(__.lTrim(Scope.global))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_rTrim()  {
        compare(g.V().map(__.rTrim()), eval("g.V().map(__.rTrim())"));
        compare(g.V().map(__.rTrim(Scope.global)), eval("g.V().map(__.rTrim(Scope.global))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_reverse()  {
        compare(g.V().map(__.reverse()), eval("g.V().map(__.reverse())"));
    }

    @Test
    public void shouldParseAnonymousTraversal_replace()  {
        compare(g.V().map(__.replace("hello", "world")), eval("g.V().map(__.replace(\"hello\", \"world\"))"));
        compare(g.V().map(__.replace(Scope.global, "hello", "world")), eval("g.V().map(__.replace(Scope.global, \"hello\", \"world\"))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_split()  {
        compare(g.V().map(__.split(",")), eval("g.V().map(__.split(\",\"))"));
        compare(g.V().map(__.split(Scope.global, ",")), eval("g.V().map(__.split(Scope.global, \",\"))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_substring()  {
        compare(g.V().map(__.substring(5)), eval("g.V().map(__.substring(5))"));
        compare(g.V().map(__.substring(Scope.global, 5)), eval("g.V().map(__.substring(Scope.global, 5))"));
        compare(g.V().map(__.substring(5, 9)), eval("g.V().map(__.substring(5, 9))"));
        compare(g.V().map(__.substring(Scope.global, 5, 9)), eval("g.V().map(__.substring(Scope.global, 5, 9))"));
    }

    @Test
    public void shouldParseAnonymousTraversal_format()  {
        compare(g.V().map(__.format("Hello %{name}")), eval("g.V().map(__.format(\"Hello %{name}\"))"));
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
