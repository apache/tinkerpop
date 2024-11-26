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
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Before;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
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
    public void shouldParseAnonymousTraversal_addE()  {
        compare(g.V().map(__.addE("person")), eval("g.V().map(__.addE(\"person\"))"));
        compare(g.V().map(__.addE(GValue.of("foo", "bar"))), eval("g.V().map(__.addE(foo))"));
        compare(g.V().map(__.addE(__.hasLabel("person").label())), eval("g.V().map(__.addE(__.hasLabel(\"person\").label()))"));
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
