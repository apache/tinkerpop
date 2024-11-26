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
