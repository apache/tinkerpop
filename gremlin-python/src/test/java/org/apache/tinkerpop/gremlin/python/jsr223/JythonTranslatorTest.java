/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.python.jsr223;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.TranslationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class JythonTranslatorTest {

    @Test
    public void shouldSupportStringSupplierLambdas() throws Exception {
        GraphTraversalSource g = TinkerFactory.createModern().traversal();
        g = g.withStrategies(new TranslationStrategy(g, JythonTranslator.of("g")));
        final GraphTraversal.Admin<Vertex, Integer> t = g.withSideEffect("lengthSum", 0).withSack(1)
                .V()
                .filter(Lambda.predicate("x : x.get().label() == 'person'"))
                .flatMap(Lambda.function("lambda x : x.get().vertices(Direction.OUT)"))
                .map(Lambda.<Traverser<Object>, Integer>function("lambda x : len(x.get().value('name'))"))
                .sideEffect(Lambda.consumer(" x : x.sideEffects(\"lengthSum\", x.sideEffects('lengthSum') + x.get())    "))
                .order().by(Lambda.comparator("  lambda a,b : 0 if a == b else 1 if a > b else -1"))
                .sack(Lambda.biFunction("lambda a,b : a + b"))
                .asAdmin();
        final List<Integer> sacks = new ArrayList<>();
        final List<Integer> lengths = new ArrayList<>();
        while (t.hasNext()) {
            final Traverser.Admin<Integer> traverser = t.nextTraverser();
            sacks.add(traverser.sack());
            lengths.add(traverser.get());
        }
        assertFalse(t.hasNext());
        //
        assertEquals(6, lengths.size());
        assertEquals(3, lengths.get(0).intValue());
        assertEquals(3, lengths.get(1).intValue());
        assertEquals(3, lengths.get(2).intValue());
        assertEquals(4, lengths.get(3).intValue());
        assertEquals(5, lengths.get(4).intValue());
        assertEquals(6, lengths.get(5).intValue());
        ///
        assertEquals(6, sacks.size());
        assertEquals(4, sacks.get(0).intValue());
        assertEquals(4, sacks.get(1).intValue());
        assertEquals(4, sacks.get(2).intValue());
        assertEquals(5, sacks.get(3).intValue());
        assertEquals(6, sacks.get(4).intValue());
        assertEquals(7, sacks.get(5).intValue());
        //
        assertEquals(24, t.getSideEffects().<Number>get("lengthSum").intValue());
    }

    @Test
    public void shouldHaveValidToString() {
        assertEquals("translator[h:gremlin-jython]", JythonTranslator.of("h").toString());
    }

    @Test
    public void shouldTranslateToJythonAndNotPython() throws Exception {
        // the jython translation bind "g" to java classes and thus does not require the strict python syntax which
        // converts steps like as() to as_(). if those steps are converted then the traversal does not evaluate
        // properly in the python engine. not much of an assertion here to worry about - just need to ensure that
        // the traversal with such steps evaluates to success
        GraphTraversalSource g = TinkerFactory.createModern().traversal();
        g = g.withStrategies(new TranslationStrategy(g, JythonTranslator.of("g")));
        final List<Object> o = g.V().has("name").
                match(__.as("x").label().as("lbl"),
                        __.as("x").id().as("id")).
                select("lbl", "id").
                                 map(Lambda.function("lambda x: type(x.get())")).toList();

        assertEquals(6, o.size());
    }

    @Test
    public void shouldTranslateToJythonWhenUsingLambdasAndStrategies() throws Exception {
        // the jython translation kicks in when you add a lambda so ensure that it translates when strategies are
        // present
        GraphTraversalSource g = TinkerFactory.createModern().traversal();
        g = g.withStrategies(new TranslationStrategy(g, JythonTranslator.of("g")));
        final List<Object> o = g.withStrategies(ReadOnlyStrategy.instance(),
                                                SubgraphStrategy.build().checkAdjacentVertices(false).vertices(hasLabel("person")).create()).
                                 V().has("name").map(Lambda.function("lambda x: type(x.get())")).toList();

        assertEquals(4, o.size());
    }
}
