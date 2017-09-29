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

package org.apache.tinkerpop.gremlin.groovy.jsr223;

import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.TranslationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroovyTranslatorTest {

    @Test
    public void shouldHandleStrategies() throws Exception {
        final TinkerGraph graph = TinkerFactory.createModern();
        GraphTraversalSource g = graph.traversal();
        g = g.withStrategies(SubgraphStrategy.create(new MapConfiguration(new HashMap<String, Object>() {{
            put(SubgraphStrategy.VERTICES, __.has("name", "marko"));
        }})));
        final Bindings bindings = new SimpleBindings();
        bindings.put("g", g);
        Traversal.Admin<Vertex, Object> traversal = new GremlinGroovyScriptEngine().eval(g.V().values("name").asAdmin().getBytecode(), bindings, "g");
        assertEquals("marko", traversal.next());
        assertFalse(traversal.hasNext());
        //
        traversal = new GremlinGroovyScriptEngine().eval(g.withoutStrategies(SubgraphStrategy.class).V().count().asAdmin().getBytecode(), bindings, "g");
        assertEquals(new Long(6), traversal.next());
        assertFalse(traversal.hasNext());
        //
        traversal = new GremlinGroovyScriptEngine().eval(g.withStrategies(SubgraphStrategy.create(new MapConfiguration(new HashMap<String, Object>() {{
            put(SubgraphStrategy.VERTICES, __.has("name", "marko"));
        }})), ReadOnlyStrategy.instance()).V().values("name").asAdmin().getBytecode(), bindings, "g");
        assertEquals("marko", traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    public void shouldSupportStringSupplierLambdas() throws Exception {
        final TinkerGraph graph = TinkerFactory.createModern();
        GraphTraversalSource g = graph.traversal();
        g = g.withStrategies(new TranslationStrategy(g, GroovyTranslator.of("g")));
        GraphTraversal.Admin<Vertex, Integer> t = g.withSideEffect("lengthSum", 0).withSack(1)
                .V()
                .filter(Lambda.predicate("it.get().label().equals('person')"))
                .flatMap(Lambda.function("it.get().vertices(Direction.OUT)"))
                .map(Lambda.<Traverser<Object>, Integer>function("it.get().value('name').length()"))
                .sideEffect(Lambda.consumer("{ x -> x.sideEffects(\"lengthSum\", x.<Integer>sideEffects('lengthSum') + x.get()) }"))
                .order().by(Lambda.comparator("a,b -> a <=> b"))
                .sack(Lambda.biFunction("{ a,b -> a + b }"))
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
    public void shouldHandleMaps() {
        final TinkerGraph graph = TinkerFactory.createModern();
        GraphTraversalSource g = graph.traversal();
        String script = GroovyTranslator.of("g").translate(g.V().id().is(new LinkedHashMap() {{
            put(3, "32");
            put(Arrays.asList(1, 2, 3.1d), 4);
        }}).asAdmin().getBytecode());
        assertEquals(script, "g.V().id().is([((int) 3):(\"32\"),([(int) 1, (int) 2, 3.1d]):((int) 4)])");
    }

    @Test
    public void shouldHaveValidToString() {
        assertEquals("translator[h:gremlin-groovy]", GroovyTranslator.of("h").toString());
    }
}
