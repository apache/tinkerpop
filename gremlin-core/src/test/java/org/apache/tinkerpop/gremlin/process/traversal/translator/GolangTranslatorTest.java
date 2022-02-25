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

package org.apache.tinkerpop.gremlin.process.traversal.translator;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.junit.Assert.assertEquals;

public class GolangTranslatorTest {
    private static final GraphTraversalSource g = traversal().withEmbedded(EmptyGraph.instance());
    private static final Translator.ScriptTranslator translator = GolangTranslator.of("g");

    @Test
    public void shouldHaveValidToString() {
        assertEquals("translator[h:gremlin-go]", GolangTranslator.of("h").toString());
    }

    @Test
    public void shouldTranslateOption() {
        final String gremlinAsGo = translator.translate(
                g.V().has("person", "name", "marko").asAdmin().getBytecode()).getScript();
        assertEquals("g.V().Has(\"person\", \"name\", \"marko\")", gremlinAsGo);
    }

    @Test
    public void shouldTranslateCardinality() {
        final String gremlinAsGo = translator.translate(
                g.addV("person").property(VertexProperty.Cardinality.list, "name", "marko").asAdmin().getBytecode()).getScript();
        assertEquals("g.AddV(\"person\").Property(Cardinality.List, \"name\", \"marko\")", gremlinAsGo);
    }

    @Test
    public void shouldTranslateMultilineStrings() {
        final String gremlinAsGo = translator.translate(
                g.addV().property("text", "a" + System.lineSeparator() + "\"and\"" + System.lineSeparator() + "b").asAdmin().getBytecode()).getScript();
        final String escapedSeparator = StringEscapeUtils.escapeJava(System.lineSeparator());
        final String expected = "g.AddV().Property(\"text\", \"a" + escapedSeparator + StringEscapeUtils.escapeJava("\"and\"") + escapedSeparator + "b\")";
        assertEquals(expected, gremlinAsGo);
    }

    @Test
    public void shouldTranslateChildTraversals() {
        final String gremlinAsGo = translator.translate(
                g.V().has("person", "name", "marko").
                        where(outE()).asAdmin().getBytecode()).getScript();
        assertEquals("g.V().Has(\"person\", \"name\", \"marko\").Where(gremlingo.AnonTrav__.OutE())", gremlinAsGo);
    }

    @Test
    public void shouldTranslateGoNamedSteps() {
        final String gremlinAsGo = translator.translate(
                g.V().has("person", "name", "marko").
                        where(outE().count().is(2).and(__.not(inE().count().is(3)))).asAdmin().getBytecode()).getScript();
        assertEquals("g.V().Has(\"person\", \"name\", \"marko\").Where(gremlingo.AnonTrav__.OutE().Count().Is(2).And(gremlingo.AnonTrav__.Not(gremlingo.AnonTrav__.InE().Count().Is(3))))", gremlinAsGo);
    }

    // TODO AN-987: TraversalStrategy implementation in Gremlin-go
    @Ignore
    @Test
    public void shouldTranslateStrategies() {
        assertEquals("g.withStrategies(*[TraversalStrategy('ReadOnlyStrategy', None, 'org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy'),TraversalStrategy('SubgraphStrategy',{'checkAdjacentVertices':False,'vertices':__.hasLabel('person')}, 'org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy'),TraversalStrategy('SeedStrategy',{'seed':999999,'strategy':'org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy'}, 'org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy')]).V().has('name')",
                translator.translate(g.withStrategies(ReadOnlyStrategy.instance(),
                                SubgraphStrategy.build().checkAdjacentVertices(false).vertices(hasLabel("person")).create(),
                                new SeedStrategy(999999)).
                        V().has("name").asAdmin().getBytecode()).getScript());
    }

    // TODO: AN-1037 Lambda support in Gremlin-Go
    @Ignore
    @Test
    public void shouldTranslateLambdas() {
        final Bytecode bytecode = g.withSideEffect("lengthSum", 0).withSack(1)
                .V()
                .filter(Lambda.predicate("x -> x.get().label() == 'person'", "gremlin-groovy"))
                .flatMap(Lambda.function("it.get().vertices(Direction.OUT)", "gremlin-groovy"))
                .map(Lambda.<Traverser<Object>, Integer>function("x -> x : len(x.get().value('name'))", "gremlin-groovy"))
                .sideEffect(Lambda.consumer("x -> x.sideEffects(\"lengthSum\", x.sideEffects('lengthSum') + x.get())    ", "gremlin-groovy"))
                .order().by(Lambda.comparator("a,b -> a == b ? 0 : (a > b) ? 1 : -1)", "gremlin-groovy"))
                .sack(Lambda.biFunction("a,b -> a + b", "gremlin-groovy"))
                .asAdmin().getBytecode();
        assertEquals("g.withSideEffect('lengthSum',0).withSack(1).V().filter(lambda: \"x -> x.get().label() == 'person'\").flatMap(lambda: \"it.get().vertices(Direction.OUT)\").map(lambda: \"x -> x : len(x.get().value('name'))\").sideEffect(lambda: \"x -> x.sideEffects(\\\"lengthSum\\\", x.sideEffects('lengthSum') + x.get())\").order().by(lambda: \"a,b -> a == b ? 0 : (a > b) ? 1 : -1)\").sack(lambda: \"a,b -> a + b\")",
                translator.translate(bytecode).getScript());
    }
}
