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

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.junit.Assert.assertEquals;

public class PythonTranslatorTest {
    private static final GraphTraversalSource g = traversal().withEmbedded(EmptyGraph.instance());
    private static final Translator.ScriptTranslator translator = PythonTranslator.of("g");

    @Test
    public void shouldHaveValidToString() {
        assertEquals("translator[h:gremlin-python]", PythonTranslator.of("h").toString());
    }

    @Test
    public void shouldTranslate() {
        final String gremlinAsPython = translator.translate(
                g.V().has("person", "name", "marko").asAdmin().getBytecode());
        assertEquals("g.V().has('person','name','marko')", gremlinAsPython);
    }

    @Test
    public void shouldTranslateCardinality() {
        final String gremlinAsPython = translator.translate(
                g.addV("person").property(VertexProperty.Cardinality.list, "name", "marko").asAdmin().getBytecode());
        assertEquals("g.addV('person').property(Cardinality.list_,'name','marko')", gremlinAsPython);
    }

    @Test
    public void shouldTranslateMultilineStrings() {
        final String gremlinAsPython = translator.translate(
                g.addV().property("text", "a"+ System.lineSeparator() + "\"and\"" + System.lineSeparator() + "b").asAdmin().getBytecode());
        assertEquals("g.addV().property('text',\"\"\"a" + System.lineSeparator() + "\"and\"" + System.lineSeparator() + "b\"\"\")", gremlinAsPython);
    }

    @Test
    public void shouldTranslateChildTraversals() {
        final String gremlinAsPython = translator.translate(
                g.V().has("person", "name", "marko").
                  where(outE()).asAdmin().getBytecode());
        assertEquals("g.V().has('person','name','marko').where(__.outE())", gremlinAsPython);
    }

    @Test
    public void shouldTranslatePythonNamedSteps() {
        final String gremlinAsPython = translator.translate(
                g.V().has("person", "name", "marko").
                        where(outE().count().is(2).and(__.not(inE().count().is(3)))).asAdmin().getBytecode());
        assertEquals("g.V().has('person','name','marko').where(__.outE().count().is_(2).and_(__.not_(__.inE().count().is_(3))))", gremlinAsPython);
    }

    @Test
    public void shouldTranslateStrategies() {
        assertEquals("g.withStrategies([TraversalStrategy('ReadOnlyStrategy'),TraversalStrategy('SubgraphStrategy',{'checkAdjacentVertices':False,'vertices':__.hasLabel('person')})]).V().has('name')",
                translator.translate(g.withStrategies(ReadOnlyStrategy.instance(),
                        SubgraphStrategy.build().checkAdjacentVertices(false).vertices(hasLabel("person")).create()).
                        V().has("name").asAdmin().getBytecode()));
    }

    @Test
    public void shouldTranslatePythonLambdas() {
        final Bytecode bytecode = g.withSideEffect("lengthSum", 0).withSack(1)
                .V()
                .filter(Lambda.predicate("x : x.get().label() == 'person'"))
                .flatMap(Lambda.function("lambda x : x.get().vertices(Direction.OUT)"))
                .map(Lambda.<Traverser<Object>, Integer>function("lambda x : len(x.get().value('name'))"))
                .sideEffect(Lambda.consumer(" x : x.sideEffects(\"lengthSum\", x.sideEffects('lengthSum') + x.get())    "))
                .order().by(Lambda.comparator("  lambda: a,b : 0 if a == b else 1 if a > b else -1"))
                .sack(Lambda.biFunction("lambda: a,b : a + b"))
                .asAdmin().getBytecode();
        assertEquals("g.withSideEffect('lengthSum',0).withSack(1).V().filter(lambda: \"x : x.get().label() == 'person'\").flatMap(lambda: \"lambda x : x.get().vertices(Direction.OUT)\").map(lambda: \"lambda x : len(x.get().value('name'))\").sideEffect(lambda: \"x : x.sideEffects(\\\"lengthSum\\\", x.sideEffects('lengthSum') + x.get())\").order().by(lambda: \"lambda: a,b : 0 if a == b else 1 if a > b else -1\").sack(lambda: \"lambda: a,b : a + b\")",
                translator.translate(bytecode));
    }

    @Test
    public void shouldTranslateGroovyLambdas() {
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
                translator.translate(bytecode));
    }
}
