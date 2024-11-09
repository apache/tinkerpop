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
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.GraphOp;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.CardinalityValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.junit.Assert.assertEquals;

public class PythonTranslatorTest {
    private static final GraphTraversalSource g = traversal().withEmbedded(EmptyGraph.instance());
    private static final Translator.ScriptTranslator translator = PythonTranslator.of("g");
    private static final Translator.ScriptTranslator noSugarTranslator = PythonTranslator.of("g", new PythonTranslator.NoSugarTranslator(false));

    @Test
    public void shouldHaveValidToString() {
        assertEquals("translator[h:gremlin-python]", PythonTranslator.of("h").toString());
    }

    @Test
    public void shouldTranslateOption() {
        final String gremlinAsPython = translator.translate(
                g.V().has("person", "name", "marko").asAdmin().getBytecode()).getScript();
        assertEquals("g.V().has('person','name','marko')", gremlinAsPython);
    }

    @Test
    public void shouldTranslateCardinality() {
        final String gremlinAsPython = translator.translate(
                g.addV("person").property(VertexProperty.Cardinality.list, "name", "marko").asAdmin().getBytecode()).getScript();
        assertEquals("g.addV('person').property(Cardinality.list_,'name','marko')", gremlinAsPython);
    }

    @Test
    public void shouldTranslateCardinalityValue() {
        final Map<Object, Object> m = new HashMap<>();
        m.put("name", VertexProperty.Cardinality.set("marko"));
        final String gremlinAsPython = translator.translate(
                g.mergeV(new HashMap<>()).option(Merge.onMatch, m).asAdmin().getBytecode()).getScript();
        assertEquals("g.merge_v({}).option(Merge.on_match,{'name':CardinalityValue.set_('marko')})", gremlinAsPython);
    }

    @Test
    public void shouldTranslateMultilineStrings() {
        final String gremlinAsPython = translator.translate(
                g.addV().property("text", "a"+ System.lineSeparator() + "\"and\"" + System.lineSeparator() + "b").asAdmin().getBytecode()).getScript();
        assertEquals("g.addV().property('text',\"\"\"a" + System.lineSeparator() + "\"and\"" + System.lineSeparator() + "b\"\"\")", gremlinAsPython);
    }

    @Test
    public void shouldTranslateChildTraversals() {
        final String gremlinAsPython = translator.translate(
                g.V().has("person", "name", "marko").
                  where(outE()).asAdmin().getBytecode()).getScript();
        assertEquals("g.V().has('person','name','marko').where(__.outE())", gremlinAsPython);
    }

    @Test
    public void shouldTranslatePythonNamedSteps() {
        final String gremlinAsPython = translator.translate(
                g.V().has("person", "name", "marko").
                        where(outE().count().is(2).and(__.not(inE().count().is(3)))).asAdmin().getBytecode()).getScript();
        assertEquals("g.V().has('person','name','marko').where(__.outE().count().is_(2).and_(__.not_(__.inE().count().is_(3))))", gremlinAsPython);
    }

    @Test
    public void shouldTranslateTextP() {
        assertTranslation("TextP.containing('ark')", TextP.containing("ark"));
        assertTranslation("TextP.regex('ark')", TextP.regex("ark"));
        assertTranslation("TextP.not_regex('ark')", TextP.notRegex("ark"));
    }

    @Test
    public void shouldTranslateStrategies() {
        assertEquals("g.withStrategies(*[TraversalStrategy('ReadOnlyStrategy', None, 'org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy'),TraversalStrategy('SubgraphStrategy',{'checkAdjacentVertices':False,'vertices':__.hasLabel('person')}, 'org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy'),TraversalStrategy('SeedStrategy',{'seed':999999,'strategy':'org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy'}, 'org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy')]).V().has('name')",
                translator.translate(g.withStrategies(ReadOnlyStrategy.instance(),
                        SubgraphStrategy.build().checkAdjacentVertices(false).vertices(hasLabel("person")).create(),
                        SeedStrategy.build().seed(999999).create()).
                        V().has("name").asAdmin().getBytecode()).getScript());
    }

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
        assertEquals("g.withSideEffect('lengthSum',0).withSack(1).V().filter_(lambda: \"x -> x.get().label() == 'person'\").flatMap(lambda: \"it.get().vertices(Direction.OUT)\").map(lambda: \"x -> x : len(x.get().value('name'))\").sideEffect(lambda: \"x -> x.sideEffects(\\\"lengthSum\\\", x.sideEffects('lengthSum') + x.get())\").order().by(lambda: \"a,b -> a == b ? 0 : (a > b) ? 1 : -1)\").sack(lambda: \"a,b -> a + b\")",
                translator.translate(bytecode).getScript());
    }

    @Test
    public void shouldTranslateWithSyntaxSugar() {
      final String gremlinAsPython = translator.translate(g.V().range(0, 10).has("person", "name", "marko").limit(2).values("name").asAdmin().getBytecode()) 
          .getScript();
      assertEquals("g.V()[0:10].has('person','name','marko')[0:2].name", gremlinAsPython);
    }

    @Test
    public void shouldTranslateWithoutSyntaxSugar() {
      final String gremlinAsPython = noSugarTranslator
          .translate(g.V().range(0, 10).has("person", "name", "marko").limit(2).values("name").asAdmin().getBytecode())
          .getScript();
      assertEquals("g.V().range_(0,10).has('person','name','marko').limit(2).values('name')", gremlinAsPython);
    }

    @Test
    public void shouldTranslateTx() {
        String script = translator.translate(GraphOp.TX_COMMIT.getBytecode()).getScript();
        assertEquals("g.tx().commit()", script);
        script = translator.translate(GraphOp.TX_ROLLBACK.getBytecode()).getScript();
        assertEquals("g.tx().rollback()", script);
    }

    private void assertTranslation(final String expectedTranslation, final Object... objs) {
        final String script = translator.translate(g.inject(objs).asAdmin().getBytecode()).getScript();
        assertEquals(String.format("g.inject(%s)", expectedTranslation), script);
    }
}
