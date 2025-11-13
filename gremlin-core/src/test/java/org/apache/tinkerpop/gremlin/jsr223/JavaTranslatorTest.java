package org.apache.tinkerpop.gremlin.jsr223;

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JavaTranslatorTest {
    private GraphTraversalSource g = EmptyGraph.instance().traversal();
    private JavaTranslator<GraphTraversalSource, Traversal.Admin<?, ?>> translator = JavaTranslator.of(EmptyGraph.instance().traversal());

    @Test
    public void shouldTranslateHasWithObjectThirdArgValue() {
        final Bytecode bytecode = new Bytecode();
        bytecode.addStep("E");
        bytecode.addStep("has", "knows", "weight", 1.0);
        final Traversal.Admin<?, ?> translation = translator.translate(bytecode);
        assertEquals(g.E().has("knows", "weight", 1.0).asAdmin(), translation);
    }

    @Test
    public void shouldTranslateHasWithPredicateThirdArgValue() {
        final Bytecode bytecode = new Bytecode();
        bytecode.addStep("E");
        bytecode.addStep("has", "knows", "weight", P.eq(1.0));
        final Traversal.Admin<?, ?> translation = translator.translate(bytecode);
        assertEquals(g.E().has("knows", "weight", P.eq(1.0)).asAdmin(), translation);
    }

    @Test
    public void shouldTranslateHasWithNullThirdArgValue() {
        final Bytecode bytecode = new Bytecode();
        bytecode.addStep("E");
        bytecode.addStep("has", "knows", "weight", null);
        final Traversal.Admin<?, ?> translation = translator.translate(bytecode);
        assertEquals(g.E().has("knows", "weight", (String) null).asAdmin(), translation);
    }

    @Test
    public void shouldTranslateHasWithObjectSecondArgValue() {
        final Bytecode bytecode = new Bytecode();
        bytecode.addStep("E");
        bytecode.addStep("has", "weight", 1.0);
        final Traversal.Admin<?, ?> translation = translator.translate(bytecode);
        assertEquals(g.E().has("weight", 1.0).asAdmin(), translation);
    }

    @Test
    public void shouldTranslateHasWithPredicateSecondArgValue() {
        final Bytecode bytecode = new Bytecode();
        bytecode.addStep("E");
        bytecode.addStep("has", "weight", P.eq(1.0));
        final Traversal.Admin<?, ?> translation = translator.translate(bytecode);
        assertEquals(g.E().has("weight", P.eq(1.0)).asAdmin(), translation);
    }

    @Test
    public void shouldTranslateHasWithNullSecondArgValue() {
        final Bytecode bytecode = new Bytecode();
        bytecode.addStep("E");
        bytecode.addStep("has", "weight", null);
        final Traversal.Admin<?, ?> translation = translator.translate(bytecode);
        assertEquals(g.E().has("weight", (String) null).asAdmin(), translation);
    }

}