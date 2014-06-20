package com.tinkerpop.gremlin.groovy.engine.function;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.ElementValueStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.tinkergraph.process.graph.step.map.TinkerGraphStep;
import com.tinkerpop.gremlin.util.Serializer;
import com.tinkerpop.gremlin.util.function.SSupplier;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinGroovySSupplierTest {

    @Test
    public void shouldSerialize() throws Exception {
        SSupplier<Traversal> traversalSSupplier = new GremlinGroovySSupplier<>("TinkerFactory.createClassic().V.out.name");
        byte[] bytes = Serializer.serializeObject(traversalSSupplier);
        traversalSSupplier = (SSupplier<Traversal>) Serializer.deserializeObject(bytes);
        Traversal traversal = traversalSSupplier.get();
        assertEquals(TinkerGraphStep.class, traversal.getSteps().get(0).getClass());
        assertEquals(VertexStep.class, traversal.getSteps().get(1).getClass());
        assertEquals(ElementValueStep.class, traversal.getSteps().get(2).getClass());
        assertEquals(traversal.getSteps().size(), 3);
    }
}
