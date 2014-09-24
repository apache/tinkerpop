package com.tinkerpop.gremlin.groovy.function;

import com.tinkerpop.gremlin.groovy.function.GSSupplier;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.ValueStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.tinkergraph.process.graph.step.sideEffect.TinkerGraphStep;
import com.tinkerpop.gremlin.util.Serializer;
import org.junit.Test;

import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GSSupplierTest {

    @Test
    public void shouldSerialize() throws Exception {
        Supplier<Traversal> traversalSSupplier = new GSSupplier<>("TinkerFactory.createClassic().V().out().value('name')");
        byte[] bytes = Serializer.serializeObject(traversalSSupplier);
        traversalSSupplier = (Supplier<Traversal>) Serializer.deserializeObject(bytes);
        Traversal traversal = traversalSSupplier.get();
        assertEquals(TinkerGraphStep.class, traversal.getSteps().get(0).getClass());
        assertEquals(VertexStep.class, traversal.getSteps().get(1).getClass());
        assertEquals(ValueStep.class, traversal.getSteps().get(2).getClass());
        assertEquals(traversal.getSteps().size(), 3);
    }
}
