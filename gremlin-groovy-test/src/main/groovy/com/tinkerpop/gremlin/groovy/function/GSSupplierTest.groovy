package com.tinkerpop.gremlin.groovy.function;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.ValueStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.util.Serializer
import org.junit.Ignore;
import org.junit.Test;

import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GSSupplierTest extends AbstractGremlinTest {

    // todo: need to apply bindings to GSSupplier so that we can bind "g" to the Graph instance

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    @Ignore
    public void shouldSerialize() throws Exception {
        final GSSupplier<Traversal> traversalSSupplier = new GSSupplier<>("g.V().out().value('name')");
        final byte[] bytes = Serializer.serializeObject(traversalSSupplier);
        traversalSSupplier = (Supplier<Traversal>) Serializer.deserializeObject(bytes);
        Traversal traversal = traversalSSupplier.get();
        assertEquals(graphProvider.getGraphStepImplementation(), traversal.getSteps().get(0).getClass());
        assertEquals(VertexStep.class, traversal.getSteps().get(1).getClass());
        assertEquals(ValueStep.class, traversal.getSteps().get(2).getClass());
        assertEquals(traversal.getSteps().size(), 3);
    }
}
