package com.tinkerpop.gremlin.process.graph;

import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;

import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface VertexTraversal extends ElementTraversal<Vertex> {

    @Override
    default GraphTraversal<Vertex, Vertex> start() {
        final GraphTraversal<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(this.getClass());
        return traversal.asAdmin().addStep(new StartStep<>(traversal, this));
    }

    @Override
    public default <E2> GraphTraversal<Vertex, VertexProperty<E2>> properties(final String... propertyKeys) {
        return (GraphTraversal) this.start().properties(propertyKeys);
    }

    public default <E2> GraphTraversal<Vertex, Map<String, List<VertexProperty<E2>>>> propertyMap(final String... propertyKeys) {
        return this.start().propertyMap(propertyKeys);
    }

    public default <E2> GraphTraversal<Vertex, Map<String, List<E2>>> valueMap(final String... propertyKeys) {
        return this.start().valueMap(propertyKeys);
    }

    public default <E2> GraphTraversal<Vertex, Map<String, List<E2>>> valueMap(final boolean includeTokens, final String... propertyKeys) {
        return this.start().valueMap(includeTokens, propertyKeys);
    }

    // necessary so VertexProperty.value() as a non-traversal method works
    public default <E2> GraphTraversal<Vertex, E2> value() {
        return this.start().value();
    }

}
