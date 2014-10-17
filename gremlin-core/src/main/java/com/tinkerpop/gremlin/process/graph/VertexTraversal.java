package com.tinkerpop.gremlin.process.graph;

import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;

import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface VertexTraversal extends ElementTraversal<Vertex> {

    public default <E2> GraphTraversal<Vertex, VertexProperty<E2>> properties(final String... propertyKeys) {
        return (GraphTraversal) this.start().properties(propertyKeys);
    }

    public default <E2> GraphTraversal<Vertex, Map<String, List<VertexProperty<E2>>>> propertyMap(final String... propertyKeys) {
        return this.start().propertyMap(propertyKeys);
    }

    public default <E2> GraphTraversal<Vertex, VertexProperty<E2>> hiddens(final String... propertyKeys) {
        return (GraphTraversal) this.start().hiddens(propertyKeys);
    }

    public default <E2> GraphTraversal<Vertex, Map<String, List<VertexProperty<E2>>>> hiddenMap(final String... propertyKeys) {
        return this.start().hiddenMap(propertyKeys);
    }

    public default <E2> GraphTraversal<Vertex, Map<String, List<E2>>> valueMap(final String... propertyKeys) {
        return this.start().valueMap(propertyKeys);
    }

    public default <E2> GraphTraversal<Vertex, Map<String, List<E2>>> hiddenValueMap(final String... propertyKeys) {
        return this.start().hiddenValueMap(propertyKeys);
    }

    public default <E2> GraphTraversal<Vertex, E2> value() {
        return this.start().value();
    }
}
