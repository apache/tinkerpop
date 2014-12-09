package com.tinkerpop.gremlin.process.graph;

import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface VertexPropertyTraversal extends ElementTraversal<VertexProperty> {

    public default <E2> GraphTraversal<VertexProperty, Property<E2>> properties(final String... propertyKeys) {
        return (GraphTraversal) this.start().properties(propertyKeys);
    }

    public default <E2> GraphTraversal<VertexProperty, Map<String, Property<E2>>> propertyMap(final String... propertyKeys) {
        return this.start().propertyMap(propertyKeys);
    }

    public default <E2> GraphTraversal<VertexProperty, Map<String, E2>> valueMap(final String... propertyKeys) {
        return this.start().valueMap(propertyKeys);
    }
}
