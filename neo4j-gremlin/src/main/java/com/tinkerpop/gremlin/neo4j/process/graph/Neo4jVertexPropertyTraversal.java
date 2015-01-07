package com.tinkerpop.gremlin.neo4j.process.graph;

import com.tinkerpop.gremlin.process.graph.VertexPropertyTraversal;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Neo4jVertexPropertyTraversal extends Neo4jElementTraversal<VertexProperty>, VertexPropertyTraversal {

    public default <E2> Neo4jGraphTraversal<VertexProperty, Property<E2>> properties(final String... propertyKeys) {
        return (Neo4jGraphTraversal) this.start().properties(propertyKeys);
    }

    public default <E2> Neo4jGraphTraversal<VertexProperty, Map<String, Property<E2>>> propertyMap(final String... propertyKeys) {
        return (Neo4jGraphTraversal) this.start().propertyMap(propertyKeys);
    }

    public default <E2> Neo4jGraphTraversal<VertexProperty, Map<String, E2>> valueMap(final String... propertyKeys) {
        return (Neo4jGraphTraversal) this.start().valueMap(propertyKeys);
    }

    public default <E2> Neo4jGraphTraversal<VertexProperty, Map<String, E2>> valueMap(final boolean includeTokens, final String... propertyKeys) {
        return (Neo4jGraphTraversal) this.start().valueMap(includeTokens, propertyKeys);
    }
}
