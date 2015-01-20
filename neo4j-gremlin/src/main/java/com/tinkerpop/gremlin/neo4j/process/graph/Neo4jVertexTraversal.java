package com.tinkerpop.gremlin.neo4j.process.graph;

import com.tinkerpop.gremlin.process.graph.VertexTraversal;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;

import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Neo4jVertexTraversal extends Neo4jElementTraversal<Vertex>, VertexTraversal {

    @Override
    public Neo4jGraphTraversal<Vertex, Vertex> start();

    @Override
    public default <E2> Neo4jGraphTraversal<Vertex, VertexProperty<E2>> properties(final String... propertyKeys) {
        return (Neo4jGraphTraversal) this.start().properties(propertyKeys);
    }

    @Override
    public default <E2> Neo4jGraphTraversal<Vertex, Map<String, List<VertexProperty<E2>>>> propertyMap(final String... propertyKeys) {
        return (Neo4jGraphTraversal) this.start().propertyMap(propertyKeys);
    }

    @Override
    public default <E2> Neo4jGraphTraversal<Vertex, Map<String, List<E2>>> valueMap(final String... propertyKeys) {
        return (Neo4jGraphTraversal) this.start().valueMap(propertyKeys);
    }

    @Override
    public default <E2> Neo4jGraphTraversal<Vertex, Map<String, List<E2>>> valueMap(final boolean includeTokens, final String... propertyKeys) {
        return (Neo4jGraphTraversal) this.start().valueMap(includeTokens, propertyKeys);
    }

    @Override
    public default <E2> Neo4jGraphTraversal<Vertex, E2> value() {
        return (Neo4jGraphTraversal) this.start().value();
    }
}
