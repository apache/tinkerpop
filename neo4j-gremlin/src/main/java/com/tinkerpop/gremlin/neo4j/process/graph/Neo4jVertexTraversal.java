package com.tinkerpop.gremlin.neo4j.process.graph;

import com.tinkerpop.gremlin.process.graph.VertexTraversal;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Neo4jVertexTraversal extends Neo4jElementTraversal<Vertex>, VertexTraversal {

    public default <E2> Neo4jGraphTraversal<Vertex, VertexProperty<E2>> properties(final String... propertyKeys) {
        return (Neo4jGraphTraversal) this.start().properties(propertyKeys);
    }

    public default <E2> Neo4jGraphTraversal<Vertex, Map<String, List<VertexProperty<E2>>>> propertyMap(final String... propertyKeys) {
        return (Neo4jGraphTraversal) this.start().propertyMap(propertyKeys);
    }

    public default <E2> Neo4jGraphTraversal<Vertex, Map<String, List<E2>>> valueMap(final String... propertyKeys) {
        return this.start().valueMap(propertyKeys);
    }


    public default <E2> Neo4jGraphTraversal<Vertex, Map<String, List<E2>>> valueMap(final boolean includeTokens, final String... propertyKeys) {
        return this.start().valueMap(includeTokens,propertyKeys);
    }

    public default <E2> Neo4jGraphTraversal<Vertex, E2> value() {
        return this.start().value();
    }
}
