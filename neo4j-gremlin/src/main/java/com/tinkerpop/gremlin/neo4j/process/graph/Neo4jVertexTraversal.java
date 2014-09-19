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

    public default <E2> Neo4jTraversal<Vertex, VertexProperty<E2>> properties(final String... propertyKeys) {
        return (Neo4jTraversal) this.start().properties(propertyKeys);
    }

    public default <E2> Neo4jTraversal<Vertex, Map<String, List<VertexProperty<E2>>>> propertyMap(final String... propertyKeys) {
        return (Neo4jTraversal) this.start().propertyMap(propertyKeys);
    }

    public default <E2> Neo4jTraversal<Vertex, VertexProperty<E2>> hiddens(final String... propertyKeys) {
        return (Neo4jTraversal) this.start().hiddens(propertyKeys);
    }

    public default <E2> Neo4jTraversal<Vertex, Map<String, List<VertexProperty<E2>>>> hiddenMap(final String... propertyKeys) {
        return (Neo4jTraversal) this.start().hiddenMap(propertyKeys);
    }

    public default <E2> Neo4jTraversal<Vertex, Map<String, List<E2>>> valueMap(final String... propertyKeys) {
        return this.start().valueMap(propertyKeys);
    }

    public default <E2> Neo4jTraversal<Vertex, Map<String, List<E2>>> hiddenValueMap(final String... propertyKeys) {
        return this.start().hiddenValueMap(propertyKeys);
    }

    public default <E2> Neo4jTraversal<Vertex, E2> value() {
        return this.start().value();
    }
}
