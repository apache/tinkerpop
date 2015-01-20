package com.tinkerpop.gremlin.neo4j.process.graph;

import com.tinkerpop.gremlin.process.graph.EdgeTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Neo4jEdgeTraversal extends Neo4jElementTraversal<Edge>, EdgeTraversal {

    @Override
    public Neo4jGraphTraversal<Edge, Edge> start();

    @Override
    public default <E2> Neo4jGraphTraversal<Edge, Property<E2>> properties(final String... propertyKeys) {
        return (Neo4jGraphTraversal) this.start().properties(propertyKeys);
    }

    @Override
    public default <E2> Neo4jGraphTraversal<Edge, Map<String, Property<E2>>> propertyMap(final String... propertyKeys) {
        return (Neo4jGraphTraversal) this.start().propertyMap(propertyKeys);
    }

    @Override
    public default <E2> Neo4jGraphTraversal<Edge, Map<String, E2>> valueMap(final String... propertyKeys) {
        return (Neo4jGraphTraversal) this.start().valueMap(propertyKeys);
    }

    @Override
    public default <E2> Neo4jGraphTraversal<Edge, Map<String, E2>> valueMap(final boolean includeTokens, final String... propertyKeys) {
        return (Neo4jGraphTraversal) this.start().valueMap(includeTokens, propertyKeys);
    }

    @Override
    public default <E2> Neo4jGraphTraversal<Edge, E2> value() {
        return (Neo4jGraphTraversal) this.start().value();
    }
}
