package com.tinkerpop.gremlin.neo4j.process.graph;

import com.tinkerpop.gremlin.process.graph.EdgeTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Neo4jEdgeTraversal extends Neo4jElementTraversal<Edge>, EdgeTraversal {

    public default <E2> Neo4jTraversal<Edge, Property<E2>> properties(final String... propertyKeys) {
        return (Neo4jTraversal) this.start().properties(propertyKeys);
    }

    public default <E2> Neo4jTraversal<Edge, Map<String, Property<E2>>> propertyMap(final String... propertyKeys) {
        return (Neo4jTraversal) this.start().propertyMap(propertyKeys);
    }

    public default <E2> Neo4jTraversal<Edge, Property<E2>> hiddens(final String... propertyKeys) {
        return (Neo4jTraversal) this.start().hiddens(propertyKeys);
    }

    public default <E2> Neo4jTraversal<Edge, Map<String, Property<E2>>> hiddenMap(final String... propertyKeys) {
        return (Neo4jTraversal) this.start().hiddenMap(propertyKeys);
    }

    public default <E2> Neo4jTraversal<Edge, Map<String, E2>> valueMap(final String... propertyKeys) {
        return this.start().valueMap(propertyKeys);
    }

    public default <E2> Neo4jTraversal<Edge, Map<String, E2>> hiddenValueMap(final String... propertyKeys) {
        return this.start().hiddenValueMap(propertyKeys);
    }

    public default <E2> Neo4jTraversal<Edge, E2> value() {
        return this.start().value();
    }
}
