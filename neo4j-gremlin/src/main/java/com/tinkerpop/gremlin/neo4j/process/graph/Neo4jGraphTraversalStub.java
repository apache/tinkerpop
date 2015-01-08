package com.tinkerpop.gremlin.neo4j.process.graph;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Element;

import java.util.HashMap;
import java.util.Map;

/**
 * Neo4jGraphTraversalStub is merged with {@link GraphTraversal} via the Maven exec-plugin.
 * The Maven plugin yields Neo4jGraphTraversal which is ultimately what is depended on by user source.
 * This class maintains {@link Neo4jGraphTraversal} specific methods that extend {@link GraphTraversal}.
 * Adding {@link Element} to the JavaDoc so it sticks in the import during "optimize imports" code cleaning.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
interface Neo4jGraphTraversalStub<S, E> extends GraphTraversal.Admin<S, E>, GraphTraversal<S, E> {

    @Override
    public default <E2> Neo4jGraphTraversal<S, E2> addStep(final Step<?, E2> step) {
        return (Neo4jGraphTraversal) GraphTraversal.Admin.super.addStep((Step) step);
    }

    public default <E2> Neo4jGraphTraversal<S, Map<String, E2>> cypher(final String query) {
        return this.cypher(query, new HashMap<>());
    }

    public <E2> Neo4jGraphTraversal<S, Map<String, E2>> cypher(final String query, final Map<String, Object> parameters);
}