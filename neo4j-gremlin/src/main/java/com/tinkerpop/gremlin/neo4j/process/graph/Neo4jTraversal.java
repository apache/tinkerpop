package com.tinkerpop.gremlin.neo4j.process.graph;

import com.tinkerpop.gremlin.neo4j.process.graph.strategy.Neo4jGraphStepStrategy;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.util.DefaultTraversal;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Neo4jTraversal<S,E> extends GraphTraversal<S,E> {

    public default Neo4jTraversal<S, E> cypher(final String query) {
        return null;
    }

    public default Neo4jTraversal<S, E> cypher(final String query, final Map<String,Object> params) {
        return null;
    }

    public static <S> Neo4jTraversal<S, S> of(final Graph graph) {
        if (!(graph instanceof Neo4jGraph)) throw new IllegalArgumentException(String.format("graph must be of type %s", Neo4jGraph.class));

        final Neo4jTraversal traversal = new DefaultNeo4jTraversal();
        traversal.memory().set("g", graph);
        traversal.memory().set("cypher", ((Neo4jGraph) graph).getCypher());
        return traversal;
    }

    public class DefaultNeo4jTraversal<S,E> extends DefaultTraversal<S,E> implements Neo4jTraversal<S,E> {
        public DefaultNeo4jTraversal() {
            super();
            this.strategies.register(Neo4jGraphStepStrategy.instance());
        }
    }
}
