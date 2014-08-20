package com.tinkerpop.gremlin.neo4j.process.graph.util;

import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jTraversal;
import com.tinkerpop.gremlin.neo4j.process.graph.strategy.Neo4jGraphStepStrategy;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultNeo4jTraversal<S, E> extends DefaultGraphTraversal<S, E> implements Neo4jTraversal<S, E> {

    public DefaultNeo4jTraversal() {
        super();
        this.strategies.register(Neo4jGraphStepStrategy.instance());
    }

    public DefaultNeo4jTraversal(final Neo4jGraph neo4jGraph) {
        this();
        this.sideEffects().setGraph(neo4jGraph);
    }
}
