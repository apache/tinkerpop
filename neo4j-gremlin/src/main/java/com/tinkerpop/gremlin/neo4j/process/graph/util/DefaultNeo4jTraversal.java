package com.tinkerpop.gremlin.neo4j.process.graph.util;

import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jTraversal;
import com.tinkerpop.gremlin.neo4j.process.graph.strategy.Neo4jGraphStepStrategy;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

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

    @Override
    public <E2> Neo4jTraversal<S,E2> addStep(final Step<?, E2> step) {
        TraversalHelper.insertStep(step, this.getSteps().size(), this);
        return (Neo4jTraversal) this;
    }
}
