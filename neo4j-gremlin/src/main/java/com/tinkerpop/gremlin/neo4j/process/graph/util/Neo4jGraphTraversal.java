package com.tinkerpop.gremlin.neo4j.process.graph.util;

import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jTraversal;
import com.tinkerpop.gremlin.neo4j.process.graph.strategy.Neo4jGraphStepStrategy;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Neo4jGraphTraversal<S, E> extends DefaultGraphTraversal<S, E> implements Neo4jTraversal<S, E> {

    public Neo4jGraphTraversal() {
        super();
    }

    public Neo4jGraphTraversal(final Neo4jGraph neo4jGraph) {
        this();
        this.sideEffects().setGraph(neo4jGraph);
        this.getStrategies().register(Neo4jGraphStepStrategy.instance());
    }

    @Override
    public <E2> Neo4jTraversal<S, E2> addStep(final Step<?, E2> step) {
        if (this.strategies.complete()) throw Exceptions.traversalIsLocked();
        TraversalHelper.insertStep(step, this);
        return (Neo4jTraversal) this;
    }
}
