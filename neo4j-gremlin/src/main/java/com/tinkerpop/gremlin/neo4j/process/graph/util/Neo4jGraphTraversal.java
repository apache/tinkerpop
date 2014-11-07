package com.tinkerpop.gremlin.neo4j.process.graph.util;

import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jTraversal;
import com.tinkerpop.gremlin.neo4j.process.graph.strategy.Neo4jGraphStepStrategy;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.graph.strategy.GraphTraversalStrategyRegistry;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.DefaultTraversalStrategies;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Neo4jGraphTraversal<S, E> extends DefaultGraphTraversal<S, E> implements Neo4jTraversal<S, E> {

    static {
        final DefaultTraversalStrategies traversalStrategies = new DefaultTraversalStrategies();
        GraphTraversalStrategyRegistry.instance().getTraversalStrategies().forEach(traversalStrategies::addStrategy);
        traversalStrategies.addStrategy(Neo4jGraphStepStrategy.instance());
        TraversalStrategies.GlobalCache.registerStrategies(Neo4jGraphTraversal.class, traversalStrategies);
    }

    public Neo4jGraphTraversal() {
        super();
    }

    public Neo4jGraphTraversal(final Neo4jGraph neo4jGraph) {
        super(neo4jGraph);
    }

    @Override
    public <E2> Neo4jTraversal<S, E2> addStep(final Step<?, E2> step) {
        if (this.locked) throw Exceptions.traversalIsLocked();
        TraversalHelper.insertStep(step, this);
        return (Neo4jTraversal) this;
    }
}
