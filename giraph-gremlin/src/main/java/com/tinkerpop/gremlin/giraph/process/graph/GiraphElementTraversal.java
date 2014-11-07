package com.tinkerpop.gremlin.giraph.process.graph;

import com.tinkerpop.gremlin.giraph.process.graph.strategy.GiraphElementStepStrategy;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.strategy.GraphTraversalStrategyRegistry;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.DefaultTraversalStrategies;
import com.tinkerpop.gremlin.structure.Element;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphElementTraversal<A> extends DefaultGraphTraversal<A, A> {

    static {
        final DefaultTraversalStrategies traversalStrategies = new DefaultTraversalStrategies();
        GraphTraversalStrategyRegistry.instance().getTraversalStrategies().forEach(traversalStrategies::addStrategy);
        traversalStrategies.addStrategy(GiraphElementStepStrategy.instance());
        TraversalStrategies.GlobalCache.registerStrategies(GiraphElementTraversal.class, traversalStrategies);
    }

    public GiraphElementTraversal(final Element element, final GiraphGraph graph) {
        super(graph);
        this.addStep(new StartStep<>(this, element));
    }
}
