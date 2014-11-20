package com.tinkerpop.gremlin.hadoop.process.graph;

import com.tinkerpop.gremlin.hadoop.process.graph.strategy.HadoopElementStepStrategy;
import com.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.strategy.GraphTraversalStrategyRegistry;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.DefaultTraversalStrategies;
import com.tinkerpop.gremlin.structure.Element;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopElementTraversal<A> extends DefaultGraphTraversal<A, A> {

    static {
        final DefaultTraversalStrategies traversalStrategies = new DefaultTraversalStrategies();
        GraphTraversalStrategyRegistry.instance().getTraversalStrategies().forEach(traversalStrategies::addStrategy);
        traversalStrategies.addStrategy(HadoopElementStepStrategy.instance());
        TraversalStrategies.GlobalCache.registerStrategies(HadoopElementTraversal.class, traversalStrategies);
    }

    public HadoopElementTraversal(final Element element, final HadoopGraph graph) {
        super(graph);
        this.addStep(new StartStep<>(this, element));
    }
}
