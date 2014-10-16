package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphStandardStrategy extends AbstractTraversalStrategy implements TraversalStrategy.NoDependencies {

    private static final GraphStandardStrategy INSTANCE = new GraphStandardStrategy();

    private GraphStandardStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal) {
        traversal.getSteps().stream()
                .filter(step -> step instanceof EngineDependent)
                .forEach(step -> ((EngineDependent) step).onEngine(EngineDependent.Engine.STANDARD));
    }

    public static GraphStandardStrategy instance() {
        return INSTANCE;
    }
}
