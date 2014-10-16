package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphComputerStrategy extends AbstractTraversalStrategy implements TraversalStrategy.NoDependencies {

    private static final GraphComputerStrategy INSTANCE = new GraphComputerStrategy();

    private GraphComputerStrategy() {
    }


    @Override
    public void apply(final Traversal<?, ?> traversal) {
        traversal.getSteps().stream()
                .filter(step -> step instanceof EngineDependent)
                .forEach(step -> ((EngineDependent) step).onEngine(EngineDependent.Engine.COMPUTER));
    }

    public static GraphComputerStrategy instance() {
        return INSTANCE;
    }
}
