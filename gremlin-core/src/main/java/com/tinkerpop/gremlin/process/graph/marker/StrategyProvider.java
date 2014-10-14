package com.tinkerpop.gremlin.process.graph.marker;

import com.tinkerpop.gremlin.process.TraversalStrategy;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface StrategyProvider {

    public List<TraversalStrategy> getStrategies(final EngineDependent.Engine engine);
}
