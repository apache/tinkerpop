package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Graph;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StrategyWrappedAnnotations implements Graph.Annotations, StrategyWrapped {

    private final Graph.Annotations baseAnnotations;
    private final transient Strategy.Context<StrategyWrappedAnnotations> strategyContext;
    private final StrategyWrappedGraph strategyWrappedGraph;

    public StrategyWrappedAnnotations(final Graph.Annotations baseAnnotations, final StrategyWrappedGraph strategyWrappedGraph) {
        this.baseAnnotations = baseAnnotations;
        this.strategyContext = new Strategy.Context<>(strategyWrappedGraph.getBaseGraph(), this);
        this.strategyWrappedGraph = strategyWrappedGraph;
    }

    public Graph.Annotations getBaseAnnotations() {
        return this.baseAnnotations;
    }

    @Override
    public void set(String key, Object value) {
        this.strategyWrappedGraph.strategy().compose(
                s -> s.getGraphAnnotationsSet(strategyContext),
                this.baseAnnotations::set).accept(key, value);
    }

    @Override
    public <T> Optional<T> get(String key) {
        return this.baseAnnotations.get(key);
    }

    @Override
    public Set<String> getKeys() {
        return this.baseAnnotations.getKeys();
    }

    @Override
    public Map<String, Object> getAnnotations() {
        return this.baseAnnotations.getAnnotations();
    }
}
