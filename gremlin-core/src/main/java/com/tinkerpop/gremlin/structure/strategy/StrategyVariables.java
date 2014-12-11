package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedVariables;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class StrategyVariables implements StrategyWrapped, Graph.Variables, WrappedVariables<Graph.Variables> {

    protected final StrategyGraph strategyGraph;
    private final Graph.Variables baseVariables;
    private final Strategy.StrategyContext<StrategyVariables> variableStrategyContext;

    public StrategyVariables(final Graph.Variables variables, final StrategyGraph strategyGraph) {
        if (variables instanceof StrategyWrapped) throw new IllegalArgumentException(
                String.format("The variables %s is already StrategyWrapped and must be a base Variables", variables));
        this.baseVariables = variables;
        this.strategyGraph = strategyGraph;
        this.variableStrategyContext = new Strategy.StrategyContext<>(strategyGraph, this);
    }

    public Strategy.StrategyContext<StrategyVariables> getVariableStrategyContext() {
        return variableStrategyContext;
    }

    @Override
    public Set<String> keys() {
        return this.strategyGraph.getStrategy().compose(
                s -> s.getVariableKeysStrategy(this.variableStrategyContext),
                this.baseVariables::keys).get();
    }

    @Override
    public <R> Optional<R> get(final String key) {
        return this.strategyGraph.getStrategy().compose(
                s -> s.<R>getVariableGetStrategy(this.variableStrategyContext),
                this.baseVariables::get).apply(key);
    }

    @Override
    public void set(final String key, final Object value) {
        this.strategyGraph.getStrategy().compose(
                s -> s.getVariableSetStrategy(this.variableStrategyContext),
                this.baseVariables::set).accept(key, value);
    }

    @Override
    public void remove(final String key) {
        this.strategyGraph.getStrategy().compose(
                s -> s.getVariableRemoveStrategy(this.variableStrategyContext),
                this.baseVariables::remove).accept(key);
    }

    @Override
    public Map<String, Object> asMap() {
        return this.strategyGraph.getStrategy().compose(
                s -> s.getVariableAsMapStrategy(this.variableStrategyContext),
                this.baseVariables::asMap).get();
    }

    @Override
    public Graph.Variables getBaseVariables() {
        return this.baseVariables;
    }

    @Override
    public String toString() {
        return StringFactory.graphStrategyVariables(this);
    }
}
