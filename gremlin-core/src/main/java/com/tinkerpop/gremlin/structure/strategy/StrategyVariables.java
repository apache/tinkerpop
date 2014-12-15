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
    private final StrategyContext<StrategyVariables> variableStrategyContext;
    private final GraphStrategy strategy;

    public StrategyVariables(final Graph.Variables variables, final StrategyGraph strategyGraph) {
        if (variables instanceof StrategyWrapped) throw new IllegalArgumentException(
                String.format("The variables %s is already StrategyWrapped and must be a base Variables", variables));
        this.baseVariables = variables;
        this.strategyGraph = strategyGraph;
        this.variableStrategyContext = new StrategyContext<>(strategyGraph, this);
        this.strategy = strategyGraph.getStrategy();
    }

    @Override
    public Set<String> keys() {
        return this.strategyGraph.compose(
                s -> s.getVariableKeysStrategy(this.variableStrategyContext, strategy),
                this.baseVariables::keys).get();
    }

    @Override
    public <R> Optional<R> get(final String key) {
        return this.strategyGraph.compose(
                s -> s.<R>getVariableGetStrategy(this.variableStrategyContext, strategy),
                this.baseVariables::get).apply(key);
    }

    @Override
    public void set(final String key, final Object value) {
        this.strategyGraph.compose(
                s -> s.getVariableSetStrategy(this.variableStrategyContext, strategy),
                this.baseVariables::set).accept(key, value);
    }

    @Override
    public void remove(final String key) {
        this.strategyGraph.compose(
                s -> s.getVariableRemoveStrategy(this.variableStrategyContext, strategy),
                this.baseVariables::remove).accept(key);
    }

    @Override
    public Map<String, Object> asMap() {
        return this.strategyGraph.compose(
                s -> s.getVariableAsMapStrategy(this.variableStrategyContext, strategy),
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
