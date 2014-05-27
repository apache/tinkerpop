package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Graph;

import java.util.Map;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StrategyWrappedVariables implements StrategyWrapped, Graph.Variables {

	protected final StrategyWrappedGraph strategyWrappedGraph;
	private final Graph.Variables baseVariables;
	private final Strategy.Context<StrategyWrappedVariables> variableStrategyContext;

	public StrategyWrappedVariables(final Graph.Variables variables, final StrategyWrappedGraph strategyWrappedGraph) {
		this.baseVariables = variables;
		this.strategyWrappedGraph = strategyWrappedGraph;
		this.variableStrategyContext = new Strategy.Context<>(strategyWrappedGraph.getBaseGraph(), this);
	}

	@Override
	public Set<String> getVariables() {
		return this.strategyWrappedGraph.strategy().compose(
				s -> s.getVariableGetVariablesStrategy(variableStrategyContext),
				this.baseVariables::getVariables).get();
	}

	@Override
	public <R> R get(final String variable) {
		return this.strategyWrappedGraph.strategy().compose(
				s -> s.<R>getVariableGetStrategy(variableStrategyContext),
				this.baseVariables::get).apply(variable);
	}

	@Override
	public void set(final String variable, final Object value) {
		this.set(variable, value);
	}

	@Override
	public Graph getGraph() {
		return strategyWrappedGraph;
	}

	@Override
	public Map<String, Object> asMap() {
		return this.baseVariables.asMap();
	}
}
