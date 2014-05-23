package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import org.javatuples.Pair;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class StrategyWrappedElement implements Element, StrategyWrapped {
    protected final StrategyWrappedGraph strategyWrappedGraph;
    private final Element baseElement;
    private final Strategy.Context<StrategyWrappedElement> elementStrategyContext;

    protected StrategyWrappedElement(final Element baseElement, final StrategyWrappedGraph strategyWrappedGraph) {
        this.strategyWrappedGraph = strategyWrappedGraph;
        this.baseElement = baseElement;
        this.elementStrategyContext = new Strategy.Context<>(strategyWrappedGraph.getBaseGraph(), this);
    }

    public Element getBaseElement() {
        return this.baseElement;
    }

    @Override
    public <V> V value(final String key) throws NoSuchElementException {
        return this.baseElement.value(key);
    }

    @Override
    public void properties(final Object... keyValues) {
		this.strategyWrappedGraph.strategy().compose(
			s -> s.getElementProperties(elementStrategyContext),
			this.baseElement::properties).accept(keyValues);
    }

    @Override
    public <V> Property<V> property(final String key) {
        return new StrategyWrappedProperty<>(this.strategyWrappedGraph.strategy().compose(
                s -> s.<V>getElementGetProperty(elementStrategyContext),
                this.baseElement::property).apply(key), this.strategyWrappedGraph);
    }

    @Override
    public Map<String, Property> properties() {
        return this.baseElement.properties().entrySet().stream().map(e -> Pair.with(e.getKey(), new StrategyWrappedProperty(e.getValue(), strategyWrappedGraph)))
                .collect(Collectors.toMap(p -> p.getValue0(), p -> p.getValue1()));
    }

    @Override
    public Map<String, Property> hiddens() {
        return this.baseElement.hiddens();
    }

    @Override
    public Set<String> keys() {
        return this.baseElement.keys();
    }

    @Override
    public String label() {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.getElementLabel(elementStrategyContext),
                this.baseElement::label).get();
    }

    @Override
    public Object id() {
		return this.strategyWrappedGraph.strategy().compose(
                s -> s.getElementId(elementStrategyContext),
                this.baseElement::id).get();
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.<V>getElementProperty(elementStrategyContext),
                this.baseElement::property).apply(key, value);
    }

    @Override
    public void remove() {
        this.strategyWrappedGraph.strategy().compose(
                s -> s.getRemoveElementStrategy(elementStrategyContext),
                () -> {
                    this.baseElement.remove();
                    return null;
                }).get();
    }

	@Override
	public String toString() {
		final GraphStrategy strategy = this.strategyWrappedGraph.strategy().getGraphStrategy().orElse(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
		return String.format("[%s[%s]]", strategy, baseElement.toString());
	}

    protected <S,E> GraphTraversal<S,E> applyStrategy(final GraphTraversal<S,E> traversal) {
        traversal.strategies().register(new StrategyWrappedTraversalStrategy(this.strategyWrappedGraph));
        this.strategyWrappedGraph.strategy().getGraphStrategy().ifPresent(s -> s.applyStrategyToTraversal(traversal));
        return traversal;
    }
}
