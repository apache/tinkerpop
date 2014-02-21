package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.NoSuchElementException;
import java.util.function.Consumer;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StrategyWrappedVertex extends StrategyWrappedElement implements Vertex, StrategyWrapped {
    private final Vertex baseVertex;
    private final Strategy.Context<StrategyWrappedVertex> strategyContext;

    public StrategyWrappedVertex(final Vertex baseVertex, final StrategyWrappedGraph strategyWrappedGraph) {
        super(baseVertex, strategyWrappedGraph);
        this.strategyContext = new Strategy.Context<>(strategyWrappedGraph.getBaseGraph(), this);
        this.baseVertex = baseVertex;
    }

    public Vertex getBaseVertex() {
        return this.baseVertex;
    }

    @Override
    public <V> V getValue(final String key) throws NoSuchElementException {
        return baseVertex.getValue(key);
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        final Vertex baseInVertex = (inVertex instanceof StrategyWrappedVertex) ? ((StrategyWrappedVertex) inVertex).getBaseVertex() : inVertex;
        return new StrategyWrappedEdge(this.strategyWrappedGraph.strategy().compose(
                s -> s.getAddEdgeStrategy(strategyContext),
                this.baseVertex::addEdge)
                .apply(label, baseInVertex, keyValues), this.strategyWrappedGraph);
    }

    @Override
    public Traversal<Vertex, Vertex> out(final int branchFactor, final String... labels) {
        return this.baseVertex.out(branchFactor, labels);
    }

    @Override
    public Traversal<Vertex, Vertex> in(final int branchFactor, final String... labels) {
        return this.baseVertex.in(branchFactor, labels);
    }

    @Override
    public Traversal<Vertex, Vertex> both(final int branchFactor, final String... labels) {
        return this.baseVertex.both(branchFactor, labels);
    }

    @Override
    public Traversal<Vertex, Edge> outE(final int branchFactor, final String... labels) {
        return this.baseVertex.outE(branchFactor, labels);
    }

    @Override
    public Traversal<Vertex, Edge> inE(final int branchFactor, final String... labels) {
        return this.baseVertex.inE(branchFactor, labels);
    }

    @Override
    public Traversal<Vertex, Edge> bothE(final int branchFactor, final String... labels) {
        return this.baseVertex.bothE(branchFactor, labels);
    }

    @Override
    public Traversal<Vertex, Vertex> out(final String... labels) {
        return this.baseVertex.out(labels);
    }

    @Override
    public Traversal<Vertex, Vertex> in(final String... labels) {
        return this.baseVertex.in(labels);
    }

    @Override
    public Traversal<Vertex, Vertex> both(final String... labels) {
        return this.baseVertex.both(labels);
    }

    @Override
    public Traversal<Vertex, Edge> outE(final String... labels) {
        return this.baseVertex.outE(labels);
    }

    @Override
    public Traversal<Vertex, Edge> inE(final String... labels) {
        return this.baseVertex.inE(labels);
    }

    @Override
    public Traversal<Vertex, Edge> bothE(final String... labels) {
        return this.baseVertex.bothE(labels);
    }

    @Override
    public Traversal<Vertex, Vertex> start() {
        return this.baseVertex.start();
    }

    @Override
    public Traversal<Vertex, Vertex> as(final String as) {
        return this.baseVertex.as(as);
    }

    @Override
    public <E2> Traversal<Vertex, AnnotatedValue<E2>> annotatedValues(final String propertyKey) {
        return this.baseVertex.annotatedValues(propertyKey);
    }

    @Override
    public <E2> Traversal<Vertex, Property<E2>> property(final String propertyKey) {
        return this.baseVertex.property(propertyKey);
    }

    @Override
    public <E2> Traversal<Vertex, E2> value(final String propertyKey) {
        return this.baseVertex.value(propertyKey);
    }

    @Override
    public Traversal<Vertex, Vertex> with(final Object... variableValues) {
        return this.baseVertex.with(variableValues);
    }

    @Override
    public Traversal<Vertex, Vertex> sideEffect(final Consumer<Holder<Vertex>> consumer) {
        return this.baseVertex.sideEffect(consumer);
    }
}
