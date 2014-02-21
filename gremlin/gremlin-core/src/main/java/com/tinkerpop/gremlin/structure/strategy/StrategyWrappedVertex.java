package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Strategy;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.NoSuchElementException;
import java.util.function.Consumer;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StrategyWrappedVertex extends StrategyWrappedElement implements Vertex {
    private final Vertex baseVertex;
    private transient final Strategy.Context<Vertex> strategyContext;


    public StrategyWrappedVertex(final Vertex baseVertex, final StrategyWrappedGraph strategyWrappedGraph) {
        super(strategyWrappedGraph, baseVertex);
        this.strategyContext = new Strategy.Context<>(strategyWrappedGraph, baseVertex);
        this.baseVertex = baseVertex;
    }

    public Vertex getBaseVertex() {
        return this.baseVertex;
    }

    @Override
    public <V> V getValue(String key) throws NoSuchElementException {
        return baseVertex.getValue(key);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        final Vertex baseInVertex = (inVertex instanceof StrategyWrappedVertex) ? ((StrategyWrappedVertex) inVertex).getBaseVertex() : inVertex;
        return new StrategyWrappedEdge(this.strategyWrappedGraph.strategy().compose(
                s -> s.getAddEdgeStrategy(strategyContext),
                this.baseVertex::addEdge)
                .apply(label, baseInVertex, keyValues), this.strategyWrappedGraph);
    }

    @Override
    public Traversal<Vertex, Vertex> out(int branchFactor, String... labels) {
        return this.baseVertex.out(branchFactor, labels);
    }

    @Override
    public Traversal<Vertex, Vertex> in(int branchFactor, String... labels) {
        return this.baseVertex.in(branchFactor, labels);
    }

    @Override
    public Traversal<Vertex, Vertex> both(int branchFactor, String... labels) {
        return this.baseVertex.both(branchFactor, labels);
    }

    @Override
    public Traversal<Vertex, Edge> outE(int branchFactor, String... labels) {
        return this.baseVertex.outE(branchFactor, labels);
    }

    @Override
    public Traversal<Vertex, Edge> inE(int branchFactor, String... labels) {
        return this.baseVertex.inE(branchFactor, labels);
    }

    @Override
    public Traversal<Vertex, Edge> bothE(int branchFactor, String... labels) {
        return this.baseVertex.bothE(branchFactor, labels);
    }

    @Override
    public Traversal<Vertex, Vertex> out(String... labels) {
        return this.baseVertex.out(labels);
    }

    @Override
    public Traversal<Vertex, Vertex> in(String... labels) {
        return this.baseVertex.in(labels);
    }

    @Override
    public Traversal<Vertex, Vertex> both(String... labels) {
        return this.baseVertex.both(labels);
    }

    @Override
    public Traversal<Vertex, Edge> outE(String... labels) {
        return this.baseVertex.outE(labels);
    }

    @Override
    public Traversal<Vertex, Edge> inE(String... labels) {
        return this.baseVertex.inE(labels);
    }

    @Override
    public Traversal<Vertex, Edge> bothE(String... labels) {
        return this.baseVertex.bothE(labels);
    }

    @Override
    public Traversal<Vertex, Vertex> start() {
        return this.baseVertex.start();
    }

    @Override
    public Traversal<Vertex, Vertex> as(String as) {
        return this.baseVertex.as(as);
    }

    @Override
    public <E2> Traversal<Vertex, AnnotatedValue<E2>> annotatedValues(String propertyKey) {
        return this.baseVertex.annotatedValues(propertyKey);
    }

    @Override
    public <E2> Traversal<Vertex, Property<E2>> property(String propertyKey) {
        return this.baseVertex.property(propertyKey);
    }

    @Override
    public <E2> Traversal<Vertex, E2> value(String propertyKey) {
        return this.baseVertex.value(propertyKey);
    }

    @Override
    public Traversal<Vertex, Vertex> with(Object... variableValues) {
        return this.baseVertex.with(variableValues);
    }

    @Override
    public Traversal<Vertex, Vertex> sideEffect(Consumer<Holder<Vertex>> consumer) {
        return this.baseVertex.sideEffect(consumer);
    }
}
