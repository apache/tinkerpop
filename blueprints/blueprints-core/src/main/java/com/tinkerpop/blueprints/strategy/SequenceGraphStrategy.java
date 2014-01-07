package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.javatuples.Triplet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.UnaryOperator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SequenceGraphStrategy implements GraphStrategy {
    private final List<GraphStrategy> graphStrategySequence;

    public SequenceGraphStrategy(final GraphStrategy... strategies) {
        this.graphStrategySequence = new ArrayList<>(Arrays.asList(strategies));
    }

    @Override
    public UnaryOperator<Object[]> getPreAddVertex() {
        return this.graphStrategySequence.stream().map(s -> s.getPreAddVertex()).reduce(null,
                (acc, next) -> acc == null ? next : (UnaryOperator<Object[]>) acc.andThen(next));
    }

    @Override
    public UnaryOperator<Vertex> getPostAddVertex() {
        return this.graphStrategySequence.stream().map(s -> s.getPostAddVertex()).reduce(null,
                (acc, next) -> acc == null ? next : (UnaryOperator<Vertex>) acc.andThen(next));
    }

    @Override
    public UnaryOperator<Triplet<String, Vertex, Object[]>> getPreAddEdge() {
        return this.graphStrategySequence.stream().map(s -> s.getPreAddEdge()).reduce(null,
                (acc, next) -> acc == null ? next : (UnaryOperator<Triplet<String, Vertex, Object[]>>) acc.andThen(next));
    }

    @Override
    public UnaryOperator<Edge> getPostAddEdge() {
        return this.graphStrategySequence.stream().map(s -> s.getPostAddEdge()).reduce(null,
                (acc, next) -> acc == null ? next : (UnaryOperator<Edge>) acc.andThen(next));
    }
}