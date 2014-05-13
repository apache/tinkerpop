package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.filter.HasStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Contains;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.util.function.TriFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class PartitionGraphStrategy implements GraphStrategy {

    private String writePartition;
    private final String partitionKey;
    private final Set<String> readPartitions = new HashSet<>();

    public PartitionGraphStrategy(final String partitionKey, final String partition) {
        this.writePartition = partition;
        this.addReadPartition(partition);
        this.partitionKey = partitionKey;
    }

    public String getWritePartition() {
        return writePartition;
    }

    public void setWritePartition(final String writePartition) {
        this.writePartition = writePartition;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public Set<String> getReadPartitions() {
        return Collections.unmodifiableSet(readPartitions);
    }

    public void removeReadPartition(final String readPartition) {
        this.readPartitions.remove(readPartition);
    }

    public void addReadPartition(final String readPartition) {
        this.readPartitions.add(readPartition);
    }

    @Override
    public UnaryOperator<Supplier<GraphTraversal<Vertex, Vertex>>> getVStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return (f) -> () -> {
            final GraphTraversal traversal = f.get();
            // assume the first step is just to iterate all vertices - not sure if that's
            TraversalHelper.insertStep(new HasStep(traversal, new HasContainer(this.partitionKey, T.convert(T.in), readPartitions)), 1, traversal);
            return traversal;
        };
    }

    @Override
    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return (f) -> (keyValues) -> {
            final List<Object> o = new ArrayList<>(Arrays.asList(keyValues));
            o.addAll(Arrays.asList(this.partitionKey, writePartition));
            return f.apply(o.toArray());
        };
    }

    @Override
    public UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return (f) -> (label, v, keyValues) -> {
            final List<Object> o = new ArrayList<>(Arrays.asList(keyValues));
            o.addAll(Arrays.asList(this.partitionKey, writePartition));
            return f.apply(label, v, o.toArray());
        };
    }
}