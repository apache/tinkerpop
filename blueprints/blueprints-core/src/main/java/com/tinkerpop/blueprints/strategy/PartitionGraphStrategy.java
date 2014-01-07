package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.TriFunction;
import org.javatuples.Triplet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
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
    public UnaryOperator<Function<Object[], Vertex>> getWrapAddVertex() {
        return (f) -> (keyValues) -> {
            final List<Object> o = new ArrayList<>(Arrays.asList(keyValues));
            o.addAll(Arrays.asList(this.partitionKey, writePartition));
            return f.apply(o.toArray());
        };
    }

    @Override
    public UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getWrapAddEdge() {
        return (f) -> (label, v, keyValues) -> {
            final List<Object> o = new ArrayList<>(Arrays.asList(keyValues));
            o.addAll(Arrays.asList(this.partitionKey, writePartition));
            return f.apply(label, v, o.toArray());
        };
    }
}