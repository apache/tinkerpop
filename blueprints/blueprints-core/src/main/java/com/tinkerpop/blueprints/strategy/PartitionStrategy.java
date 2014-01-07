package com.tinkerpop.blueprints.strategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */

public class PartitionStrategy implements Strategy {

    private String writePartition;
    private final String partitionKey;

    public PartitionStrategy(final String partitionKey, final String partition) {
        this.writePartition = partition;
        this.partitionKey = partitionKey;
    }

    @Override
    public Function<Object[], Object[]> getPreAddVertex() {
        return (args) -> {
            final List<Object> o = new ArrayList<>(Arrays.asList(args));
            o.addAll(Arrays.asList(this.partitionKey, writePartition));
            return o.toArray();
        };
    }
}