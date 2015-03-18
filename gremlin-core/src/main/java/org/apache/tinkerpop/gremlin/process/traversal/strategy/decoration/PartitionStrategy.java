package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.process.Step;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.AddEdgeByPathStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.graph.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Contains;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class PartitionStrategy extends AbstractTraversalStrategy {
    private String writePartition;
    private final String partitionKey;
    private final Set<String> readPartitions;

    private PartitionStrategy(final String partitionKey, final String partition, final Set<String> readPartitions) {
        this.writePartition = partition;
        this.partitionKey = partitionKey;
        this.readPartitions = Collections.unmodifiableSet(readPartitions);
    }

    public String getWritePartition() {
        return this.writePartition;
    }

    public String getPartitionKey() {
        return this.partitionKey;
    }

    public Set<String> getReadPartitions() {
        return readPartitions;
    }

    public static Builder build() {
        return new Builder();
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final Set<String> readPartitionsWithWritePartition = new HashSet<>(readPartitions);
        readPartitionsWithWritePartition.add(writePartition);

        // no need to add has after mutating steps because we want to make it so that the write partition can
        // be independent of the read partition.  in other words, i don't need to be able to read from a partition
        // in order to write to it.
        final List<Step> stepsToInsertHasAfter = new ArrayList<>();
        stepsToInsertHasAfter.addAll(TraversalHelper.getStepsOfAssignableClass(GraphStep.class, traversal));
        stepsToInsertHasAfter.addAll(TraversalHelper.getStepsOfAssignableClass(VertexStep.class, traversal));
        stepsToInsertHasAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeOtherVertexStep.class, traversal));
        stepsToInsertHasAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeVertexStep.class, traversal));

        // all steps that return a vertex need to have has(paritionKey,within,partitionValues) injected after it
        stepsToInsertHasAfter.forEach(s -> TraversalHelper.insertAfterStep(
                new HasStep(traversal, new HasContainer(partitionKey, Contains.within, new ArrayList<>(readPartitions))), s, traversal));

        // all write edge steps need to have partition keys tossed into the property key/value list after mutating steps
        TraversalHelper.getStepsOfAssignableClass(AddEdgeStep.class, traversal).forEach(s -> {
            final Object[] keyValues = Stream.concat(Stream.of(s.getKeyValues()), Stream.of(partitionKey, writePartition)).toArray();
            TraversalHelper.replaceStep(s, new AddEdgeStep(traversal, s.getDirection(), s.getEdgeLabel(), s.getVertices().iterator(), keyValues), traversal);
        });

        TraversalHelper.getStepsOfAssignableClass(AddEdgeByPathStep.class, traversal).forEach(s -> {
            final Object[] keyValues = Stream.concat(Stream.of(s.getKeyValues()), Stream.of(partitionKey, writePartition)).toArray();
            TraversalHelper.replaceStep(s, new AddEdgeByPathStep(traversal, s.getDirection(), s.getEdgeLabel(), s.getStepLabel(), keyValues), traversal);
        });

        // all write vertex steps need to have partition keys tossed into the property key/value list after mutating steps
        TraversalHelper.getStepsOfAssignableClass(AddVertexStep.class, traversal).forEach(s -> {
            final Object[] keyValues = Stream.concat(Stream.of(s.getKeyValues()), Stream.of(partitionKey, writePartition)).toArray();
            TraversalHelper.replaceStep(s, new AddVertexStep(traversal, keyValues), traversal);
        });

        TraversalHelper.getStepsOfAssignableClass(AddVertexStartStep.class, traversal).forEach(s -> {
            final Object[] keyValues = Stream.concat(Stream.of(s.getKeyValues()), Stream.of(partitionKey, writePartition)).toArray();
            TraversalHelper.replaceStep(s, new AddVertexStartStep(traversal, keyValues), traversal);
        });
    }

    public static class Builder {
        private String writePartition;
        private String partitionKey;
        private Set<String> readPartitions = new HashSet<>();

        Builder() {}

        public Builder writePartition(final String writePartition) {
            this.writePartition = writePartition;
            return this;
        }

        public Builder partitionKey(final String partitionKey) {
            this.partitionKey = partitionKey;
            return this;
        }

        public Builder addReadPartition(final String readPartition) {
            this.readPartitions.add(readPartition);
            return this;
        }

        public PartitionStrategy create() {
            if (partitionKey == null || partitionKey.isEmpty()) throw new IllegalStateException("The partitionKey cannot be null or empty");

            return new PartitionStrategy(this.partitionKey, this.writePartition, this.readPartitions);
        }
    }
}
