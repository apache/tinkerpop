package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.process.Step;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.filter.HasStep;
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
    private final Set<String> readPartitions = new HashSet<>();

    public PartitionStrategy(final String partitionKey, final String partition) {
        this.writePartition = partition;
        this.addReadPartition(partition);
        this.partitionKey = partitionKey;
    }

    public String getWritePartition() {
        return this.writePartition;
    }

    public void setWritePartition(final String writePartition) {
        this.writePartition = writePartition;
        this.readPartitions.add(writePartition);
    }

    public String getPartitionKey() {
        return this.partitionKey;
    }

    public Set<String> getReadPartitions() {
        return Collections.unmodifiableSet(this.readPartitions);
    }

    public void removeReadPartition(final String readPartition) {
        this.readPartitions.remove(readPartition);
    }

    public void addReadPartition(final String readPartition) {
        this.readPartitions.add(readPartition);
    }

    public void clearReadPartitions() {
        this.readPartitions.clear();
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // todo: what about "add vertex"
        final List<Step> stepsToInsertHasAfter = new ArrayList<>();
        stepsToInsertHasAfter.addAll(TraversalHelper.getStepsOfAssignableClass(GraphStep.class, traversal));
        stepsToInsertHasAfter.addAll(TraversalHelper.getStepsOfAssignableClass(VertexStep.class, traversal));
        stepsToInsertHasAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeOtherVertexStep.class, traversal));
        stepsToInsertHasAfter.addAll(TraversalHelper.getStepsOfAssignableClass(AddEdgeStep.class, traversal));
        stepsToInsertHasAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeVertexStep.class, traversal));
        stepsToInsertHasAfter.addAll(TraversalHelper.getStepsOfAssignableClass(AddVertexStep.class, traversal));

        // all steps that return a vertex need to have has(paritionKey,within,partitionValues) injected after it
        stepsToInsertHasAfter.forEach(s -> TraversalHelper.insertAfterStep(
                new HasStep(traversal, new HasContainer(partitionKey, Contains.within, new ArrayList<>(readPartitions))), s, traversal));

        // all write edge steps need to have partition keys tossed into the property key/value list after mutating steps
        TraversalHelper.getStepsOfAssignableClass(AddEdgeStep.class, traversal).forEach(s -> {
            final Object[] keyValues = Stream.concat(Stream.of(s.getKeyValues()), Stream.of(partitionKey, writePartition)).toArray();
            TraversalHelper.replaceStep(s, new AddEdgeStep(traversal, s.getDirection(), s.getEdgeLabel(), s.getVertices().iterator(), keyValues), traversal);
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
}
