/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PartitionStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {
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
        // no need to add has after mutating steps because we want to make it so that the write partition can
        // be independent of the read partition.  in other words, i don't need to be able to read from a partition
        // in order to write to it.
        final List<Step> stepsToInsertHasAfter = new ArrayList<>();
        stepsToInsertHasAfter.addAll(TraversalHelper.getStepsOfAssignableClass(GraphStep.class, traversal));
        stepsToInsertHasAfter.addAll(TraversalHelper.getStepsOfAssignableClass(VertexStep.class, traversal));
        stepsToInsertHasAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeOtherVertexStep.class, traversal));
        stepsToInsertHasAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeVertexStep.class, traversal));

        // all steps that return a vertex need to have has(paritionKey,within,partitionValues) injected after it
        stepsToInsertHasAfter.forEach(step -> TraversalHelper.insertAfterStep(
                new HasStep(traversal, new HasContainer(this.partitionKey, P.within(new ArrayList<>(this.readPartitions)))), step, traversal));

        traversal.getSteps().forEach(step -> {
            if (step instanceof AddEdgeStep || step instanceof AddVertexStep || step instanceof AddVertexStartStep) {
                ((Mutating) step).addPropertyMutations(this.partitionKey, this.writePartition);
            }
        });
    }

    public final static class Builder {
        private String writePartition;
        private String partitionKey;
        private Set<String> readPartitions = new HashSet<>();

        Builder() {
        }

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
            if (partitionKey == null || partitionKey.isEmpty())
                throw new IllegalStateException("The partitionKey cannot be null or empty");

            return new PartitionStrategy(this.partitionKey, this.writePartition, this.readPartitions);
        }
    }
}
