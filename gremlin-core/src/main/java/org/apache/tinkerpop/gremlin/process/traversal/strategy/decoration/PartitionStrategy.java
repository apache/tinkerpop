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
import org.apache.tinkerpop.gremlin.process.traversal.Parameterizing;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.LambdaFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PartitionStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {
    private String writePartition;
    private final String partitionKey;
    private final Set<String> readPartitions;
    private final boolean includeMetaProperties;

    private PartitionStrategy(final Builder builder) {
        this.writePartition = builder.writePartition;
        this.partitionKey = builder.partitionKey;
        this.readPartitions = Collections.unmodifiableSet(builder.readPartitions);
        this.includeMetaProperties  = builder.includeMetaProperties;
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

    public boolean isIncludeMetaProperties() {
        return includeMetaProperties;
    }

    public static Builder build() {
        return new Builder();
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final boolean supportsMetaProperties = traversal.getGraph().isPresent()
                && traversal.getGraph().get().features().vertex().supportsMetaProperties();
        if (includeMetaProperties && !supportsMetaProperties)
            throw new IllegalStateException("PartitionStrategy is configured to include meta-properties but the Graph does not support them");

        // no need to add has after mutating steps because we want to make it so that the write partition can
        // be independent of the read partition.  in other words, i don't need to be able to read from a partition
        // in order to write to it.
        final List<Step> stepsToInsertHasAfter = new ArrayList<>();
        stepsToInsertHasAfter.addAll(TraversalHelper.getStepsOfAssignableClass(GraphStep.class, traversal));
        stepsToInsertHasAfter.addAll(TraversalHelper.getStepsOfAssignableClass(VertexStep.class, traversal));
        stepsToInsertHasAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeOtherVertexStep.class, traversal));
        stepsToInsertHasAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeVertexStep.class, traversal));

        // all steps that return a vertex need to have has(partitionKey,within,partitionValues) injected after it
        stepsToInsertHasAfter.forEach(step -> TraversalHelper.insertAfterStep(
                new HasStep(traversal, new HasContainer(partitionKey, P.within(new ArrayList<>(readPartitions)))), step, traversal));

        if (includeMetaProperties) {
            final List<PropertiesStep> propertiesSteps = TraversalHelper.getStepsOfAssignableClass(PropertiesStep.class, traversal);
            propertiesSteps.forEach(step -> {
                if (step.getReturnType() == PropertyType.PROPERTY) {
                    // check the following step to see if it is a has(partitionKey, *) - if so then this strategy was
                    // already applied down below via g.V().values() which injects a properties() step
                    final Step next = step.getNextStep();
                    if (!(next instanceof HasStep) || !((HasContainer) ((HasStep) next).getHasContainers().get(0)).getKey().equals(partitionKey)) {
                        // use choose() to determine if the properties() step is called on a Vertex to get a VertexProperty
                        // if not, pass it through.
                        final Traversal choose = __.choose(
                                __.filter(new TypeChecker<>(VertexProperty.class)),
                                __.has(partitionKey, P.within(new ArrayList<>(readPartitions))),
                                __.__());
                        TraversalHelper.insertTraversal(step, choose.asAdmin(), traversal);
                    }
                } else if (step.getReturnType() == PropertyType.VALUE) {
                    // use choose() to determine if the values() step is called on a Vertex to get a VertexProperty
                    // if not, pass it through otherwise explode g.V().values() to g.V().properties().has().value()
                    final Traversal choose = __.choose(
                            __.filter(new TypeChecker<>(Vertex.class)),
                            __.properties(step.getPropertyKeys()).has(partitionKey, P.within(new ArrayList<>(readPartitions))).value(),
                            __.__());
                    TraversalHelper.insertTraversal(step, choose.asAdmin(), traversal);
                    traversal.removeStep(step);
                } else {
                    throw new IllegalStateException(String.format("%s is not accounting for a particular PropertyType %s",
                            PartitionStrategy.class.getSimpleName(), step.getReturnType()));
                }
            });
        }

        final List<Step> stepsToInsertPropertyMutations = traversal.getSteps().stream().filter(step ->
            step instanceof AddEdgeStep || step instanceof AddVertexStep ||
                    step instanceof AddVertexStartStep || (includeMetaProperties && step instanceof AddPropertyStep)
        ).collect(Collectors.toList());

        stepsToInsertPropertyMutations.forEach(step -> {
            // note that with AddPropertyStep we just add the partition key/value regardless of whether this
            // ends up being a Vertex or not.  AddPropertyStep currently chooses to simply not bother
            // to use the additional "property mutations" if the Element being mutated is a Edge or
            // VertexProperty
            ((Mutating) step).addPropertyMutations(partitionKey, writePartition);

            if (includeMetaProperties) {
                // GraphTraversal folds g.addV().property('k','v') to just AddVertexStep/AddVertexStartStep so this
                // has to be exploded back to g.addV().property('k','v','partition','A')
                if (step instanceof AddVertexStartStep || step instanceof AddVertexStep) {
                    final Parameters parameters = ((Parameterizing) step).getParameters();
                    final Map<Object, Object> params = parameters.getRaw();
                    params.forEach((k, v) -> {
                        final AddPropertyStep addPropertyStep = new AddPropertyStep(traversal, null, k, v);
                        addPropertyStep.addPropertyMutations(partitionKey, writePartition);
                        TraversalHelper.insertAfterStep(addPropertyStep, step, traversal);

                        // need to remove the parameter from the AddVertex/StartStep because it's now being added
                        // via the AddPropertyStep
                        parameters.remove(k);
                    });
                }
            }
        });
    }

    public final class TypeChecker<A> implements Predicate<Traverser<A>>, Serializable {
        final Class<? extends Element> toCheck;

        public TypeChecker(final Class<? extends Element> toCheck) {
            this.toCheck = toCheck;
        }

        @Override
        public boolean test(final Traverser traverser) {
            return toCheck.isAssignableFrom(traverser.get().getClass());
        }

        @Override
        public String toString() {
            return "instanceOf(" + toCheck.getSimpleName() + ")";
        }
    }

    public final static class Builder {
        private String writePartition;
        private String partitionKey;
        private Set<String> readPartitions = new HashSet<>();
        private boolean includeMetaProperties = false;

        Builder() {
        }

        /**
         * Set to {@code true} if the {@link VertexProperty} instances should get assigned to partitions.  This
         * has the effect of hiding properties within a particular partition so that in order for the
         * {@link VertexProperty} to be seen both the parent {@link Vertex} and the {@link VertexProperty} must have
         * readable partitions defined in the strategy.
         * <p/>
         * When setting this to {@code true} (it is {@code false} by default) it is important that the {@link Graph}
         * support the meta-properties feature.  If it does not errors will ensue.
         */
        public Builder includeMetaProperties(final boolean includeMetaProperties) {
            this.includeMetaProperties = includeMetaProperties;
            return this;
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

            return new PartitionStrategy(this);
        }
    }
}
