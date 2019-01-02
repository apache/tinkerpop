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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.Parameterizing;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * {@code PartitionStrategy} partitions the vertices, edges and vertex properties of a graph into String named
 * partitions (i.e. buckets, subgraphs, etc.).  It blinds a {@link Traversal} from "seeing" specified areas of
 * the graph given the partition names assigned to {@link Builder#addReadPartition(String)}.  The traversal will
 * ignore all graph elements not in those "read" partitions.
 *
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
        this.includeMetaProperties = builder.includeMetaProperties;
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
        final Graph graph = traversal.getGraph().orElseThrow(() -> new IllegalStateException("PartitionStrategy does not work with anonymous Traversals"));
        final Graph.Features.VertexFeatures vertexFeatures = graph.features().vertex();
        final boolean supportsMetaProperties = vertexFeatures.supportsMetaProperties();
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
                // check length first because keyExists will return true otherwise
                if (step.getPropertyKeys().length > 0 && ElementHelper.keyExists(partitionKey, step.getPropertyKeys()))
                    throw new IllegalStateException("Cannot explicitly request the partitionKey in the traversal");

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
                                __.__()).filter(new PartitionKeyHider());
                        TraversalHelper.insertTraversal(step, choose.asAdmin(), traversal);
                    }
                } else if (step.getReturnType() == PropertyType.VALUE) {
                    // use choose() to determine if the values() step is called on a Vertex to get a VertexProperty
                    // if not, pass it through otherwise explode g.V().values() to g.V().properties().has().value()
                    final Traversal choose = __.choose(
                            __.filter(new TypeChecker<>(Vertex.class)),
                            __.properties(step.getPropertyKeys()).has(partitionKey, P.within(new ArrayList<>(readPartitions))).filter(new PartitionKeyHider()).value(),
                            __.__().filter(new PartitionKeyHider()));
                    TraversalHelper.insertTraversal(step, choose.asAdmin(), traversal);
                    traversal.removeStep(step);
                } else {
                    throw new IllegalStateException(String.format("%s is not accounting for a particular %s %s",
                            PartitionStrategy.class.getSimpleName(), PropertyType.class.toString(), step.getReturnType()));
                }
            });

            final List<PropertyMapStep> propertyMapSteps = TraversalHelper.getStepsOfAssignableClass(PropertyMapStep.class, traversal);
            propertyMapSteps.forEach(step -> {
                // check length first because keyExists will return true otherwise
                if (step.getPropertyKeys().length > 0 && ElementHelper.keyExists(partitionKey, step.getPropertyKeys()))
                    throw new IllegalStateException("Cannot explicitly request the partitionKey in the traversal");

                if (step.getReturnType() == PropertyType.PROPERTY) {
                    // via map() filter out properties that aren't in the partition if it is a PropertyVertex,
                    // otherwise just let them pass through
                    TraversalHelper.insertAfterStep(new LambdaMapStep<>(traversal, new MapPropertiesFilter()), step, traversal);
                } else if (step.getReturnType() == PropertyType.VALUE) {
                    // as this is a value map, replace that step with propertiesMap() that returns PropertyType.VALUE.
                    // from there, add the filter as shown above and then unwrap the properties as they would have
                    // been done under valueMap()
                    final PropertyMapStep propertyMapStep = new PropertyMapStep(traversal, PropertyType.PROPERTY, step.getPropertyKeys());
                    propertyMapStep.configure(WithOptions.tokens, step.getIncludedTokens());
                    TraversalHelper.replaceStep(step, propertyMapStep, traversal);

                    final LambdaMapStep mapPropertiesFilterStep = new LambdaMapStep<>(traversal, new MapPropertiesFilter());
                    TraversalHelper.insertAfterStep(mapPropertiesFilterStep, propertyMapStep, traversal);
                    TraversalHelper.insertAfterStep(new LambdaMapStep<>(traversal, new MapPropertiesConverter()), mapPropertiesFilterStep, traversal);
                } else {
                    throw new IllegalStateException(String.format("%s is not accounting for a particular %s %s",
                            PartitionStrategy.class.getSimpleName(), PropertyType.class.toString(), step.getReturnType()));
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
            ((Mutating) step).configure(partitionKey, writePartition);

            if (includeMetaProperties) {
                // GraphTraversal folds g.addV().property('k','v') to just AddVertexStep/AddVertexStartStep so this
                // has to be exploded back to g.addV().property(cardinality, 'k','v','partition','A')
                if (step instanceof AddVertexStartStep || step instanceof AddVertexStep) {
                    final Parameters parameters = ((Parameterizing) step).getParameters();
                    final Map<Object, List<Object>> params = parameters.getRaw();
                    params.forEach((k, v) -> {
                        final List<Step> addPropertyStepsToAppend = new ArrayList<>(v.size());
                        final VertexProperty.Cardinality cardinality = vertexFeatures.getCardinality((String) k);
                        v.forEach(o -> {
                            final AddPropertyStep addPropertyStep = new AddPropertyStep(traversal, cardinality, k, o);
                            addPropertyStep.configure(partitionKey, writePartition);
                            addPropertyStepsToAppend.add(addPropertyStep);

                            // need to remove the parameter from the AddVertex/StartStep because it's now being added
                            // via the AddPropertyStep
                            parameters.remove(k);
                        });

                        Collections.reverse(addPropertyStepsToAppend);
                        addPropertyStepsToAppend.forEach(s -> TraversalHelper.insertAfterStep(s, step, traversal));
                    });
                }
            }
        });
    }

    /**
     * A concrete lambda implementation that checks if the type passing through on the {@link Traverser} is
     * of a specific {@link Element} type.
     */
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

    /**
     * A concrete lambda implementation that filters out the partition key so that it isn't visible when making
     * calls to {@link GraphTraversal#valueMap}.
     */
    public final class PartitionKeyHider<A extends Property> implements Predicate<Traverser<A>>, Serializable {
        @Override
        public boolean test(final Traverser<A> traverser) {
            return !traverser.get().key().equals(partitionKey);
        }

        @Override
        public String toString() {
            return "remove(" + partitionKey + ")";
        }
    }

    /**
     * Takes the result of a {@link Map} containing {@link Property} lists and if the property is a
     * {@link VertexProperty} it applies a filter based on the current partitioning.  If is not a
     * {@link VertexProperty} the property is simply passed through.
     */
    public final class MapPropertiesFilter implements Function<Traverser<Map<String, List<Property>>>, Map<String, List<Property>>>, Serializable {
        @Override
        public Map<String, List<Property>> apply(final Traverser<Map<String, List<Property>>> mapTraverser) {
            final Map<String, List<Property>> values = mapTraverser.get();
            final Map<String, List<Property>> filtered = new HashMap<>();

            // note the final filter that removes the partitionKey from the outgoing Map
            values.entrySet().forEach(p -> {
                final List l = p.getValue().stream().filter(property -> {
                    if (property instanceof VertexProperty) {
                        final Iterator<String> itty = ((VertexProperty) property).values(partitionKey);
                        return itty.hasNext() && readPartitions.contains(itty.next());
                    } else {
                        return true;
                    }
                }).filter(property -> !property.key().equals(partitionKey)).collect(Collectors.toList());
                if (l.size() > 0) filtered.put(p.getKey(), l);
            });

            return filtered;
        }

        @Override
        public String toString() {
            return "applyPartitionFilter";
        }
    }

    /**
     * Takes a {@link Map} of a {@link List} of {@link Property} objects and unwraps the {@link Property#value()}.
     */
    public final class MapPropertiesConverter implements Function<Traverser<Map<String, List<Property>>>, Map<String, List<Property>>>, Serializable {
        @Override
        public Map<String, List<Property>> apply(final Traverser<Map<String, List<Property>>> mapTraverser) {
            final Map<String, List<Property>> values = mapTraverser.get();
            final Map<String, List<Property>> converted = new HashMap<>();

            values.entrySet().forEach(p -> {
                final List l = p.getValue().stream().map(Property::value).collect(Collectors.toList());
                converted.put(p.getKey(), l);
            });

            return converted;
        }

        @Override
        public String toString() {
            return "extractValuesInPropertiesMap";
        }
    }

    @Override
    public Configuration getConfiguration() {
        final Map<String, Object> map = new HashMap<>();
        map.put(STRATEGY, PartitionStrategy.class.getCanonicalName());
        map.put(INCLUDE_META_PROPERTIES, this.includeMetaProperties);
        if (null != this.writePartition)
            map.put(WRITE_PARTITION, this.writePartition);
        if (null != this.readPartitions)
            map.put(READ_PARTITIONS, this.readPartitions);
        if (null != this.partitionKey)
            map.put(PARTITION_KEY, this.partitionKey);
        return new MapConfiguration(map);
    }

    public static final String INCLUDE_META_PROPERTIES = "includeMetaProperties";
    public static final String WRITE_PARTITION = "writePartition";
    public static final String PARTITION_KEY = "partitionKey";
    public static final String READ_PARTITIONS = "readPartitions";

    public static PartitionStrategy create(final Configuration configuration) {
        final PartitionStrategy.Builder builder = PartitionStrategy.build();
        if (configuration.containsKey(INCLUDE_META_PROPERTIES))
            builder.includeMetaProperties(configuration.getBoolean(INCLUDE_META_PROPERTIES));
        if (configuration.containsKey(WRITE_PARTITION))
            builder.writePartition(configuration.getString(WRITE_PARTITION));
        if (configuration.containsKey(PARTITION_KEY))
            builder.partitionKey(configuration.getString(PARTITION_KEY));
        if (configuration.containsKey(READ_PARTITIONS))
            builder.readPartitions(new ArrayList((Collection)configuration.getProperty(READ_PARTITIONS)));
        return builder.create();
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

        /**
         * Specifies the name of the partition to write when adding vertices, edges and vertex properties.  This
         * name can be any user defined value.  It is only possible to write to a single partition at a time.
         */
        public Builder writePartition(final String writePartition) {
            this.writePartition = writePartition;
            return this;
        }

        /**
         * Specifies the partition key name.  This is the property key that contains the partition value. It
         * may a good choice to index on this key in certain cases (in graphs that support such things). This
         * value must be specified for the {@code PartitionStrategy} to be constructed properly.
         */
        public Builder partitionKey(final String partitionKey) {
            this.partitionKey = partitionKey;
            return this;
        }

        /**
         * Specifies the partition of the graph to read from.  It is possible to assign multiple partition keys so
         * as to read from multiple partitions at the same time.
         */
        public Builder readPartitions(final List<String> readPartitions) {
            this.readPartitions.addAll(readPartitions);
            return this;
        }

        /**
         * Specifies the partition of the graph to read from.  It is possible to assign multiple partition keys so
         * as to read from multiple partitions at the same time.
         */
        public Builder readPartitions(final String... readPartitions) {
            return this.readPartitions(Arrays.asList(readPartitions));
        }

        /**
         * Creates the {@code PartitionStrategy}.
         */
        public PartitionStrategy create() {
            if (partitionKey == null || partitionKey.isEmpty())
                throw new IllegalStateException("The partitionKey cannot be null or empty");

            return new PartitionStrategy(this);
        }
    }
}
