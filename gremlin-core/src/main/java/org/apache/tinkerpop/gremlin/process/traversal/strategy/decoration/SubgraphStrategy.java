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
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ClassFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This {@link TraversalStrategy} provides a way to limit the view of a {@link Traversal}.  By providing
 * {@link Traversal} representations that represent a form of filtering criterion for vertices and/or edges,
 * this strategy will inject that criterion into the appropriate places of a traversal thus restricting what
 * it traverses and returns.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SubgraphStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy>
        implements TraversalStrategy.DecorationStrategy {

    private final Traversal.Admin<Vertex, ?> vertexCriterion;
    private final Traversal.Admin<Edge, ?> edgeCriterion;
    private final Traversal.Admin<VertexProperty, ?> vertexPropertyCriterion;

    private static final Set<Class<? extends DecorationStrategy>> POSTS = Collections.singleton(ConnectiveStrategy.class);

    private final String MARKER = Graph.Hidden.hide(UUID.randomUUID().toString());

    private SubgraphStrategy(final Traversal<Vertex, ?> vertexCriterion, final Traversal<Edge, ?> edgeCriterion, final Traversal<VertexProperty, ?> vertexPropertyCriterion) {

        this.vertexCriterion = null == vertexCriterion ? null : vertexCriterion.asAdmin();

        // if there is no vertex predicate there is no need to test either side of the edge
        if (null == this.vertexCriterion) {
            this.edgeCriterion = null == edgeCriterion ? null : edgeCriterion.asAdmin();
        } else {
            final Traversal.Admin<Edge, ?> vertexPredicate;
            vertexPredicate = __.<Edge>and(
                    __.inV().filter(this.vertexCriterion.clone()),
                    __.outV().filter(this.vertexCriterion.clone())).asAdmin();

            // if there is a vertex predicate then there is an implied edge filter on vertices even if there is no
            // edge predicate provided by the user.
            this.edgeCriterion = null == edgeCriterion ?
                    vertexPredicate :
                    edgeCriterion.asAdmin().addStep(new TraversalFilterStep<>(edgeCriterion.asAdmin(), vertexPredicate));
        }

        this.vertexPropertyCriterion = null == vertexPropertyCriterion ? null : vertexPropertyCriterion.asAdmin();

        if (null != this.vertexCriterion)
            this.addLabelMarker(this.vertexCriterion);
        if (null != this.edgeCriterion)
            this.addLabelMarker(this.edgeCriterion);
        if (null != this.vertexPropertyCriterion)
            this.addLabelMarker(this.vertexPropertyCriterion);
    }

    private final void addLabelMarker(final Traversal.Admin<?, ?> traversal) {
        traversal.getStartStep().addLabel(MARKER);
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (step instanceof TraversalParent) {
                ((TraversalParent) step).getLocalChildren().forEach(this::addLabelMarker);
                ((TraversalParent) step).getGlobalChildren().forEach(this::addLabelMarker);
            }
        }
    }

    private static void applyCriterion(final List<Step> stepsToApplyCriterionAfter, final Traversal.Admin traversal,
                                       final Traversal.Admin<? extends Element, ?> criterion) {
        for (final Step<?, ?> step : stepsToApplyCriterionAfter) {
            // re-assign the step label to the criterion because the label should apply seamlessly after the filter
            final Step filter = new TraversalFilterStep<>(traversal, criterion.clone());
            TraversalHelper.insertAfterStep(filter, step, traversal);
            TraversalHelper.copyLabels(step, filter, true);
        }
    }

    private static final char processesPropertyType(Step step) {
        while (!(step instanceof EmptyStep)) {
            if (step instanceof FilterStep || step instanceof SideEffectStep)
                step = step.getPreviousStep();
            else if (step instanceof GraphStep && ((GraphStep) step).returnsVertex())
                return 'v';
            else if (step instanceof EdgeVertexStep)
                return 'v';
            else if (step instanceof VertexStep)
                return ((VertexStep) step).returnsVertex() ? 'v' : 'p';
            else if (step instanceof PropertyMapStep || step instanceof PropertiesStep)
                return 'p';
            else
                return 'x';
        }
        return 'x';
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // do not apply subgraph strategy to already created subgraph filter branches (or else you get infinite recursion)
        if (traversal.getStartStep().getLabels().contains(MARKER)) {
            traversal.getStartStep().removeLabel(MARKER);
            return;
        }
        //
        final List<GraphStep> graphSteps = TraversalHelper.getStepsOfAssignableClass(GraphStep.class, traversal);
        final List<VertexStep> vertexSteps = TraversalHelper.getStepsOfAssignableClass(VertexStep.class, traversal);
        if (null != this.vertexCriterion) {
            final List<Step> vertexStepsToInsertFilterAfter = new ArrayList<>();
            vertexStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeOtherVertexStep.class, traversal));
            vertexStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeVertexStep.class, traversal));
            vertexStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(AddVertexStep.class, traversal));
            vertexStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(AddVertexStartStep.class, traversal));
            vertexStepsToInsertFilterAfter.addAll(graphSteps.stream().filter(GraphStep::returnsVertex).collect(Collectors.toList()));
            applyCriterion(vertexStepsToInsertFilterAfter, traversal, this.vertexCriterion);
        }

        if (null != this.edgeCriterion) {
            final List<Step> edgeStepsToInsertFilterAfter = new ArrayList<>();
            edgeStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(AddEdgeStep.class, traversal));
            edgeStepsToInsertFilterAfter.addAll(graphSteps.stream().filter(GraphStep::returnsEdge).collect(Collectors.toList()));
            edgeStepsToInsertFilterAfter.addAll(vertexSteps.stream().filter(VertexStep::returnsEdge).collect(Collectors.toList()));
            applyCriterion(edgeStepsToInsertFilterAfter, traversal, this.edgeCriterion);
        }

        // turn g.V().out() to g.V().outE().inV() only if there is an edge predicate otherwise
        for (final VertexStep<?> step : vertexSteps) {
            if (step.returnsEdge())
                continue;
            if (null != this.vertexCriterion && null == edgeCriterion) {
                TraversalHelper.insertAfterStep(new TraversalFilterStep<>(traversal, (Traversal) this.vertexCriterion.clone()), step, traversal);
            } else {
                final VertexStep<Edge> someEStep = new VertexStep<>(traversal, Edge.class, step.getDirection(), step.getEdgeLabels());
                final Step<Edge, Vertex> someVStep = step.getDirection() == Direction.BOTH ?
                        new EdgeOtherVertexStep(traversal) :
                        new EdgeVertexStep(traversal, step.getDirection().opposite());

                TraversalHelper.replaceStep((Step<Vertex, Edge>) step, someEStep, traversal);
                TraversalHelper.insertAfterStep(someVStep, someEStep, traversal);
                TraversalHelper.copyLabels(step, someVStep, true);

                if (null != this.edgeCriterion)
                    TraversalHelper.insertAfterStep(new TraversalFilterStep<>(traversal, this.edgeCriterion.clone()), someEStep, traversal);
                if (null != this.vertexCriterion)
                    TraversalHelper.insertAfterStep(new TraversalFilterStep<>(traversal, this.vertexCriterion.clone()), someVStep, traversal);
            }
        }

        // turn g.V().properties() to g.V().properties().xxx
        // turn g.V().values() to g.V().properties().xxx.value()
        if (null != this.vertexPropertyCriterion) {
            final OrStep<Object> checkPropertyCriterion = new OrStep(traversal,
                    new DefaultTraversal<>().addStep(new ClassFilterStep<>(traversal, VertexProperty.class, false)),
                    new DefaultTraversal<>().addStep(new TraversalFilterStep<>(traversal, this.vertexPropertyCriterion.clone())));
            final Traversal.Admin nonCheckPropertyCriterion = new DefaultTraversal<>().addStep(new TraversalFilterStep<>(traversal, this.vertexPropertyCriterion.clone()));

            // turn all ElementValueTraversals into filters
            for (final Step<?, ?> step : traversal.getSteps()) {
                if (step instanceof TraversalParent) {
                    if (step instanceof PropertyMapStep) {
                        final char propertyType = processesPropertyType(step.getPreviousStep());
                        if ('p' != propertyType) {
                            final Traversal.Admin<?, ?> temp = new DefaultTraversal<>();
                            temp.addStep(new PropertiesStep<>(temp, PropertyType.PROPERTY, ((PropertyMapStep) step).getPropertyKeys()));
                            if ('v' == propertyType)
                                TraversalHelper.insertTraversal(0, nonCheckPropertyCriterion.clone(), temp);
                            else
                                temp.addStep(checkPropertyCriterion.clone());
                            ((PropertyMapStep) step).setPropertyTraversal(temp);
                        }
                    } else {
                        Stream.concat(((TraversalParent) step).getGlobalChildren().stream(), ((TraversalParent) step).getLocalChildren().stream())
                                .filter(t -> t instanceof ElementValueTraversal)
                                .forEach(t -> {
                                    final char propertyType = processesPropertyType(step.getPreviousStep());
                                    if ('p' != propertyType) {
                                        final Traversal.Admin<?, ?> temp = new DefaultTraversal<>();
                                        temp.addStep(new PropertiesStep<>(temp, PropertyType.PROPERTY, ((ElementValueTraversal) t).getPropertyKey()));
                                        if ('v' == propertyType)
                                            TraversalHelper.insertTraversal(0, nonCheckPropertyCriterion.clone(), temp);
                                        else
                                            temp.addStep(checkPropertyCriterion.clone());
                                        temp.addStep(new PropertyValueStep<>(temp));
                                        temp.setParent((TraversalParent) step);
                                        ((ElementValueTraversal) t).setBypassTraversal(temp);
                                    }
                                });
                    }
                }
            }
            for (final PropertiesStep<?> step : TraversalHelper.getStepsOfAssignableClass(PropertiesStep.class, traversal)) {
                final char propertyType = processesPropertyType(step.getPreviousStep());
                if ('p' != propertyType) {
                    if (PropertyType.PROPERTY == ((PropertiesStep) step).getReturnType()) {
                        // if the property step returns a property, then simply append the criterion
                        if ('v' == propertyType) {
                            final Traversal.Admin<?, ?> temp = nonCheckPropertyCriterion.clone();
                            TraversalHelper.insertTraversal((Step) step, temp, traversal);
                            TraversalHelper.copyLabels(step, temp.getEndStep(), true);
                        } else {
                            final Step<?, ?> temp = checkPropertyCriterion.clone();
                            TraversalHelper.insertAfterStep(temp, (Step) step, traversal);
                            TraversalHelper.copyLabels(step, temp, true);
                        }
                    } else {
                        // if the property step returns value, then replace it with a property step, append criterion, then append a value() step
                        final Step propertiesStep = new PropertiesStep(traversal, PropertyType.PROPERTY, ((PropertiesStep) step).getPropertyKeys());
                        TraversalHelper.replaceStep(step, propertiesStep, traversal);
                        final Step propertyValueStep = new PropertyValueStep(traversal);
                        TraversalHelper.copyLabels(step, propertyValueStep, false);
                        if ('v' == propertyType) {
                            TraversalHelper.insertAfterStep(propertyValueStep, propertiesStep, traversal);
                            TraversalHelper.insertTraversal(propertiesStep, nonCheckPropertyCriterion.clone(), traversal);
                        } else {
                            TraversalHelper.insertAfterStep(propertyValueStep, propertiesStep, traversal);
                            TraversalHelper.insertAfterStep(checkPropertyCriterion.clone(), propertiesStep, traversal);
                        }
                    }
                }
            }
        }
    }

    @Override
    public Set<Class<? extends DecorationStrategy>> applyPost() {
        return POSTS;
    }

    public Traversal<Vertex, ?> getVertexCriterion() {
        return this.vertexCriterion;
    }

    public Traversal<Edge, ?> getEdgeCriterion() {
        return this.edgeCriterion;
    }

    public Traversal<VertexProperty, ?> getVertexPropertyCriterion() {
        return this.vertexPropertyCriterion;
    }

    public static SubgraphStrategy create(final Configuration configuration) {
        final Builder builder = SubgraphStrategy.build();
        configuration.getKeys().forEachRemaining(key -> {
            if (key.equals("vertices") || key.equals("vertexCriterion"))
                builder.vertices((Traversal) configuration.getProperty(key));
            else if (key.equals("edges") || key.equals("edgeCriterion"))
                builder.edges((Traversal) configuration.getProperty(key));
            else if (key.equals("vertexProperties"))
                builder.vertexProperties((Traversal) configuration.getProperty(key));
            else
                throw new IllegalArgumentException("The following configuration is unknown: " + key + ":" + configuration.getProperty(key));
        });
        return builder.create();
    }

    public static Builder build() {
        return new Builder();
    }

    public final static class Builder {

        private Traversal<Vertex, ?> vertexPredicate = null;
        private Traversal<Edge, ?> edgePredicate = null;
        private Traversal<VertexProperty, ?> vertexPropertyPredicate = null;

        private Builder() {
        }

        public Builder vertices(final Traversal<Vertex, ?> vertexPredicate) {
            this.vertexPredicate = vertexPredicate;
            return this;
        }

        public Builder edges(final Traversal<Edge, ?> edgePredicate) {
            this.edgePredicate = edgePredicate;
            return this;
        }

        public Builder vertexProperties(final Traversal<VertexProperty, ?> vertexPropertyPredicate) {
            this.vertexPropertyPredicate = vertexPropertyPredicate;
            return this;
        }

        @Deprecated
        /**
         * @deprecated Since 3.2.2, use {@code Builder#vertices} instead.
         */
        public Builder vertexCriterion(final Traversal<Vertex, ?> predicate) {
            return this.vertices(predicate);
        }

        /**
         * @deprecated Since 3.2.2, use {@code Builder#edges} instead.
         */
        @Deprecated
        public Builder edgeCriterion(final Traversal<Edge, ?> predicate) {
            return this.edges(predicate);
        }

        public SubgraphStrategy create() {
            if (null == this.vertexPredicate && null == this.edgePredicate && null == this.vertexPropertyPredicate)
                throw new IllegalStateException("A subgraph must be filtered by a vertex, edge, or vertex property criterion");
            return new SubgraphStrategy(this.vertexPredicate, this.edgePredicate, this.vertexPropertyPredicate);
        }
    }
}