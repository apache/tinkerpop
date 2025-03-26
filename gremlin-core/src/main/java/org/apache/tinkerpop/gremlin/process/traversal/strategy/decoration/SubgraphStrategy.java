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

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
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
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
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
    private final boolean checkAdjacentVertices;

    private static final Set<Class<? extends DecorationStrategy>> POSTS = Collections.singleton(ConnectiveStrategy.class);

    private final String MARKER = Graph.Hidden.hide("gremlin.subgraphStrategy");

    private SubgraphStrategy(final Builder builder) {

        this.vertexCriterion = null == builder.vertexCriterion ? null : builder.vertexCriterion.asAdmin().clone();
        this.edgeCriterion = null == builder.edgeCriterion ? null : builder.edgeCriterion.asAdmin().clone();
        this.checkAdjacentVertices = builder.checkAdjacentVertices;

        this.vertexPropertyCriterion = null == builder.vertexPropertyCriterion ? null : builder.vertexPropertyCriterion.asAdmin().clone();

        if (null != this.vertexCriterion)
            TraversalHelper.applyTraversalRecursively(t -> t.getStartStep().addLabel(MARKER), this.vertexCriterion);
        if (null != this.edgeCriterion)
            TraversalHelper.applyTraversalRecursively(t -> t.getStartStep().addLabel(MARKER), this.edgeCriterion);
        if (null != this.vertexPropertyCriterion)
            TraversalHelper.applyTraversalRecursively(t -> t.getStartStep().addLabel(MARKER), this.vertexPropertyCriterion);
    }

    private static void applyCriterion(final List<Step> stepsToApplyCriterionAfter, final Traversal.Admin traversal,
                                       final Function<Step<?,?>, Optional<Traversal.Admin<? extends Element, ?>>> criterionMaker) {
        for (final Step<?, ?> step : stepsToApplyCriterionAfter) {
            // re-assign the step label to the criterion because the label should apply seamlessly after the filter
            final Optional<Traversal.Admin<? extends Element, ?>> crit = criterionMaker.apply(step);
            if (crit.isPresent()) {
                final Step filter = new TraversalFilterStep<>(traversal, crit.get());
                TraversalHelper.insertAfterStep(filter, step, traversal);
                TraversalHelper.copyLabels(step, filter, true);
            }
        }
    }

    private static char processesPropertyType(Step step) {
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

        final List<GraphStep> graphSteps = TraversalHelper.getStepsOfAssignableClass(GraphStep.class, traversal);
        final List<VertexStep> vertexSteps = TraversalHelper.getStepsOfAssignableClass(VertexStep.class, traversal);
        if (null != this.vertexCriterion) {
            final List<Step> vertexStepsToInsertFilterAfter = new ArrayList<>();
            vertexStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeOtherVertexStep.class, traversal));
            vertexStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeVertexStep.class, traversal));
            vertexStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(AddVertexStep.class, traversal));
            vertexStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(AddVertexStartStep.class, traversal));
            vertexStepsToInsertFilterAfter.addAll(graphSteps.stream().filter(GraphStep::returnsVertex).collect(Collectors.toList()));
            vertexStepsToInsertFilterAfter.addAll(vertexSteps.stream().filter(VertexStep::returnsVertex).collect(Collectors.toList()));
            applyCriterion(vertexStepsToInsertFilterAfter, traversal, s -> Optional.of(this.vertexCriterion.clone()));
        }

        if (null != this.edgeCriterion || checkAdjacentVertices) {
            final List<Step> edgeStepsToInsertFilterAfter = new ArrayList<>();
            edgeStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(AddEdgeStep.class, traversal));
            edgeStepsToInsertFilterAfter.addAll(graphSteps.stream().filter(GraphStep::returnsEdge).collect(Collectors.toList()));
            edgeStepsToInsertFilterAfter.addAll(vertexSteps.stream().filter(VertexStep::returnsEdge).collect(Collectors.toList()));
            applyCriterion(edgeStepsToInsertFilterAfter, traversal, s -> {
                if (checkAdjacentVertices && this.vertexCriterion != null) {
                    if (s instanceof VertexStep) {
                        // based on the directionality of the step choose the appropriate filter direction to apply.
                        // we scan skip AddEdgeStep, because its vertices must be in the Subgraph for it to even work.
                        final Direction d = ((VertexStep) s).getDirection();
                        final Traversal.Admin<Edge, ? extends Element> vertexPredicate;
                        vertexPredicate = getVertexPredicateGivenDirection(d);
                        return applyVertexPredicate(vertexPredicate);
                    } else if (s instanceof GraphStep) {
                        // for E() we need to test both incident vertices as there is no directionality
                        final Traversal.Admin<Edge, ? extends Element> vertexPredicate = __.<Edge>and(
                                __.inV().filter(this.vertexCriterion.clone()),
                                __.outV().filter(this.vertexCriterion.clone())).asAdmin();
                        return applyVertexPredicate(vertexPredicate);
                    }
                }

                if (null == edgeCriterion)
                    return Optional.empty();
                else {
                    final Traversal.Admin<Edge, ?> ec = edgeCriterion.clone();
                    TraversalHelper.applyTraversalRecursively(t -> t.getStartStep().addLabel(MARKER), ec);
                    return Optional.of(ec);
                }
            });
        }

        // turn g.V().out() to g.V().outE().inV() only if there is an edge predicate
        for (final VertexStep<?> step : vertexSteps) {
            if (step.returnsEdge())
                continue;

            if (edgeCriterion != null) {
                final VertexStep<Edge> someEStep = new VertexStep<>(traversal, Edge.class, step.getDirection(), step.getEdgeLabelsGValue());
                final Step<Edge, Vertex> someVStep = step.getDirection() == Direction.BOTH ?
                        new EdgeOtherVertexStep(traversal) :
                        new EdgeVertexStep(traversal, step.getDirection().opposite());

                TraversalHelper.replaceStep((Step<Vertex, Edge>) step, someEStep, traversal);
                TraversalHelper.insertAfterStep(someVStep, someEStep, traversal);
                TraversalHelper.copyLabels(step, someVStep, true);

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
                    new DefaultTraversal<>().addStep(new TraversalFilterStep<>(traversal, this.vertexPropertyCriterion)));
            final Traversal.Admin nonCheckPropertyCriterion = new DefaultTraversal<>().addStep(new TraversalFilterStep<>(traversal, this.vertexPropertyCriterion));

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
                                .filter(t -> t instanceof ValueTraversal)
                                .forEach(t -> {
                                    final char propertyType = processesPropertyType(step.getPreviousStep());
                                    if ('p' != propertyType) {
                                        final Traversal.Admin<?, ?> temp = new DefaultTraversal<>();
                                        temp.addStep(new PropertiesStep<>(temp, PropertyType.PROPERTY, ((ValueTraversal) t).getPropertyKey()));
                                        if ('v' == propertyType)
                                            TraversalHelper.insertTraversal(0, nonCheckPropertyCriterion.clone(), temp);
                                        else
                                            temp.addStep(checkPropertyCriterion.clone());
                                        temp.addStep(new PropertyValueStep<>(temp));
                                        temp.setParent((TraversalParent) step);
                                        ((ValueTraversal) t).setBypassTraversal(temp);
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

    private Optional<Traversal.Admin<? extends Element, ?>> applyVertexPredicate(final Traversal.Admin<Edge, ? extends Element> vertexPredicate) {
        if (null == edgeCriterion) {
            TraversalHelper.applyTraversalRecursively(t -> t.getStartStep().addLabel(MARKER), vertexPredicate);
            return Optional.of(vertexPredicate);
        } else {
            final Traversal.Admin<Edge, ?> ec = edgeCriterion.clone();
            ec.addStep(new TraversalFilterStep<>(ec, vertexPredicate));
            TraversalHelper.applyTraversalRecursively(t -> t.getStartStep().addLabel(MARKER), ec);
            return Optional.of(ec);
        }
    }

    private Traversal.Admin<Edge, Vertex> getVertexPredicateGivenDirection(final Direction d) {
        final Traversal.Admin<Edge, Vertex> vertexPredicate;
        if (d == Direction.OUT) {
            vertexPredicate = __.inV().filter(this.vertexCriterion.clone()).asAdmin();
        } else if (d == Direction.IN) {
            vertexPredicate = __.outV().filter(this.vertexCriterion.clone()).asAdmin();
        } else {
            vertexPredicate = __.otherV().filter(this.vertexCriterion.clone()).asAdmin();
        }
        return vertexPredicate;
    }

    @Override
    public Configuration getConfiguration() {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put(CHECK_ADJACENT_VERTICES, this.checkAdjacentVertices);
        if (null != this.vertexCriterion)
            map.put(VERTICES, this.vertexCriterion);
        if (null != this.edgeCriterion)
            map.put(EDGES, this.edgeCriterion);
        if (null != this.vertexPropertyCriterion)
            map.put(VERTEX_PROPERTIES, this.vertexPropertyCriterion);
        return new MapConfiguration(map);
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

    public static final String VERTICES = "vertices";
    public static final String EDGES = "edges";
    public static final String VERTEX_PROPERTIES = "vertexProperties";
    public static final String CHECK_ADJACENT_VERTICES = "checkAdjacentVertices";

    public static SubgraphStrategy create(final Configuration configuration) {
        final Builder builder = SubgraphStrategy.build();
        if (configuration.containsKey(VERTICES))
            builder.vertices((Traversal) configuration.getProperty(VERTICES));
        if (configuration.containsKey(EDGES))
            builder.edges((Traversal) configuration.getProperty(EDGES));
        if (configuration.containsKey(VERTEX_PROPERTIES))
            builder.vertexProperties((Traversal) configuration.getProperty(VERTEX_PROPERTIES));
        if (configuration.containsKey(CHECK_ADJACENT_VERTICES))
            builder.checkAdjacentVertices(configuration.getBoolean(CHECK_ADJACENT_VERTICES));
        return builder.create();
    }

    public static Builder build() {
        return new Builder();
    }

    public final static class Builder {

        private Traversal<Vertex, ?> vertexCriterion = null;
        private Traversal<Edge, ?> edgeCriterion = null;
        private Traversal<VertexProperty, ?> vertexPropertyCriterion = null;
        private boolean checkAdjacentVertices = true;

        private Builder() {
        }

        /**
         * Enables the strategy to apply the {@link #vertices(Traversal)} filter to the adjacent vertices of an edge.
         * If using this strategy for OLAP then this value should be set to {@code false} as checking adjacent vertices
         * will force the traversal to leave the local star graph (which is not possible in OLAP) and will cause an
         * error. By default, this value is {@code true}.
         */
        public Builder checkAdjacentVertices(final boolean enable) {
            this.checkAdjacentVertices = enable;
            return this;
        }

        /**
         * The traversal predicate that defines the vertices to include in the subgraph. If
         * {@link #checkAdjacentVertices(boolean)} is {@code true} then this predicate will also be applied to the
         * adjacent vertices of edges. Take care when setting this value for OLAP based traversals as the traversal
         * predicate cannot be written in such a way as to leave the local star graph and can thus only evaluate the
         * current vertex and its related edges.
         */
        public Builder vertices(final Traversal<Vertex, ?> vertexPredicate) {
            this.vertexCriterion = vertexPredicate;
            return this;
        }

        /**
         * The traversal predicate that defines the edges to include in the subgraph.
         */
        public Builder edges(final Traversal<Edge, ?> edgePredicate) {
            this.edgeCriterion = edgePredicate;
            return this;
        }

        /**
         * The traversal predicate that defines the vertex properties to include in the subgraph.
         */
        public Builder vertexProperties(final Traversal<VertexProperty, ?> vertexPropertyPredicate) {
            this.vertexPropertyCriterion = vertexPropertyPredicate;
            return this;
        }

        public SubgraphStrategy create() {
            if (null == this.vertexCriterion && null == this.edgeCriterion && null == this.vertexPropertyCriterion)
                throw new IllegalStateException("A subgraph must be filtered by a vertex, edge, or vertex property criterion");
            return new SubgraphStrategy(this);
        }
    }
}