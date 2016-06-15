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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This {@link TraversalStrategy} provides a way to limit the view of a {@link Traversal}.  By providing
 * {@link Traversal} representations that represent a form of filtering criterion for vertices and/or edges,
 * this strategy will inject that criterion into the appropriate places of a traversal thus restricting what
 * it traverses and returns.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class SubgraphStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy>
        implements TraversalStrategy.DecorationStrategy {

    private final Traversal<Vertex, ?> vertexCriterion;
    private final Traversal<Edge, ?> edgeCriterion;

    private SubgraphStrategy(final Traversal<Vertex, ?> vertexCriterion, final Traversal<Edge, ?> edgeCriterion) {
        this.vertexCriterion = vertexCriterion;

        // if there is no vertex predicate there is no need to test either side of the edge
        if (null == vertexCriterion) {
            this.edgeCriterion = edgeCriterion;
        } else {
            final Traversal<Object, Vertex> inVertexPredicate = __.inV().filter(vertexCriterion);
            final Traversal<Object, Vertex> outVertexPredicate = __.outV().filter(vertexCriterion);

            // if there is a vertex predicate then there is an implied edge filter on vertices even if there is no
            // edge predicate provided by the user.
            if (null == edgeCriterion)
                this.edgeCriterion = __.and(inVertexPredicate.asAdmin(), outVertexPredicate.asAdmin());
            else
                this.edgeCriterion = edgeCriterion.asAdmin().addStep(new TraversalFilterStep<>(edgeCriterion.asAdmin(), __.and(inVertexPredicate.asAdmin(), outVertexPredicate.asAdmin())));
        }
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final List<GraphStep> graphSteps = TraversalHelper.getStepsOfAssignableClass(GraphStep.class, traversal);
        final List<VertexStep> vertexSteps = TraversalHelper.getStepsOfAssignableClass(VertexStep.class, traversal);

        if (vertexCriterion != null) {
            final List<Step> vertexStepsToInsertFilterAfter = new ArrayList<>();
            vertexStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeOtherVertexStep.class, traversal));
            vertexStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeVertexStep.class, traversal));
            vertexStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(AddVertexStep.class, traversal));
            vertexStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(AddVertexStartStep.class, traversal));
            vertexStepsToInsertFilterAfter.addAll(graphSteps.stream().filter(GraphStep::returnsVertex).collect(Collectors.toList()));

            applyCriterion(vertexStepsToInsertFilterAfter, traversal, vertexCriterion.asAdmin());
        }

        if (edgeCriterion != null) {
            final List<Step> edgeStepsToInsertFilterAfter = new ArrayList<>();
            edgeStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(AddEdgeStep.class, traversal));
            edgeStepsToInsertFilterAfter.addAll(graphSteps.stream().filter(GraphStep::returnsEdge).collect(Collectors.toList()));
            edgeStepsToInsertFilterAfter.addAll(vertexSteps.stream().filter(VertexStep::returnsEdge).collect(Collectors.toList()));

            applyCriterion(edgeStepsToInsertFilterAfter, traversal, edgeCriterion.asAdmin());
        }

        // explode g.V().out() to g.V().outE().inV() only if there is an edge predicate otherwise
        vertexSteps.stream().filter(VertexStep::returnsVertex).forEach(s -> {
            if (null == edgeCriterion)
                TraversalHelper.insertAfterStep(new TraversalFilterStep<>(traversal, vertexCriterion.asAdmin().clone()), s, traversal);
            else {
                final VertexStep someEStep = new VertexStep(traversal, Edge.class, s.getDirection(), s.getEdgeLabels());
                final Step someVStep = (s.getDirection() == Direction.BOTH) ?
                        new EdgeOtherVertexStep(traversal) : new EdgeVertexStep(traversal, s.getDirection().opposite());

                // if s was labelled then propagate those labels to the new step that will return the vertex
                transferLabels(s, someVStep);

                TraversalHelper.replaceStep(s, someEStep, traversal);
                TraversalHelper.insertAfterStep(someVStep, someEStep, traversal);
                TraversalHelper.insertAfterStep(new TraversalFilterStep<>(traversal, edgeCriterion.asAdmin().clone()), someEStep, traversal);

                if (vertexCriterion != null)
                    TraversalHelper.insertAfterStep(new TraversalFilterStep<>(traversal, vertexCriterion.asAdmin().clone()), someVStep, traversal);
            }
        });
    }

    public Traversal<Vertex, ?> getVertexCriterion() {
        return vertexCriterion;
    }

    public Traversal<Edge, ?> getEdgeCriterion() {
        return edgeCriterion;
    }

    public static Builder build() {
        return new Builder();
    }

    private void applyCriterion(final List<Step> stepsToApplyCriterionAfter, final Traversal.Admin traversal,
                                final Traversal.Admin<? extends Element, ?> criterion) {
        stepsToApplyCriterionAfter.forEach(s -> {
            // re-assign the step label to the criterion because the label should apply seamlessly after the filter
            final Step filter = new TraversalFilterStep<>(traversal, criterion.clone());
            transferLabels(s, filter);
            TraversalHelper.insertAfterStep(filter, s, traversal);
        });
    }

    private static void transferLabels(final Step from, final Step to) {
        from.getLabels().forEach(label -> to.addLabel((String) label));
        to.getLabels().forEach(label -> from.removeLabel((String) label));
    }

    public final static class Builder {

        private Traversal<Vertex, ?> vertexCriterion = null;
        private Traversal<Edge, ?> edgeCriterion = null;

        private Builder() {
        }

        public Builder vertexCriterion(final Traversal<Vertex, ?> predicate) {
            vertexCriterion = predicate;
            return this;
        }

        public Builder edgeCriterion(final Traversal<Edge, ?> predicate) {
            edgeCriterion = predicate;
            return this;
        }

        public SubgraphStrategy create() {
            if (null == edgeCriterion && null == vertexCriterion)
                throw new IllegalStateException("A subgraph must be filtered by an edge or vertex criterion");
            return new SubgraphStrategy(vertexCriterion, edgeCriterion);
        }
    }
}
