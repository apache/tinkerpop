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
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.LambdaFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeByPathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SubgraphStrategy extends AbstractTraversalStrategy implements TraversalStrategy.DecorationStrategy {

    private final Predicate<Traverser<Vertex>> vertexPredicate;
    private final Predicate<Traverser<Edge>> edgePredicate;

    private SubgraphStrategy(final Predicate<Vertex> vertexPredicate, final Predicate<Edge> edgePredicate) {
        this.vertexPredicate = t -> vertexPredicate.test(t.get());
        this.edgePredicate = t -> edgePredicate.test(t.get()) && vertexPredicate.test(t.get().inVertex()) && vertexPredicate.test(t.get().outVertex());
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final List<GraphStep> graphSteps = TraversalHelper.getStepsOfAssignableClass(GraphStep.class, traversal);
        final List<VertexStep> vertexSteps = TraversalHelper.getStepsOfAssignableClass(VertexStep.class, traversal);

        final List<Step> vertexStepsToInsertFilterAfter = new ArrayList<>();
        vertexStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeOtherVertexStep.class, traversal));
        vertexStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeVertexStep.class, traversal));
        vertexStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(AddVertexStep.class, traversal));
        vertexStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(AddVertexStartStep.class, traversal));
        vertexStepsToInsertFilterAfter.addAll(graphSteps.stream().filter(s -> s.getReturnClass().equals(Vertex.class)).collect(Collectors.toList()));

        vertexStepsToInsertFilterAfter.forEach(s -> TraversalHelper.insertAfterStep(new LambdaFilterStep(traversal, vertexPredicate), s, traversal));

        final List<Step> edgeStepsToInsertFilterAfter = new ArrayList<>();
        edgeStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(AddEdgeStep.class, traversal));
        edgeStepsToInsertFilterAfter.addAll(TraversalHelper.getStepsOfAssignableClass(AddEdgeByPathStep.class, traversal));
        edgeStepsToInsertFilterAfter.addAll(graphSteps.stream().filter(s -> s.getReturnClass().equals(Edge.class)).collect(Collectors.toList()));
        edgeStepsToInsertFilterAfter.addAll(vertexSteps.stream().filter(s -> s.getReturnClass().equals(Edge.class)).collect(Collectors.toList()));

        edgeStepsToInsertFilterAfter.forEach(s -> TraversalHelper.insertAfterStep(new LambdaFilterStep(traversal, edgePredicate), s, traversal));

        // explode g.V().out() to g.V().outE().inV()
        vertexSteps.stream().filter(s -> s.getReturnClass().equals(Vertex.class)).forEach(s -> {
            final VertexStep replacementVertexStep = new VertexStep(traversal, Edge.class, s.getDirection(), s.getEdgeLabels());
            Step intermediateFilterStep = null;
            if (s.getDirection() == Direction.BOTH)
                intermediateFilterStep = new EdgeOtherVertexStep(traversal);
            else
                intermediateFilterStep = new EdgeVertexStep(traversal, s.getDirection().opposite());

            TraversalHelper.replaceStep(s, replacementVertexStep, traversal);
            TraversalHelper.insertAfterStep(intermediateFilterStep, replacementVertexStep, traversal);
            TraversalHelper.insertAfterStep(new LambdaFilterStep(traversal, edgePredicate), replacementVertexStep, traversal);
            TraversalHelper.insertAfterStep(new LambdaFilterStep(traversal, vertexPredicate), intermediateFilterStep, traversal);
        });
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {

        private Predicate<Vertex> vertexPredicate = v -> true;
        private Predicate<Edge> edgePredicate = v -> true;

        private Builder() {}

        public Builder vertexPredicate(final Predicate<Vertex> predicate) {
            vertexPredicate = predicate;
            return this;
        }

        public Builder edgePredicate(final Predicate<Edge> predicate) {
            edgePredicate = predicate;
            return this;
        }

        public SubgraphStrategy create() {
            return new SubgraphStrategy(vertexPredicate, edgePredicate);
        }
    }
}
