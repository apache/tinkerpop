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
package org.apache.tinkerpop.gremlin.console.plugin

import groovy.transform.CompileStatic
import org.apache.tinkerpop.gremlin.process.traversal.Step
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy
import org.apache.tinkerpop.gremlin.process.traversal.Traverser
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.UnfoldStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.LambdaSideEffectStep
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper
import org.apache.tinkerpop.gremlin.structure.Vertex

import java.util.stream.Collectors

/**
 * A strategy that works in conjuction with the {@link GephiRemoteAcceptor} to automatically inject visualization
 * steps after "vertex" steps to show the vertices traversed for a step.  If the traversal was evaluated in the
 * console normally then the visualization strategy will not be applied.  It must be {@code :submit} to the
 * console for the strategy to be applied.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@CompileStatic
class GephiTraversalVisualizationStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy>
        implements TraversalStrategy.DecorationStrategy {

    private final GephiRemoteAcceptor acceptor

    GephiTraversalVisualizationStrategy(final GephiRemoteAcceptor acceptor) {
        this.acceptor = acceptor
    }

    @Override
    void apply(final Traversal.Admin<?, ?> traversal) {
        // only apply these strategies if the traversal was :submit to the acceptor - otherwise process as usual
        if (acceptor.traversalSubmittedForViz) {
            final List<GraphStep> graphSteps = TraversalHelper.getStepsOfAssignableClass(GraphStep.class, traversal)
            final List<VertexStep> vertexSteps = TraversalHelper.getStepsOfAssignableClass(VertexStep.class, traversal)

            def List<Step> addAfter = new ArrayList<>()
            addAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeOtherVertexStep.class, traversal))
            addAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeVertexStep.class, traversal))
            addAfter.addAll(graphSteps.stream().filter { it.returnsVertex() }.collect(Collectors.toList()))
            addAfter.addAll(vertexSteps.stream().filter { it.returnsVertex() }.collect(Collectors.toList()))

            addAfter.each { Step s ->
                // add steps in reverse order as they will then appear as: vertex -> fold -> lambda -> unfold
                TraversalHelper.insertAfterStep(new UnfoldStep(traversal), s, traversal)
                TraversalHelper.insertAfterStep(new LambdaSideEffectStep(traversal, { Traverser traverser ->
                    acceptor.updateVisitedVertices()
                    traverser.get().each { Vertex v -> acceptor.visitVertexToGephi(v) }
                    Thread.sleep(acceptor.vizStepDelay)
                }), s, traversal)
                TraversalHelper.insertAfterStep(new FoldStep(traversal), s, traversal)
            }
        }
    }
}
