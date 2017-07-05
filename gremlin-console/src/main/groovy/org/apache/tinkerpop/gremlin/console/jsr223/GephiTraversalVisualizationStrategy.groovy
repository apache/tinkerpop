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
package org.apache.tinkerpop.gremlin.console.jsr223

import groovy.transform.CompileStatic
import org.apache.tinkerpop.gremlin.process.traversal.Step
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy
import org.apache.tinkerpop.gremlin.process.traversal.Traverser
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateStep
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.LambdaSideEffectStep
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProfileStrategy
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper
import org.apache.tinkerpop.gremlin.structure.Vertex

import java.util.stream.Collectors

/**
 * A strategy that works in conjuction with the {@link org.apache.tinkerpop.gremlin.console.plugin.GephiRemoteAcceptor} to automatically inject visualization
 * steps after "vertex" steps to show the vertices traversed for a step.  If the traversal was evaluated in the
 * console normally then the visualization strategy will not be applied.  It must be {@code :submit} to the
 * console for the strategy to be applied.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@CompileStatic
class GephiTraversalVisualizationStrategy extends AbstractTraversalStrategy<TraversalStrategy.FinalizationStrategy>
        implements TraversalStrategy.FinalizationStrategy {

    private static final Set<Class<? extends TraversalStrategy.FinalizationStrategy>> POSTS = new HashSet<>();

    static {
        POSTS.add(ProfileStrategy.class);
    }

    private final GephiRemoteAcceptor acceptor

    private final String sideEffectKey = "viz-" + UUID.randomUUID()

    GephiTraversalVisualizationStrategy(final GephiRemoteAcceptor acceptor) {
        this.acceptor = acceptor
    }

    @Override
    void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal))
            return

        // only apply these strategies if the traversal was :submit to the acceptor - otherwise process as usual
        if (acceptor.traversalSubmittedForViz) {
            final List<GraphStep> graphSteps = TraversalHelper.getStepsOfAssignableClass(GraphStep.class, traversal)
            final List<VertexStep> vertexSteps = TraversalHelper.getStepsOfAssignableClass(VertexStep.class, traversal)

            def List<Step> addAfter = new ArrayList<>()
            addAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeOtherVertexStep.class, traversal))
            addAfter.addAll(TraversalHelper.getStepsOfAssignableClass(EdgeVertexStep.class, traversal))
            addAfter.addAll(vertexSteps.stream().filter { it.returnsVertex() }.collect(Collectors.toList()))
            addAfter.addAll(graphSteps.stream().filter { it.returnsVertex() }.collect(Collectors.toList()))

            // decay all vertices and visit each one to update their colors to the brightest
            addAfter.each { Step s ->
                TraversalHelper.insertAfterStep(new LambdaSideEffectStep(traversal, { Traverser traverser ->
                    final BulkSet<Vertex> vertices = ((BulkSet<Vertex>) traverser.sideEffects(sideEffectKey))
                    if (!vertices.isEmpty()) {
                        acceptor.updateVisitedVertices()
                        vertices.forEach { Vertex v, Long l -> acceptor.visitVertexInGephi(v) }
                        vertices.clear()
                        Thread.sleep(acceptor.vizStepDelay)
                    }
                }), s, traversal)
                TraversalHelper.insertAfterStep(new AggregateStep(traversal, sideEffectKey), s, traversal)
            }

            // decay all vertices except those that made it through the filter - "this way you can watch
            // the Gremlins dying" - said daniel kuppitz. can't do this easily with generic FilterStep as
            // it creates odd behaviors when used with loop (extra decay that probably shouldn't be there.
            // todo: can this be better? maybe we shouldn't do this at all
            TraversalHelper.getStepsOfAssignableClass(HasStep.class, traversal).each { HasStep s ->
                TraversalHelper.insertAfterStep(new LambdaSideEffectStep(traversal, { Traverser traverser ->
                    final BulkSet<Object> objects = ((BulkSet<Object>) traverser.sideEffects(sideEffectKey))
                    if (!objects.isEmpty()) {
                        final List<String> vertices = objects.findAll { Object o -> o instanceof Vertex }
                                .collect { Object o -> ((Vertex) o).id().toString() }
                        acceptor.updateVisitedVertices(vertices)
                        objects.clear()
                        Thread.sleep(acceptor.vizStepDelay)
                    }
                }), s, traversal)
                TraversalHelper.insertAfterStep(new AggregateStep(traversal, sideEffectKey), s, traversal)
            }
        }
    }

    @Override
    public Set<Class<? extends TraversalStrategy.FinalizationStrategy>> applyPost() {
        return POSTS;
    }
}
