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

package org.apache.tinkerpop.gremlin.spark.process.computer.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.MessagePassingReductionStrategy;
import org.apache.tinkerpop.gremlin.process.computer.util.EmptyMemory;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaFlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkSingleIterationStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final SparkSingleIterationStrategy INSTANCE = new SparkSingleIterationStrategy();

    private static final Set<Class> MULTI_ITERATION_CLASSES = new HashSet<>(Arrays.asList(
            EdgeVertexStep.class,
            LambdaMapStep.class,
            LambdaFlatMapStep.class
    ));

    private SparkSingleIterationStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final Graph graph = traversal.getGraph().orElse(EmptyGraph.instance()); // best guess at what the graph will be as its dynamically determined
        for (final TraversalVertexProgramStep step : TraversalHelper.getStepsOfClass(TraversalVertexProgramStep.class, traversal)) {
            final Traversal.Admin<?, ?> computerTraversal = step.generateProgram(graph, EmptyMemory.instance()).getTraversal().get().clone();
            if (!computerTraversal.isLocked())
                computerTraversal.applyStrategies();
            ///
            boolean doesMessagePass = TraversalHelper.hasStepOfAssignableClassRecursively(Scope.global, MULTI_ITERATION_CLASSES, computerTraversal);
            if (!doesMessagePass) {
                for (final VertexStep vertexStep : TraversalHelper.getStepsOfAssignableClassRecursively(Scope.global, VertexStep.class, computerTraversal)) {
                    if (vertexStep.returnsVertex() || !vertexStep.getDirection().equals(Direction.OUT)) { // in-edges require message pass in OLAP
                        doesMessagePass = true;
                        break;
                    }
                }
            }
            if (!doesMessagePass &&
                    !MessagePassingReductionStrategy.endsWithElement(computerTraversal.getEndStep()) &&
                    !(computerTraversal.getTraverserRequirements().contains(TraverserRequirement.LABELED_PATH) || // TODO: remove this when dynamic detachment is available in 3.3.0
                            computerTraversal.getTraverserRequirements().contains(TraverserRequirement.PATH))) {  // TODO: remove this when dynamic detachment is available in 3.3.0
                step.setComputer(step.getComputer()
                        // if no message passing, don't partition the loaded graph
                        .configure(Constants.GREMLIN_SPARK_SKIP_PARTITIONER, true)
                        // if no message passing, don't cache the loaded graph
                        .configure(Constants.GREMLIN_SPARK_SKIP_GRAPH_CACHE, true));
            }
        }
    }

    public static SparkSingleIterationStrategy instance() {
        return INSTANCE;
    }

}