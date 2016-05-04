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
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaFlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkPartitionAwareStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final SparkPartitionAwareStrategy INSTANCE = new SparkPartitionAwareStrategy();

    private static final Set<Class<? extends Step>> MESSAGEPASS_CLASSES = new HashSet<>(Arrays.asList(
            EdgeVertexStep.class,
            LambdaMapStep.class, // maybe?
            LambdaFlatMapStep.class // maybe?
            // VertexStep is special as you need to see if the return class is Edge or Vertex (logic below)
    ));

    private SparkPartitionAwareStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final List<TraversalVertexProgramStep> steps = TraversalHelper.getStepsOfClass(TraversalVertexProgramStep.class, traversal);
        for (final TraversalVertexProgramStep step : steps) {
            final Traversal.Admin<?, ?> computerTraversal = step.generateProgram(traversal.getGraph().orElse(EmptyGraph.instance())).getTraversal().get().clone();
            if (!computerTraversal.isLocked())
                computerTraversal.applyStrategies();
            boolean messagePasses = MESSAGEPASS_CLASSES.stream()
                    .flatMap(clazz -> TraversalHelper.<Step<?, ?>>getStepsOfAssignableClassRecursively((Class) clazz, computerTraversal).stream())
                    .filter(s -> TraversalHelper.isGlobalChild(((Step) s).getTraversal().asAdmin()))
                    .findAny()
                    .isPresent();
            if (!messagePasses) {
                for (final VertexStep vertexStep : TraversalHelper.getStepsOfAssignableClassRecursively(VertexStep.class, computerTraversal)) {
                    if (TraversalHelper.isGlobalChild(vertexStep.getTraversal()) &&
                            (vertexStep.returnsVertex() || !vertexStep.getDirection().equals(Direction.OUT))) { // in edges require message pass in OLAP
                        messagePasses = true;
                        break;
                    }
                }
            }
            if (!messagePasses)  // if no message passing, don't partition (save time)
                step.setComputer(step.getComputer().configure(Constants.GREMLIN_SPARK_SKIP_PARTITIONER, true));
        }
    }

    public static SparkPartitionAwareStrategy instance() {
        return INSTANCE;
    }

}