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
import org.apache.tinkerpop.gremlin.process.computer.util.EmptyMemory;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.spark.process.computer.traversal.strategy.optimization.interceptor.SparkStarBarrierInterceptor;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkInterceptorStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final SparkInterceptorStrategy INSTANCE = new SparkInterceptorStrategy();

    private SparkInterceptorStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final Graph graph = traversal.getGraph().orElse(EmptyGraph.instance()); // best guess at what the graph will be as its dynamically determined
        for (final TraversalVertexProgramStep step : TraversalHelper.getStepsOfClass(TraversalVertexProgramStep.class, traversal)) {
            final Traversal.Admin<?, ?> computerTraversal = step.generateProgram(graph, EmptyMemory.instance()).getTraversal().get().clone();
            if (!computerTraversal.isLocked())
                computerTraversal.applyStrategies();
            if (SparkStarBarrierInterceptor.isLegal(computerTraversal)) {
                step.setComputer(step.getComputer()
                        .configure(Constants.GREMLIN_SPARK_SKIP_PARTITIONER, true)
                        .configure(Constants.GREMLIN_SPARK_SKIP_GRAPH_CACHE, true)
                        .configure(Constants.GREMLIN_HADOOP_VERTEX_PROGRAM_INTERCEPTOR, SparkStarBarrierInterceptor.class.getCanonicalName()));
            }
        }
    }

    public static SparkInterceptorStrategy instance() {
        return INSTANCE;
    }


}