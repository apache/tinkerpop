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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.spark.process.computer.traversal.strategy.optimization.interceptor.SparkCountInterceptor;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkInterceptorStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final SparkInterceptorStrategy INSTANCE = new SparkInterceptorStrategy();

    private SparkInterceptorStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final Graph graph = traversal.getGraph().orElse(EmptyGraph.instance());
        final List<TraversalVertexProgramStep> steps = TraversalHelper.getStepsOfClass(TraversalVertexProgramStep.class, traversal);
        for (final TraversalVertexProgramStep step : steps) {
            final Traversal.Admin<?, ?> computerTraversal = step.generateProgram(graph).getTraversal().get().clone();
            if (!computerTraversal.isLocked())
                computerTraversal.applyStrategies();
            if (SparkCountInterceptor.isLegal(computerTraversal))
                step.setComputer(step.getComputer().configure(Constants.GREMLIN_HADOOP_VERTEX_PROGRAM_INTERCEPTOR, SparkCountInterceptor.class.getCanonicalName()));
        }
    }

    public static SparkInterceptorStrategy instance() {
        return INSTANCE;
    }


}