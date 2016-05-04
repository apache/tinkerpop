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

package org.apache.tinkerpop.gremlin.spark.process.computer.traversal.strategy.optimization.interceptor;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkMemory;
import org.apache.tinkerpop.gremlin.spark.process.computer.traversal.strategy.SparkVertexProgramInterceptor;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import scala.Tuple2;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkCountInterceptor implements SparkVertexProgramInterceptor<TraversalVertexProgram> {

    public SparkCountInterceptor() {

    }

    @Override
    public JavaPairRDD<Object, VertexWritable> apply(final TraversalVertexProgram vertexProgram, final JavaPairRDD<Object, VertexWritable> inputRDD, final SparkMemory memory) {
        vertexProgram.setup(memory);
        final Traversal.Admin<Vertex, Long> traversal = (Traversal.Admin) vertexProgram.getTraversal();
        JavaPairRDD<Object, VertexWritable> nextRDD = inputRDD;
        long count = -1;
        for (final Step step : traversal.getSteps()) {
            if (step instanceof GraphStep) {
                final Object[] ids = ((GraphStep) step).getIds();
                if (ids.length > 0)
                    nextRDD = nextRDD.filter(tuple -> ElementHelper.idExists(tuple._2().get().id(), ids));
            } else if (step instanceof HasStep) {
                final List<HasContainer> hasContainers = ((HasStep<?>) step).getHasContainers();
                if (hasContainers.size() > 0)
                    nextRDD = nextRDD.filter(tuple -> HasContainer.testAll(tuple._2().get(), hasContainers));
            } else if (step instanceof PropertiesStep) {
                final String[] keys = ((PropertiesStep) step).getPropertyKeys();
                count = nextRDD.mapValues(vertex -> IteratorUtils.count(vertex.get().properties(keys))).fold(new Tuple2<>("sum", 0l), (a, b) -> new Tuple2<>("sum", a._2() + b._2()))._2();
            } else if (step instanceof VertexStep) {
                final String[] labels = ((VertexStep) step).getEdgeLabels();
                final Direction direction = ((VertexStep) step).getDirection();
                count = nextRDD.mapValues(vertex -> IteratorUtils.count(vertex.get().edges(direction, labels))).fold(new Tuple2<>("sum", 0l), (a, b) -> new Tuple2<>("sum", a._2() + b._2()))._2();
            } else if (step instanceof CountGlobalStep) {
                if (count == -1)
                    count = nextRDD.count();
            }
        }
        assert count != -1;
        final TraverserSet<Long> haltedTraversers = new TraverserSet<>();
        haltedTraversers.add(traversal.getTraverserGenerator().generate(count, (Step) traversal.getEndStep(), 1l));
        memory.set(TraversalVertexProgram.HALTED_TRAVERSERS, haltedTraversers);
        memory.incrIteration();
        return inputRDD;
    }

    public static boolean isLegal(final Traversal.Admin<?, ?> traversal) {
        if (!TraversalHelper.getLabels(traversal).isEmpty())
            return false;
        final List<Step> steps = traversal.getSteps();
        if (!steps.get(0).getClass().equals(GraphStep.class) || ((GraphStep) steps.get(0)).returnsEdge())
            return false;
        if (!steps.get(steps.size() - 1).getClass().equals(CountGlobalStep.class))
            return false;
        int vertexPropertiesStepCount = 0;
        for (int i = 1; i < steps.size() - 1; i++) {
            final Step<?, ?> step = steps.get(i);
            final Class<? extends Step> stepClass = step.getClass();
            if (!stepClass.equals(HasStep.class) && !stepClass.equals(PropertiesStep.class) && !stepClass.equals(VertexStep.class))
                return false;
            if ((stepClass.equals(VertexStep.class) || stepClass.equals(PropertiesStep.class)) && (++vertexPropertiesStepCount > 1 || !step.getNextStep().getClass().equals(CountGlobalStep.class)))
                return false;
            if (stepClass.equals(HasStep.class) && !step.getPreviousStep().getClass().equals(GraphStep.class) && !step.getPreviousStep().getClass().equals(HasStep.class))
                return false;
        }
        return true;

    }

}

