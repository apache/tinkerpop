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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkMemory;
import org.apache.tinkerpop.gremlin.spark.process.computer.traversal.strategy.SparkVertexProgramInterceptor;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
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
        final Traversal.Admin<Vertex, Long> traversal = (Traversal.Admin) vertexProgram.getTraversal().getPure().clone();
        final Object[] graphStepIds = ((GraphStep) traversal.getStartStep()).getIds();
        final CountGlobalStep countGlobalStep = (CountGlobalStep) traversal.getEndStep();
        traversal.removeStep(0);                                    // remove GraphStep
        traversal.removeStep(traversal.getSteps().size() - 1);      // remove CountGlobalStep
        traversal.applyStrategies();                                // compile
        boolean identityTraversal = traversal.getSteps().isEmpty(); // if the traversal is empty, just return the vertex (fast)
        final long count = inputRDD
                .filter(tuple -> ElementHelper.idExists(tuple._2().get().id(), graphStepIds))
                .flatMapValues(vertexWritable -> {
                    if (identityTraversal)                          // g.V.count()-style (identity)
                        return () -> (Iterator) IteratorUtils.of(vertexWritable);
                    else {                                          // add the vertex to head of the traversal
                        final Traversal.Admin<Vertex, ?> clone = traversal.clone();
                        clone.getStartStep().addStart(clone.getTraverserGenerator().generate(vertexWritable.get(), EmptyStep.instance(), 1l));
                        return () -> clone;
                    }
                }).count();
        // generate the HALTED_TRAVERSERS for the memory
        final TraverserSet<Long> haltedTraversers = new TraverserSet<>();
        haltedTraversers.add(traversal.getTraverserGenerator().generate(count, countGlobalStep, 1l));
        memory.set(TraversalVertexProgram.HALTED_TRAVERSERS, haltedTraversers);
        memory.incrIteration(); // any local star graph reduction take a single iteration
        return inputRDD;
    }

    public static boolean isLegal(final Traversal.Admin<?, ?> traversal) {
        final List<Step> steps = traversal.getSteps();
        if (!steps.get(0).getClass().equals(GraphStep.class) || ((GraphStep) steps.get(0)).returnsEdge())
            return false;
        if (!steps.get(steps.size() - 1).getClass().equals(CountGlobalStep.class))
            return false;
        return TraversalHelper.isLocalStarGraph(traversal);

    }
}

