/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.hadoop.process.computer.traversal.strategy;

import org.apache.tinkerpop.gremlin.hadoop.process.computer.traversal.step.sideEffect.HadoopIoStep;
import org.apache.tinkerpop.gremlin.process.computer.clone.CloneVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.VertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.ReadWriting;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IoStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * The default implementation of the {@link IoStep} is a single threaded operation and doesn't properly take into
 * account the method by which OLAP read/writes take place with Hadoop. This strategy removes that step and replaces
 * it with the {@link HadoopIoStep} which is a {@link VertexProgramStep} that uses the {@link CloneVertexProgram} to
 * execute the IO operation in an OLAP fashion.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class HadoopIoStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final HadoopIoStrategy INSTANCE = new HadoopIoStrategy();

    private HadoopIoStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // since hadoopgraph can't be modified we can't try to use the existing IoStep for standard processing
        // without graphcomputer
        if (traversal.getStartStep() instanceof IoStep)
            throw new VerificationException("HadoopGraph requires a GraphComputer for io() step", traversal);

        // VertexProgramStrategy should wrap up the IoStep in a TraversalVertexProgramStep. use that to grab the
        // GraphComputer that was injected in there and push that in to the HadoopIoStep. this step pattern match
        // is fairly specific and since you really can't chain together steps after io() this approach should work
        if (traversal.getStartStep() instanceof TraversalVertexProgramStep) {
            final TraversalVertexProgramStep tvp = (TraversalVertexProgramStep) traversal.getStartStep();
            if (tvp.computerTraversal.get().getStartStep() instanceof ReadWriting) {
                final ReadWriting readWriting = (ReadWriting) tvp.computerTraversal.get().getStartStep();
                final HadoopIoStep hadoopIoStep = new HadoopIoStep(traversal, readWriting.getFile());
                hadoopIoStep.setMode(readWriting.getMode());
                hadoopIoStep.setComputer(tvp.getComputer());
                readWriting.getParameters().getRaw().forEach((key, value) -> value.forEach(v -> hadoopIoStep.configure(key, v)));

                TraversalHelper.replaceStep(tvp, hadoopIoStep, traversal);
            }
        }
    }

    public static HadoopIoStrategy instance() {
        return INSTANCE;
    }
}
