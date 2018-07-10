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

import org.apache.tinkerpop.gremlin.hadoop.process.computer.traversal.step.map.HadoopReadStep;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.traversal.step.map.HadoopWriteStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.Reading;
import org.apache.tinkerpop.gremlin.process.traversal.step.Writing;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NoneStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class HadoopIoStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final HadoopIoStrategy INSTANCE = new HadoopIoStrategy();

    private HadoopIoStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // replace Reading and Writing steps with hadoop specific ones
        if (traversal.getStartStep() instanceof Reading) {
            final Reading reading = (Reading) traversal.getStartStep();
            final HadoopReadStep hadoopReadStep = new HadoopReadStep(traversal, reading.getFile());
            reading.getParameters().getRaw().entrySet().forEach(kv ->
                hadoopReadStep.configure(null, kv.getKey(), kv.getValue())
            );

            TraversalHelper.replaceStep((Step) reading, hadoopReadStep, traversal);
        } else if (traversal.getStartStep() instanceof Writing) {
            final Writing writing = (Writing) traversal.getStartStep();
            final HadoopWriteStep hadoopWriteStep = new HadoopWriteStep(traversal, writing.getFile());
            writing.getParameters().getRaw().entrySet().forEach(kv ->
                hadoopWriteStep.configure(null, kv.getKey(), kv.getValue())
            );

            TraversalHelper.replaceStep((Step) writing, hadoopWriteStep, traversal);
        }

        if (traversal.getEndStep() instanceof NoneStep)
            traversal.removeStep(1);
    }

    public static HadoopIoStrategy instance() {
        return INSTANCE;
    }
}
