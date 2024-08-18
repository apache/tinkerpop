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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.GValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.structure.map.MergeElementStepStructure;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GValueReplacementStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {
    private static final GValueReplacementStrategy INSTANCE = new GValueReplacementStrategy();

    /**
     * This strategy needs to be the first decoration to run as it replaces the {@link GValueStep} with the concrete
     * step.
     */
    private static final Set<Class<? extends DecorationStrategy>> POSTS = new HashSet<>(Arrays.asList(
            ConnectiveStrategy.class, ElementIdStrategy.class, EventStrategy.class, PartitionStrategy.class,
            RequirementsStrategy.class, SackStrategy.class, SeedStrategy.class, SideEffectStrategy.class,
            SubgraphStrategy.class, VertexProgramStrategy.class));

    private GValueReplacementStrategy() {
    }

    public static GValueReplacementStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        final List<GValueStep> gvalueSteps = TraversalHelper.getStepsOfAssignableClass(GValueStep.class, traversal);

        // loop through the graphSteps and replace the GValueStep in the traversal with the concrete step
        for (final GValueStep gvalueStep : gvalueSteps) {
            if (gvalueStep instanceof GValueStep.MergeOptionStep) {
                // fold  the option argument into the previous step which should be a concrete step. we get in theis
                // situation where a step like mergeV/E takes a concrete argument, but its associated option() takes
                // a GValue. we can't fold that GValue in at traversal creation time. so this section of code cleans
                // it all up.
                if (gvalueStep.getPreviousStep() instanceof TraversalOptionParent) {
                    final TraversalOptionParent parent = (TraversalOptionParent) gvalueStep.getPreviousStep();
                    if (gvalueStep instanceof MergeElementStepStructure) {
                        final MergeElementStepStructure mergeElementStepStructure = (MergeElementStepStructure) gvalueStep;
                        if (mergeElementStepStructure.getOnCreateTraversal() != null)
                            parent.addChildOption(Merge.onCreate, mergeElementStepStructure.getOnCreateTraversal());
                        else if (mergeElementStepStructure.getOnMatchTraversal() != null)
                            parent.addChildOption(Merge.onMatch, mergeElementStepStructure.getOnMatchTraversal());
                        else
                            throw new IllegalStateException("No option found for MergeElementStepStructure");
                    } else {
                        throw new IllegalStateException("The GValueStep is not an option argument for a TraversalOptionParent");
                    }
                } else {
                    throw new IllegalStateException("The GValueStep is not an option argument for a TraversalOptionParent");
                }

                // remove the step since its just an option argument
                traversal.removeStep(gvalueStep);
            } else {
                final Step concreteStep = gvalueStep.getConcreteStep();
                TraversalHelper.replaceStep(gvalueStep, concreteStep, traversal);
            }
        }
    }

    @Override
    public Set<Class<? extends DecorationStrategy>> applyPost() {
        return POSTS;
    }
}
