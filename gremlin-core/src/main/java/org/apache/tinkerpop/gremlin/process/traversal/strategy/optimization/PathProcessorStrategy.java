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

package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.VertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * PathProcessStrategy is an OLAP strategy that does its best to turn non-local children in {@code where()} and {@code select()}
 * into local children by inlining components of the non-local child. In this way, PathProcessStrategy helps to ensure
 * that more traversals meet the local child constraint imposed on OLAP traversals.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @example <pre>
 * __.select(a).by(x)               // is replaced by select(a).map(x)
 * __.select(a,b).by(x).by(y)       // is replaced by select(a).by(x).as(a).select(b).by(y).as(b).select(a,b)
 * __.where(as(a).out().as(b))      // is replaced by as(xyz).select(a).where(out().as(b)).select(xyz)
 * __.where(as(a).out())            // is replaced by as(xyz).select(a).filter(out()).select(xyz)
 * </pre>
 */
public final class PathProcessorStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final PathProcessorStrategy INSTANCE = new PathProcessorStrategy();

    private static final boolean IS_TESTING = Boolean.valueOf(System.getProperty("is.testing", "false"));

    private static final String MARKER = Graph.Hidden.hide("gremlin.pathProcessor");
    private static final Set<Class> INVALIDATING_STEP_CLASSES = new HashSet<>(Arrays.asList(PathStep.class, TreeStep.class, LambdaHolder.class));

    private PathProcessorStrategy() {
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return Collections.singleton(MatchPredicateStrategy.class);
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPost() {
        return Collections.singleton(InlineFilterStrategy.class);
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // using a hidden label marker to denote whether the traversal should not be processed by this strategy
        if ((traversal.getParent() instanceof EmptyStep || traversal.getParent() instanceof VertexProgramStep) &&
                TraversalHelper.hasStepOfAssignableClassRecursively(INVALIDATING_STEP_CLASSES, traversal))
            TraversalHelper.applyTraversalRecursively(t -> t.getStartStep().addLabel(MARKER), traversal);

        if (traversal.getStartStep().getLabels().contains(MARKER)) {
            traversal.getStartStep().removeLabel(MARKER);
            return;
        }

        // process where(as("a").out()...) => select("a").where(out()...)
        final List<WhereTraversalStep> whereTraversalSteps = TraversalHelper.getStepsOfClass(WhereTraversalStep.class, traversal);
        for (final WhereTraversalStep<?> whereTraversalStep : whereTraversalSteps) {
            final Traversal.Admin<?, ?> localChild = whereTraversalStep.getLocalChildren().get(0);
            if ((localChild.getStartStep() instanceof WhereTraversalStep.WhereStartStep) &&
                    !((WhereTraversalStep.WhereStartStep) localChild.getStartStep()).getScopeKeys().isEmpty()) {
                boolean done = false;
                while (!done) {
                    done = true;
                    final int index = TraversalHelper.stepIndex(whereTraversalStep, traversal);
                    if (whereTraversalStep.getPreviousStep() instanceof SelectStep) {
                        done = false;
                        traversal.removeStep(index);
                        traversal.addStep(index - 1, whereTraversalStep);
                    }
                }
                final WhereTraversalStep.WhereStartStep<?> whereStartStep = (WhereTraversalStep.WhereStartStep<?>) localChild.getStartStep();
                int index = TraversalHelper.stepIndex(whereTraversalStep, traversal);
                final SelectOneStep<?, ?> selectOneStep = new SelectOneStep<>(traversal, Pop.last, whereStartStep.getScopeKeys().iterator().next());
                traversal.addStep(index, selectOneStep);
                final String generatedLabel = PathProcessorStrategy.generateLabel();
                if (selectOneStep.getPreviousStep() instanceof EmptyStep) {
                    TraversalHelper.insertBeforeStep(new IdentityStep<>(traversal), selectOneStep, traversal);
                    index++;
                }
                selectOneStep.getPreviousStep().addLabel(generatedLabel);
                TraversalHelper.insertAfterStep(new SelectOneStep<>(traversal, Pop.last, generatedLabel), whereTraversalStep, traversal);
                whereStartStep.removeScopeKey();
                // process where(as("a").out()) => as('xyz').select("a").filter(out()).select('xyz')
                if (!(localChild.getEndStep() instanceof WhereTraversalStep.WhereEndStep)) {
                    localChild.removeStep(localChild.getStartStep());
                    traversal.addStep(index + 1, new TraversalFilterStep<>(traversal, localChild));
                    traversal.removeStep(whereTraversalStep);
                }
            }
        }

        // process select("a","b").by(...).by(...)
        final List<SelectStep> selectSteps = TraversalHelper.getStepsOfClass(SelectStep.class, traversal);
        for (final SelectStep<?, Object> selectStep : selectSteps) {
            if (selectStep.getPop() != Pop.all && selectStep.getPop() != Pop.mixed && // TODO: necessary?
                    selectStep.getMaxRequirement().compareTo(PathProcessor.ElementRequirement.ID) > 0) {
                boolean oneLabel = true;
                for (final String key : selectStep.getScopeKeys()) {
                    if (labelCount(key, TraversalHelper.getRootTraversal(traversal)) > 1) {
                        oneLabel = false;
                        break;
                    }
                }
                if (!oneLabel)
                    continue;
                final int index = TraversalHelper.stepIndex(selectStep, traversal);
                final Map<String, Traversal.Admin<Object, Object>> byTraversals = selectStep.getByTraversals();
                final String[] keys = new String[byTraversals.size()];
                int counter = 0;
                for (final Map.Entry<String, Traversal.Admin<Object, Object>> entry : byTraversals.entrySet()) {
                    final SelectOneStep selectOneStep = new SelectOneStep(traversal, selectStep.getPop(), entry.getKey());
                    final TraversalMapStep<?, ?> mapStep = new TraversalMapStep<>(traversal, entry.getValue().clone());
                    mapStep.addLabel(entry.getKey());
                    traversal.addStep(index + 1, mapStep);
                    traversal.addStep(index + 1, selectOneStep);
                    keys[counter++] = entry.getKey();
                }
                traversal.addStep(index + 1 + (byTraversals.size() * 2), new SelectStep(traversal, Pop.last, keys));
                traversal.removeStep(index);
            }
        }

        // process select("a").by(...)
        final List<SelectOneStep> selectOneSteps = TraversalHelper.getStepsOfClass(SelectOneStep.class, traversal);
        for (final SelectOneStep<?, ?> selectOneStep : selectOneSteps) {
            if (selectOneStep.getPop() != Pop.all && selectOneStep.getPop() != Pop.mixed && // TODO: necessary?
                    selectOneStep.getMaxRequirement().compareTo(PathProcessor.ElementRequirement.ID) > 0 &&
                    labelCount(selectOneStep.getScopeKeys().iterator().next(), TraversalHelper.getRootTraversal(traversal)) <= 1) {
                final int index = TraversalHelper.stepIndex(selectOneStep, traversal);
                final Traversal.Admin<?, ?> localChild = selectOneStep.getLocalChildren().get(0);
                selectOneStep.removeLocalChild(localChild);
                final TraversalMapStep<?, ?> mapStep = new TraversalMapStep<>(traversal, localChild.clone());
                traversal.addStep(index + 1, mapStep);
            }
        }
    }

    public static PathProcessorStrategy instance() {
        return INSTANCE;
    }

    private static String generateLabel() {
        return IS_TESTING ? "xyz" : UUID.randomUUID().toString();
    }

    private static int labelCount(final String label, final Traversal.Admin<?, ?> traversal) {
        int count = 0;
        for (final Step step : traversal.getSteps()) {
            if (step.getLabels().contains(label))
                count++;
            if (step instanceof TraversalParent) {
                count = count + ((TraversalParent) step).getLocalChildren().stream().map(t -> labelCount(label, t)).reduce(0, (a, b) -> a + b);
                count = count + ((TraversalParent) step).getGlobalChildren().stream().map(t -> labelCount(label, t)).reduce(0, (a, b) -> a + b);
            }
        }
        return count;
    }
}
