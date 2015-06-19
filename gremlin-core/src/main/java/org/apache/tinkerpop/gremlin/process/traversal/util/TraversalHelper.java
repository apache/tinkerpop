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
package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalHelper {

    private TraversalHelper() {
    }

    public static boolean isLocalStarGraph(final Traversal.Admin<?, ?> traversal) {
        return isLocalStarGraph(traversal, 'v');
    }

    private static boolean isLocalStarGraph(final Traversal.Admin<?, ?> traversal, char state) {
        for (final Step step : traversal.getSteps()) {
            if (step instanceof RepeatStep &&
                    ((RepeatStep<?>) step).getGlobalChildren().stream()
                            .flatMap(t -> t.getSteps().stream())
                            .filter(temp -> temp instanceof VertexStep)
                            .findAny()
                            .isPresent())  // TODO: is this sufficient?
                return false;
            if (step instanceof PropertiesStep && state == 'u')
                return false;
            else if (step instanceof VertexStep) {
                if (((VertexStep) step).returnsVertex()) {
                    if (state == 'u') return false;
                    if (state == 'v') state = 'u';
                } else {
                    state = 'e';
                }
            } else if (step instanceof EdgeVertexStep) {
                if (state == 'e') state = 'u';
            } else if (step instanceof HasContainerHolder && state == 'u') {
                if (((HasContainerHolder) step).getHasContainers().stream()
                        .filter(c -> !c.getKey().equals(T.id.getAccessor())) // TODO: are labels available?
                        .findAny().isPresent()) return false;
            } else if (step instanceof TraversalParent) {
                final char currState = state;
                if (((TraversalParent) step).getLocalChildren().stream()
                        .filter(t -> !isLocalStarGraph(t.asAdmin(), currState))
                        .findAny().isPresent()) return false;
            }
        }
        return true;
    }

    public static <S, E> Step<?, E> insertTraversal(final int insertIndex, final Traversal.Admin<S, E> insertTraversal, final Traversal.Admin<?, ?> traversal) {
        if (0 == traversal.getSteps().size()) {
            Step currentStep = EmptyStep.instance();
            for (final Step insertStep : insertTraversal.getSteps()) {
                currentStep = insertStep;
                traversal.addStep(insertStep);
            }
            return currentStep;
        } else {
            Step currentStep = traversal.getSteps().get(insertIndex);
            for (final Step insertStep : insertTraversal.getSteps()) {
                TraversalHelper.insertAfterStep(insertStep, currentStep, traversal);
                currentStep = insertStep;
            }
            return currentStep;
        }
    }

    public static <S, E> int stepIndex(final Step<S, E> step, final Traversal.Admin<?, ?> traversal) {
        int i = 0;
        for (final Step s : traversal.getSteps()) {
            if (s.equals(step, true))
                return i;
            i++;
        }
        return -1;
    }

    public static <S, E> Step<?, E> insertTraversal(final Step<?, S> previousStep, final Traversal.Admin<S, E> insertTraversal, final Traversal.Admin<?, ?> traversal) {
        return TraversalHelper.insertTraversal(stepIndex(previousStep, traversal), insertTraversal, traversal);
    }

    public static <S, E> void insertBeforeStep(final Step<S, E> insertStep, final Step<E, ?> afterStep, final Traversal.Admin<?, ?> traversal) {
        traversal.addStep(stepIndex(afterStep, traversal), insertStep);
    }

    public static <S, E> void insertAfterStep(final Step<S, E> insertStep, final Step<?, S> beforeStep, final Traversal.Admin<?, ?> traversal) {
        traversal.addStep(stepIndex(beforeStep, traversal) + 1, insertStep);
    }

    public static <S, E> void replaceStep(final Step<S, E> removeStep, final Step<S, E> insertStep, final Traversal.Admin<?, ?> traversal) {
        traversal.addStep(stepIndex(removeStep, traversal), insertStep);
        traversal.removeStep(removeStep);
    }

    public static <S> List<S> getStepsOfClass(final Class<S> stepClass, final Traversal.Admin<?, ?> traversal) {
        return (List) traversal.getSteps().stream().filter(step -> step.getClass().equals(stepClass)).collect(Collectors.toList());
    }

    public static <S> List<S> getStepsOfAssignableClass(final Class<S> stepClass, final Traversal.Admin<?, ?> traversal) {
        return (List) traversal.getSteps().stream().filter(step -> stepClass.isAssignableFrom(step.getClass())).collect(Collectors.toList());
    }

    public static <S> Optional<S> getLastStepOfAssignableClass(final Class<S> stepClass, final Traversal.Admin<?, ?> traversal) {
        final List<S> steps = TraversalHelper.getStepsOfAssignableClass(stepClass, traversal);
        return steps.size() == 0 ? Optional.empty() : Optional.of(steps.get(steps.size() - 1));
    }

    public static <S> List<S> getStepsOfAssignableClassRecursively(final Class<S> stepClass, final Traversal.Admin<?, ?> traversal) {
        final List<S> list = new ArrayList<>();
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (stepClass.isAssignableFrom(step.getClass()))
                list.add((S) step);
            if (step instanceof TraversalParent) {
                for (final Traversal.Admin<?, ?> globalChild : ((TraversalParent) step).getGlobalChildren()) {
                    list.addAll(TraversalHelper.getStepsOfAssignableClassRecursively(stepClass, globalChild));
                }
            }
        }
        return list;
    }

    public static <S, E> void removeToTraversal(final Step<S, ?> startStep, final Step<?, E> endStep, final Traversal.Admin<S, E> newTraversal) {
        final Traversal.Admin<?, ?> originalTraversal = startStep.getTraversal();
        Step<?, ?> currentStep = startStep;
        while (currentStep != endStep && !(currentStep instanceof EmptyStep)) {
            final Step<?, ?> temp = currentStep.getNextStep();
            originalTraversal.removeStep(currentStep);
            newTraversal.addStep(currentStep);
            currentStep = temp;
        }
    }

    public static boolean hasStepOfClass(final Class stepClass, final Traversal.Admin<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (step.getClass().equals(stepClass)) {
                return true;
            }
        }
        return false;
    }

    public static boolean hasStepOfAssignableClass(final Class superClass, final Traversal.Admin<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (superClass.isAssignableFrom(step.getClass())) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param stepClass the step class to look for
     * @param traversal the traversal in which to look for the given step class
     * @return <code>true</code> if any step in the given traversal (and its child traversals) is an instance of the
     * given <code>stepClass</code>, otherwise <code>false</code>.
     */
    public static boolean hasStepOfAssignableClassRecursively(final Class stepClass, final Traversal.Admin<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (stepClass.isAssignableFrom(step.getClass())) {
                return true;
            }
            if (step instanceof TraversalParent) {
                for (final Traversal.Admin<?, ?> globalChild : ((TraversalParent) step).getGlobalChildren()) {
                    if (hasStepOfAssignableClassRecursively(stepClass, globalChild)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * @param stepClasses the step classes to look for
     * @param traversal   the traversal in which to look for the given step classes
     * @return <code>true</code> if any step in the given traversal (and its child traversals) is an instance of a class
     * provided in <code>stepClasses</code>, otherwise <code>false</code>.
     */
    public static boolean hasStepOfAssignableClassRecursively(final Collection<Class> stepClasses, final Traversal.Admin<?, ?> traversal) {
        if (stepClasses.size() == 1)
            return hasStepOfAssignableClassRecursively(stepClasses.iterator().next(), traversal);
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (IteratorUtils.anyMatch(stepClasses.iterator(), stepClass -> stepClass.isAssignableFrom(step.getClass()))) {
                return true;
            }
            if (step instanceof TraversalParent) {
                for (final Traversal.Admin<?, ?> globalChild : ((TraversalParent) step).getGlobalChildren()) {
                    if (hasStepOfAssignableClassRecursively(stepClasses, globalChild)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static <S> void addToCollection(final Collection<S> collection, final S s, final long bulk) {
        if (collection instanceof BulkSet) {
            ((BulkSet<S>) collection).add(s, bulk);
        } else if (collection instanceof Set) {
            collection.add(s);
        } else {
            for (long i = 0; i < bulk; i++) {
                collection.add(s);
            }
        }
    }

    public static <S> void addToCollectionUnrollIterator(final Collection<S> collection, final S s, final long bulk) {
        if (s instanceof Iterator) {
            ((Iterator<S>) s).forEachRemaining(r -> addToCollection(collection, r, bulk));
        } else if (s instanceof Iterable) {
            ((Iterable<S>) s).forEach(r -> addToCollection(collection, r, bulk));
        } else {
            addToCollection(collection, s, bulk);
        }
    }

    /**
     * Returns the name of <i>step</i> truncated to <i>maxLength</i>. An ellipses is appended when the name exceeds
     * <i>maxLength</i>.
     *
     * @param step
     * @param maxLength Includes the 3 "..." characters that will be appended when the length of the name exceeds
     *                  maxLength.
     * @return short step name.
     */
    public static String getShortName(final Step step, final int maxLength) {
        final String name = step.toString();
        if (name.length() > maxLength)
            return name.substring(0, maxLength - 3) + "...";
        return name;
    }

    public static void reIdSteps(final StepPosition stepPosition, final Traversal.Admin<?, ?> traversal) {
        stepPosition.x = 0;
        stepPosition.y = -1;
        stepPosition.z = -1;
        stepPosition.parentId = null;
        Traversal.Admin<?, ?> current = traversal;
        while (!(current instanceof EmptyTraversal)) {
            stepPosition.y++;
            final TraversalParent parent = current.getParent();
            if (null == stepPosition.parentId && !(parent instanceof EmptyStep))
                stepPosition.parentId = parent.asStep().getId();
            if (-1 == stepPosition.z) {
                final int globalChildrenSize = parent.getGlobalChildren().size();
                for (int i = 0; i < globalChildrenSize; i++) {
                    if (parent.getGlobalChildren().get(i) == current) {
                        stepPosition.z = i;
                    }
                }
                for (int i = 0; i < parent.getLocalChildren().size(); i++) {
                    if (parent.getLocalChildren().get(i) == current) {
                        stepPosition.z = i + globalChildrenSize;
                    }
                }
            }
            current = parent.asStep().getTraversal();
        }
        if (-1 == stepPosition.z) stepPosition.z = 0;
        if (null == stepPosition.parentId) stepPosition.parentId = "";
        for (final Step<?, ?> step : traversal.getSteps()) {
            step.setId(stepPosition.nextXId());
        }
    }

    public static Traversal.Admin<?, ?> getRootTraversal(Traversal.Admin<?, ?> traversal) {
        while (!((traversal.getParent()) instanceof EmptyStep)) {
            traversal = traversal.getParent().asStep().getTraversal();
        }
        return traversal;
    }

    public static Set<String> getLabels(final Traversal.Admin<?, ?> traversal) {
        return getLabels(new HashSet<>(), traversal);
    }

    private static Set<String> getLabels(final Set<String> labels, final Traversal.Admin<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            labels.addAll(step.getLabels());
            if (step instanceof TraversalParent) {
                ((TraversalParent) step).getLocalChildren().forEach(child -> getLabels(labels, child));
                ((TraversalParent) step).getGlobalChildren().forEach(child -> getLabels(labels, child));
            }
        }
        return labels;
    }
}
