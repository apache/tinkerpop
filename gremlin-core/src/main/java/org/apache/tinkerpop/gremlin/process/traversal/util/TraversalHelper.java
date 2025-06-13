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

import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.AbstractLambdaTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.PopContaining;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LabelStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class TraversalHelper {

    private TraversalHelper() {
    }

    public static boolean isLocalProperties(final Traversal.Admin<?, ?> traversal) {
        for (final Step step : traversal.getSteps()) {
            if (step instanceof RepeatStep) {
                for (final Traversal.Admin<?, ?> global : ((RepeatStep<?>) step).getGlobalChildren()) {
                    if (TraversalHelper.hasStepOfAssignableClass(VertexStep.class, global))
                        return false;
                }
            } else if (step instanceof VertexStep) {
                return false;
            } else if (step instanceof EdgeVertexStep) {
                return false;
            } else if (step instanceof TraversalParent) {
                for (final Traversal.Admin<?, ?> local : ((TraversalParent) step).getLocalChildren()) {
                    if (!TraversalHelper.isLocalProperties(local))
                        return false;
                }
            }
        }
        return true;
    }

    public static boolean isLocalStarGraph(final Traversal.Admin<?, ?> traversal) {
        return 'x' != isLocalStarGraph(traversal, 'v');
    }

    private static char isLocalStarGraph(final Traversal.Admin<?, ?> traversal, char state) {
        if (state == 'u' &&
                (traversal instanceof ValueTraversal ||
                        (traversal instanceof TokenTraversal && !((TokenTraversal) traversal).getToken().equals(T.id))))
            return 'x';
        for (final Step step : traversal.getSteps()) {
            if ((step instanceof PropertiesStep || step instanceof LabelStep || step instanceof PropertyMapStep) && state == 'u')
                return 'x';
            else if (step instanceof VertexStep) {
                if (state == 'u') return 'x';
                state = ((VertexStep) step).returnsVertex() ? 'u' : 'e';
            } else if (step instanceof EdgeVertexStep) {
                state = 'u';
            } else if (step instanceof HasContainerHolder && state == 'u') {
                for (final HasContainer hasContainer : ((HasContainerHolder) step).getHasContainers()) {
                    if (!hasContainer.getKey().equals(T.id.getAccessor()))
                        return 'x';
                }
            } else if (step instanceof TraversalParent) {
                final char currState = state;
                final Set<Character> states = new HashSet<>();
                for (final Traversal.Admin<?, ?> local : ((TraversalParent) step).getLocalChildren()) {
                    final char s = isLocalStarGraph(local, currState);
                    if ('x' == s) return 'x';
                    states.add(s);
                }
                if (!(step instanceof ByModulating)) {
                    if (states.contains('u'))
                        state = 'u';
                    else if (states.contains('e'))
                        state = 'e';
                }
                states.clear();
                if (step instanceof SelectStep || step instanceof SelectOneStep) {
                    states.add('u');
                }
                for (final Traversal.Admin<?, ?> local : ((TraversalParent) step).getGlobalChildren()) {
                    final char s = isLocalStarGraph(local, currState);
                    if ('x' == s) return 'x';
                    states.add(s);
                }
                if (states.contains('u'))
                    state = 'u';
                else if (states.contains('e'))
                    state = 'e';
                if (state != currState && (step instanceof RepeatStep || step instanceof MatchStep))
                    return 'x';
            }
        }
        return state;
    }

    /**
     * Insert a step before a specified step instance.
     *
     * @param insertStep the step to insert
     * @param afterStep  the step to insert the new step before
     * @param traversal  the traversal on which the action should occur
     */
    public static <S, E> void insertBeforeStep(final Step<S, E> insertStep, final Step<E, ?> afterStep, final Traversal.Admin<?, ?> traversal) {
        traversal.addStep(stepIndex(afterStep, traversal), insertStep);
    }

    /**
     * Insert a step after a specified step instance.
     *
     * @param insertStep the step to insert
     * @param beforeStep the step to insert the new step after
     * @param traversal  the traversal on which the action should occur
     */
    public static <S, E> void insertAfterStep(final Step<S, E> insertStep, final Step<?, S> beforeStep, final Traversal.Admin<?, ?> traversal) {
        traversal.addStep(stepIndex(beforeStep, traversal) + 1, insertStep);
    }

    /**
     * Replace a step with a new step.
     *
     * @param removeStep the step to remove
     * @param insertStep the step to insert
     * @param traversal  the traversal on which the action will occur
     */
    public static <S, E> void replaceStep(final Step<S, E> removeStep, final Step<S, E> insertStep, final Traversal.Admin<?, ?> traversal) {
        final int i;
        traversal.removeStep(i = stepIndex(removeStep, traversal));
        traversal.addStep(i, insertStep);
    }

    public static <S, E> Step<?, E> insertTraversal(final Step<?, S> previousStep, final Traversal.Admin<S, E> insertTraversal, final Traversal.Admin<?, ?> traversal) {
        return TraversalHelper.insertTraversal(stepIndex(previousStep, traversal), insertTraversal, traversal);
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

    /**
     * Gets the index of a particular step in the {@link Traversal}.
     *
     * @param step      the step to retrieve the index for
     * @param traversal the traversal to perform the action on
     * @return the index of the step or -1 if the step is not present
     */
    public static <S, E> int stepIndex(final Step<S, E> step, final Traversal.Admin<?, ?> traversal) {
        int i = 0;
        for (final Step s : traversal.getSteps()) {
            if (s.equals(step, true))
                return i;
            i++;
        }
        return -1;
    }

    public static <S> List<S> getStepsOfClass(final Class<S> stepClass, final Traversal.Admin<?, ?> traversal) {
        final List<S> steps = new ArrayList<>();
        for (final Step step : traversal.getSteps()) {
            if (step.getClass().equals(stepClass))
                steps.add((S) step);
        }
        return steps;
    }

    public static <S> List<S> getStepsOfAssignableClass(final Class<S> stepClass, final Traversal.Admin<?, ?> traversal) {
        final List<S> steps = new ArrayList<>();
        for (final Step step : traversal.getSteps()) {
            if (stepClass.isAssignableFrom(step.getClass()))
                steps.add((S) step);
        }
        return steps;
    }

    public static <S> Optional<S> getLastStepOfAssignableClass(final Class<S> stepClass, final Traversal.Admin<?, ?> traversal) {
        final List<S> steps = TraversalHelper.getStepsOfAssignableClass(stepClass, traversal);
        return steps.size() == 0 ? Optional.empty() : Optional.of(steps.get(steps.size() - 1));
    }

    public static <S> Optional<S> getFirstStepOfAssignableClass(final Class<S> stepClass, final Traversal.Admin<?, ?> traversal) {
        for (final Step step : traversal.getSteps()) {
            if (stepClass.isAssignableFrom(step.getClass()))
                return Optional.of((S) step);
        }
        return Optional.empty();
    }

    public static <S> List<S> getStepsOfAssignableClassRecursively(final Class<S> stepClass, final Traversal.Admin<?, ?> traversal) {
        return getStepsOfAssignableClassRecursively(null, stepClass, traversal);
    }

    public static <S> List<S> getStepsOfAssignableClassRecursively(final Scope scope, final Class<S> stepClass, final Traversal.Admin<?, ?> traversal) {
        final List<S> list = new ArrayList<>();
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (stepClass.isAssignableFrom(step.getClass()))
                list.add((S) step);
            if (step instanceof TraversalParent) {
                if (null == scope || Scope.local.equals(scope)) {
                    for (final Traversal.Admin<?, ?> localChild : ((TraversalParent) step).getLocalChildren()) {
                        list.addAll(TraversalHelper.getStepsOfAssignableClassRecursively(stepClass, localChild));
                    }
                }
                if (null == scope || Scope.global.equals(scope)) {
                    for (final Traversal.Admin<?, ?> globalChild : ((TraversalParent) step).getGlobalChildren()) {
                        list.addAll(TraversalHelper.getStepsOfAssignableClassRecursively(stepClass, globalChild));
                    }
                }
            }
        }
        return list;
    }

    /**
     * Get steps of the specified classes throughout the traversal.
     */
    public static List<Step<?,?>> getStepsOfAssignableClassRecursively(final Traversal.Admin<?, ?> traversal, final Class<?>... stepClasses) {
        final List<Step<?,?>> list = new ArrayList<>();
        for (final Step<?, ?> step : traversal.getSteps()) {
            for (Class<?> stepClass : stepClasses) {
                if (stepClass.isAssignableFrom(step.getClass()))
                    list.add(step);
            }

            if (step instanceof TraversalParent) {
                for (final Traversal.Admin<?, ?> localChild : ((TraversalParent) step).getLocalChildren()) {
                    list.addAll(TraversalHelper.getStepsOfAssignableClassRecursively(localChild, stepClasses));
                }
                for (final Traversal.Admin<?, ?> globalChild : ((TraversalParent) step).getGlobalChildren()) {
                    list.addAll(TraversalHelper.getStepsOfAssignableClassRecursively(globalChild, stepClasses));
                }
            }
        }
        return list;
    }

    /**
     * Get steps of the specified classes throughout the traversal, collecting them in a fashion that orders them
     * from the deepest steps first.
     */
    public static List<Step<?,?>> getStepsOfAssignableClassRecursivelyFromDepth(final Traversal.Admin<?, ?> traversal, final Class<?>... stepClasses) {
        final List<Step<?,?>> list = new ArrayList<>();
        final Stack<Step<?,?>> stack = new Stack<>();

        traversal.getSteps().forEach(stack::push);

        while (!stack.isEmpty()) {
            final Step<?,?> current = stack.pop();
            list.add(current);

            if (current instanceof TraversalParent) {
                ((TraversalParent) current).getLocalChildren().forEach(localChild -> localChild.getSteps().forEach(stack::push));
                ((TraversalParent) current).getGlobalChildren().forEach(globalChild -> globalChild.getSteps().forEach(stack::push));
            }
        }

        // sort by depth
        list.sort(DepthComparator.instance());

        return list.stream().filter(s -> {
            for (Class<?> stepClass : stepClasses) {
                if (stepClass.isAssignableFrom(s.getClass()))
                    return true;
            }
            return false;
        }).collect(Collectors.toList());
    }

    public static boolean isGlobalChild(Traversal.Admin<?, ?> traversal) {
        while (!(traversal.isRoot())) {
            if (traversal.getParent().getLocalChildren().contains(traversal))
                return false;
            traversal = traversal.getParent().asStep().getTraversal();
        }
        return true;
    }

    /**
     * Determine if the traversal has a step of a particular class.
     *
     * @param stepClass the step class to look for
     * @param traversal the traversal to perform the action on
     * @return {@code true} if the class is found and {@code false} otherwise
     */
    public static boolean hasStepOfClass(final Class stepClass, final Traversal.Admin<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (step.getClass().equals(stepClass)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Determine if the traversal has a step of an assignable class.
     *
     * @param superClass the step super class to look for
     * @param traversal  the traversal to perform the action on
     * @return {@code true} if the class is found and {@code false} otherwise
     */
    public static boolean hasStepOfAssignableClass(final Class superClass, final Traversal.Admin<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (superClass.isAssignableFrom(step.getClass())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Determine if the traversal has a step of an assignable class in the current {@link Traversal} and its
     * local and global child traversals.
     *
     * @param stepClass the step class to look for
     * @param traversal the traversal in which to look for the given step class
     * @return <code>true</code> if any step in the given traversal (and its child traversals) is an instance of the
     * given <code>stepClass</code>, otherwise <code>false</code>.
     */
    public static boolean hasStepOfAssignableClassRecursively(final Class stepClass, final Traversal.Admin<?, ?> traversal) {
        return hasStepOfAssignableClassRecursively(null, stepClass, traversal);
    }

    /**
     * Determine if the traversal has a step of an assignable class in the current {@link Traversal} and its
     * {@link Scope} child traversals.
     *
     * @param scope     the child traversal scope to check
     * @param stepClass the step class to look for
     * @param traversal the traversal in which to look for the given step class
     * @return <code>true</code> if any step in the given traversal (and its child traversals) is an instance of the
     * given <code>stepClass</code>, otherwise <code>false</code>.
     */
    public static boolean hasStepOfAssignableClassRecursively(final Scope scope, final Class stepClass, final Traversal.Admin<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (stepClass.isAssignableFrom(step.getClass())) {
                return true;
            }
            if (step instanceof TraversalParent) {
                if (null == scope || Scope.local.equals(scope)) {
                    for (final Traversal.Admin<?, ?> localChild : ((TraversalParent) step).getLocalChildren()) {
                        if (hasStepOfAssignableClassRecursively(stepClass, localChild)) return true;
                    }
                }
                if (null == scope || Scope.global.equals(scope)) {
                    for (final Traversal.Admin<?, ?> globalChild : ((TraversalParent) step).getGlobalChildren()) {
                        if (hasStepOfAssignableClassRecursively(stepClass, globalChild)) return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Determine if the traversal has any of the supplied steps of an assignable class in the current {@link Traversal}
     * and its global or local child traversals.
     *
     * @param stepClasses the step classes to look for
     * @param traversal   the traversal in which to look for the given step classes
     * @return <code>true</code> if any step in the given traversal (and its child traversals) is an instance of a class
     * provided in <code>stepClasses</code>, otherwise <code>false</code>.
     */
    public static boolean hasStepOfAssignableClassRecursively(final Collection<Class> stepClasses, final Traversal.Admin<?, ?> traversal) {
        return hasStepOfAssignableClassRecursively(null, stepClasses, traversal);
    }

    /**
     * Determine if the traversal has any of the supplied steps of an assignable class in the current {@link Traversal}
     * and its {@link Scope} child traversals.
     *
     * @param scope       whether to check global or local children (null for both).
     * @param stepClasses the step classes to look for
     * @param traversal   the traversal in which to look for the given step classes
     * @return <code>true</code> if any step in the given traversal (and its child traversals) is an instance of a class
     * provided in <code>stepClasses</code>, otherwise <code>false</code>.
     */
    public static boolean hasStepOfAssignableClassRecursively(final Scope scope, final Collection<Class> stepClasses, final Traversal.Admin<?, ?> traversal) {
        if (stepClasses.size() == 1)
            return hasStepOfAssignableClassRecursively(stepClasses.iterator().next(), traversal);
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (IteratorUtils.anyMatch(stepClasses.iterator(), stepClass -> stepClass.isAssignableFrom(step.getClass()))) {
                return true;
            }
            if (step instanceof TraversalParent) {
                if (null == scope || Scope.local.equals(scope)) {
                    for (final Traversal.Admin<?, ?> localChild : ((TraversalParent) step).getLocalChildren()) {
                        if (hasStepOfAssignableClassRecursively(stepClasses, localChild)) return true;
                    }
                }
                if (null == scope || Scope.global.equals(scope)) {
                    for (final Traversal.Admin<?, ?> globalChild : ((TraversalParent) step).getGlobalChildren()) {
                        if (hasStepOfAssignableClassRecursively(stepClasses, globalChild)) return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Determine if any step in {@link Traversal} or its children match the step given the provided {@link Predicate}.
     *
     * @param predicate the match function
     * @param traversal the traversal to perform the action on
     * @return {@code true} if there is a match and {@code false} otherwise
     */
    public static boolean anyStepRecursively(final Predicate<Step> predicate, final Traversal.Admin<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (predicate.test(step)) {
                return true;
            }

            if (step instanceof TraversalParent && anyStepRecursively(predicate, ((TraversalParent) step))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Determine if any child step of a {@link TraversalParent} match the step given the provided {@link Predicate}.
     *
     * @param predicate the match function
     * @param step      the step to perform the action on
     * @return {@code true} if there is a match and {@code false} otherwise
     */
    public static boolean anyStepRecursively(final Predicate<Step> predicate, final TraversalParent step) {
        for (final Traversal.Admin<?, ?> localChild : step.getLocalChildren()) {
            if (anyStepRecursively(predicate, localChild)) return true;
        }
        for (final Traversal.Admin<?, ?> globalChild : step.getGlobalChildren()) {
            if (anyStepRecursively(predicate, globalChild)) return true;
        }
        return false;
    }

    /**
     * Apply the provider {@link Consumer} function to the provided {@link Traversal} and all of its children.
     *
     * @param consumer  the function to apply to the each traversal in the tree
     * @param traversal the root traversal to start application
     */
    public static void applyTraversalRecursively(final Consumer<Traversal.Admin<?, ?>> consumer, final Traversal.Admin<?, ?> traversal) {
        applyTraversalRecursively(consumer, traversal, false);
    }

    /**
     * Apply the provider {@link Consumer} function to the provided {@link Traversal} and all of its children.
     *
     * @param consumer  the function to apply to the each traversal in the tree
     * @param traversal the root traversal to start application
     */
    public static void applyTraversalRecursively(final Consumer<Traversal.Admin<?, ?>> consumer, final Traversal.Admin<?, ?> traversal,
                                                 final boolean applyToChildrenOnly) {
        if (!applyToChildrenOnly)
            consumer.accept(traversal);

        // we get accused of concurrentmodification if we try a for(Iterable)
        final List<Step> steps = traversal.getSteps();
        for (int ix = 0; ix < steps.size(); ix++) {
            final Step step = steps.get(ix);
            if (step instanceof TraversalParent) {
                for (final Traversal.Admin<?, ?> local : ((TraversalParent) step).getLocalChildren()) {
                    applyTraversalRecursively(consumer, local);
                }
                for (final Traversal.Admin<?, ?> global : ((TraversalParent) step).getGlobalChildren()) {
                    applyTraversalRecursively(consumer, global);
                }
            }
        }
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
                    final Traversal currentLocalChild = parent.getLocalChildren().get(i);
                    if (currentLocalChild == current ||
                            (currentLocalChild instanceof AbstractLambdaTraversal && ((AbstractLambdaTraversal) currentLocalChild).getBypassTraversal() == current)) {
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

    public static boolean hasLabels(final Traversal.Admin<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            for (final String label : step.getLabels()) {
                if (!Graph.Hidden.isHidden(label))
                    return true;
            }
            if (step instanceof TraversalParent) {
                for (final Traversal.Admin<?, ?> local : ((TraversalParent) step).getLocalChildren()) {
                    if (TraversalHelper.hasLabels(local))
                        return true;
                }
                for (final Traversal.Admin<?, ?> global : ((TraversalParent) step).getGlobalChildren()) {
                    if (TraversalHelper.hasLabels(global))
                        return true;
                }
            }
        }
        return false;
    }

    public static Set<String> getLabels(final Traversal.Admin<?, ?> traversal) {
        return TraversalHelper.getLabels(new HashSet<>(), traversal);
    }

    private static Set<String> getLabels(final Set<String> labels, final Traversal.Admin<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            labels.addAll(step.getLabels());
            if (step instanceof TraversalParent) {
                for (final Traversal.Admin<?, ?> local : ((TraversalParent) step).getLocalChildren()) {
                    TraversalHelper.getLabels(labels, local);
                }
                for (final Traversal.Admin<?, ?> global : ((TraversalParent) step).getGlobalChildren()) {
                    TraversalHelper.getLabels(labels, global);
                }
            }
        }
        return labels;
    }

    public static Set<Scoping.Variable> getVariableLocations(final Traversal.Admin<?, ?> traversal) {
        return TraversalHelper.getVariableLocations(EnumSet.noneOf(Scoping.Variable.class), traversal);
    }

    private static Set<Scoping.Variable> getVariableLocations(final Set<Scoping.Variable> variables, final Traversal.Admin<?, ?> traversal) {
        if (variables.size() == 2) return variables;    // has both START and END so no need to compute further
        final Step<?, ?> startStep = traversal.getStartStep();
        if (StartStep.isVariableStartStep(startStep))
            variables.add(Scoping.Variable.START);
        else if (startStep instanceof WherePredicateStep) {
            if (((WherePredicateStep) startStep).getStartKey().isPresent())
                variables.add(Scoping.Variable.START);
        } else if (startStep instanceof WhereTraversalStep.WhereStartStep) {
            if (!((WhereTraversalStep.WhereStartStep) startStep).getScopeKeys().isEmpty())
                variables.add(Scoping.Variable.START);
        } else if (startStep instanceof MatchStep.MatchStartStep) {
            if (((MatchStep.MatchStartStep) startStep).getSelectKey().isPresent())
                variables.add(Scoping.Variable.START);
        } else if (startStep instanceof MatchStep) {
            for (final Traversal.Admin<?, ?> global : ((MatchStep<?, ?>) startStep).getGlobalChildren()) {
                TraversalHelper.getVariableLocations(variables, global);
            }
        } else if (startStep instanceof ConnectiveStep || startStep instanceof NotStep || startStep instanceof WhereTraversalStep) {
            for (final Traversal.Admin<?, ?> local : ((TraversalParent) startStep).getLocalChildren()) {
                TraversalHelper.getVariableLocations(variables, local);
            }
        }
        ///
        final Step<?, ?> endStep = traversal.getEndStep();
        if (endStep instanceof WherePredicateStep) {
            if (((WherePredicateStep) endStep).getStartKey().isPresent())
                variables.add(Scoping.Variable.END);
        } else if (endStep instanceof WhereTraversalStep.WhereEndStep) {
            if (!((WhereTraversalStep.WhereEndStep) endStep).getScopeKeys().isEmpty())
                variables.add(Scoping.Variable.END);
        } else if (endStep instanceof MatchStep.MatchEndStep) {
            if (((MatchStep.MatchEndStep) endStep).getMatchKey().isPresent())
                variables.add(Scoping.Variable.END);
        } else if (!endStep.getLabels().isEmpty())
            variables.add(Scoping.Variable.END);
        ///
        return variables;
    }

    public static boolean onGraphComputer(Traversal.Admin<?, ?> traversal) {
        while (!(traversal.isRoot())) {
            if (traversal.getParent() instanceof TraversalVertexProgramStep)
                return true;
            traversal = traversal.getParent().asStep().getTraversal();
        }
        return false;
    }

    public static void removeAllSteps(final Traversal.Admin<?, ?> traversal) {
        final int size = traversal.getSteps().size();
        for (int i = 0; i < size; i++) {
            traversal.removeStep(0);
        }
    }

    public static void copyLabels(final Step<?, ?> fromStep, final Step<?, ?> toStep, final boolean moveLabels) {
        if (!fromStep.getLabels().isEmpty()) {
            for (final String label : moveLabels ? new LinkedHashSet<>(fromStep.getLabels()) : fromStep.getLabels()) {
                toStep.addLabel(label);
                if (moveLabels)
                    fromStep.removeLabel(label);
            }
        }
    }

    public static boolean hasAllStepsOfClass(final Traversal.Admin<?, ?> traversal, final Class<?>... classesToCheck) {
        for (final Step step : traversal.getSteps()) {
            boolean foundInstance = false;
            for (final Class<?> classToCheck : classesToCheck) {
                if (classToCheck.isInstance(step)) {
                    foundInstance = true;
                    break;
                }
            }
            if (!foundInstance)
                return false;
        }
        return true;
    }

    public static boolean hasStepOfClass(final Traversal.Admin<?, ?> traversal, final Class<?>... classesToCheck) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            for (final Class<?> classToCheck : classesToCheck) {
                if (classToCheck.isInstance(step))
                    return true;
            }
        }
        return false;
    }

    /**
     * @deprecated As of release 3.5.2, not replaced as strategies are not applied in this fashion after 3.5.0
     */
    @Deprecated
    public static void applySingleLevelStrategies(final Traversal.Admin<?, ?> parentTraversal,
                                                  final Traversal.Admin<?, ?> childTraversal,
                                                  final Class<? extends TraversalStrategy> stopAfterStrategy) {
        childTraversal.setStrategies(parentTraversal.getStrategies());
        childTraversal.setSideEffects(parentTraversal.getSideEffects());
        parentTraversal.getGraph().ifPresent(childTraversal::setGraph);
        for (final TraversalStrategy<?> strategy : parentTraversal.getStrategies()) {
            strategy.apply(childTraversal);
            if (null != stopAfterStrategy && stopAfterStrategy.isInstance(strategy))
                break;
        }
    }

    /**
     * Used to left-fold a {@link HasContainer} to a {@link HasContainerHolder} if it exists. Else, append a {@link HasStep}.
     *
     * @param traversal    the traversal to fold or append.
     * @param hasContainer the container to add left or append.
     * @param <T>          the traversal type
     * @return the has container folded or appended traversal
     */
    public static <T extends Traversal.Admin<?, ?>> T addHasContainer(final T traversal, final HasContainer hasContainer) {
        if (traversal.getEndStep() instanceof HasContainerHolder) {
            ((HasContainerHolder) traversal.getEndStep()).addHasContainer(hasContainer);
            return traversal;
        } else
            return (T) traversal.addStep(new HasStep<>(traversal, hasContainer));
    }

    /**
     * Used to get PopInstruction of a traversal. Pop Instruction includes the labels it needs, and the pop type for each label.
     *
     * @param traversal     the traversal to get Scope Context for
     * @param <T>           the traversal type
     * @return              A Set of {@link org.apache.tinkerpop.gremlin.process.traversal.step.PopContaining.PopInstruction} values which contain the label and Pop value
     */
    public static <T extends  Traversal.Admin<?, ?>> Set<PopContaining.PopInstruction> getPopInstructions(final T traversal) {
        final Set<PopContaining.PopInstruction> scopingInfos = new HashSet<>();
        for (final Step step: traversal.getSteps()) {
            if (step instanceof PopContaining) {
                scopingInfos.addAll(((PopContaining) step).getPopInstructions());
            }
        }
        return scopingInfos;
    }

}
