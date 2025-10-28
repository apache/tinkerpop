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

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.GValueManager;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.AbstractLambdaTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.PopContaining;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailGlobalStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddElementStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CallStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LabelStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.RangeLocalStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TailLocalStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStepContract;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Utility class that provides functions that manipulate {@link Traversal} isntances. These functions are helpful when
 * writing {@link TraversalStrategy} implementations.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class TraversalHelper {

    /**
     * Registry mapping a contract/interface type to the explicit list of concrete Step classes that represent it.
     * This registry is intentionally simple and local to TraversalHelper.
     */
    private static final Map<Class<?>, List<Class<? extends Step>>> STEP_CONTRACT_REGISTRY = new HashMap<>() {{
        put(AddEdgeStepContract.class, AddEdgeStepContract.CONCRETE_STEPS);
        put(AddElementStepContract.class, AddElementStepContract.CONCRETE_STEPS);
        put(AddPropertyStepContract.class, AddPropertyStepContract.CONCRETE_STEPS);
        put(AddVertexStepContract.class, AddVertexStepContract.CONCRETE_STEPS);
        put(CallStepContract.class, CallStepContract.CONCRETE_STEPS);
        put(GraphStepContract.class, GraphStepContract.CONCRETE_STEPS);
        put(IsStepContract.class, IsStepContract.CONCRETE_STEPS);
        put(MergeStepContract.class, MergeStepContract.CONCRETE_STEPS);
        put(RangeGlobalStepContract.class, RangeGlobalStepContract.CONCRETE_STEPS);
        put(RangeLocalStepContract.class, RangeLocalStepContract.CONCRETE_STEPS);
        put(TailGlobalStepContract.class, TailGlobalStepContract.CONCRETE_STEPS);
        put(TailLocalStepContract.class, TailLocalStepContract.CONCRETE_STEPS);
        put(VertexStepContract.class, VertexStepContract.CONCRETE_STEPS);
    }};

    private TraversalHelper() { }

    /**
     * Determines whether the given traversal only touches local vertex/edge properties (i.e. does not traverse the
     * graph topology). This returns false if a step is encountered that walks the graph (e.g. VertexStep/EdgeVertexStep)
     * or if a repeat's global children contain such steps. TraversalParent children are inspected recursively.
     */
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

    /**
     * Determines whether a traversal is confined to a single-star neighborhood of a starting vertex (i.e., a
     * topologically local traversal over a vertex and its incident edges/adjacent vertices) without expanding further
     * into the graph or pulling arbitrary properties that would require remote access. This method delegates to a
     * state-machine helper and returns true if the traversal remains within the local star graph.
     */
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
            else if (step instanceof VertexStepContract) {
                if (state == 'u') return 'x';
                state = ((VertexStepContract) step).returnsVertex() ? 'u' : 'e';
            } else if (step instanceof EdgeVertexStep) {
                state = 'u';
            } else if (step instanceof HasContainerHolder && state == 'u') {
                List<HasContainer> hasContainers = ((HasContainerHolder) step).getHasContainers();
                for (final HasContainer hasContainer : hasContainers) {
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
     * Replace a step with a new step. When a step is replaced, it is also removed from the {@link GValueManager}.
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

    /**
     * Moves a step to a new position.
     *
     * @param stepToMove the step to move
     * @param indexToMoveTo the index in the traversal to move it to
     * @param traversal the traversal to move the step in which must be the same as the one assigned to the step
     */
    public static <S, E> void moveStep(final Step<S, E> stepToMove, final int indexToMoveTo, final Traversal.Admin<?, ?> traversal) {
        if (!traversal.getSteps().contains(stepToMove))
            throw new IllegalStateException("Traversal does not contain step");
        traversal.removeStep(stepToMove);
        traversal.addStep(indexToMoveTo, stepToMove);
    }

    /**
     * Inserts all steps from the supplied insertTraversal directly after the given previousStep in the specified
     * traversal, preserving their order.
     *
     * @param previousStep the step after which to insert the traversal
     * @param insertTraversal the traversal whose steps will be inserted
     * @param traversal the traversal to receive the steps
     * @return the last step that was inserted
     */
    public static <S, E> Step<?, E> insertTraversal(final Step<?, S> previousStep, final Traversal.Admin<S, E> insertTraversal, final Traversal.Admin<?, ?> traversal) {
        return TraversalHelper.insertTraversal(stepIndex(previousStep, traversal), insertTraversal, traversal);
    }

    /**
     * Inserts all steps from the supplied insertTraversal into the specified traversal starting at the given index.
     * Steps are inserted in order.
     *
     * @param insertIndex the position at which to start inserting
     * @param insertTraversal the traversal whose steps will be inserted
     * @param traversal the traversal to receive the steps
     * @return the last step that was inserted (or the existing step at insertIndex if no steps exist)
     */
    public static <S, E> Step<?, E> insertTraversal(final int insertIndex, final Traversal.Admin<S, E> insertTraversal, final Traversal.Admin<?, ?> traversal) {
        traversal.getGValueManager().mergeInto(insertTraversal.getGValueManager());
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

    /**
     * Removes steps from the traversal starting at startStep up to but not including endStep and appends them to
     * newTraversal, preserving order.
     *
     * @param startStep the first step to move
     * @param endStep the terminal step at which to stop (not moved)
     * @param newTraversal the traversal to receive the moved steps
     */
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

    /**
     * Returns all steps in the given traversal whose concrete class equals the supplied class. If the supplied class
     * is an interface registered as a step contract, the method will match by exact equality to any of the registered
     * concrete implementations for that contract. For example, calling this method with {@link GraphStepContract} will
     * not match on the interface but instead will match on its {@link GraphStepContract#CONCRETE_STEPS}.
     *
     * @param stepClass the concrete step type to match, or a registered contract interface
     * @param traversal the traversal to scan
     * @return a list of matching steps (preserving traversal order)
     */
    public static <S> List<S> getStepsOfClass(final Class<S> stepClass, final Traversal.Admin<?, ?> traversal) {
        final List<S> steps = new ArrayList<>();
        // If stepClass is an interface and registered as a contract, expand to its concrete classes
        final List<Class<? extends Step>> concrete;
        if (stepClass.isInterface()) {
            final List<Class<? extends Step>> reg = STEP_CONTRACT_REGISTRY.get(stepClass);
            concrete = reg != null ? reg : null;
        } else {
            concrete = null;
        }

        if (concrete == null) {
            // original behavior: exact equality to the provided class
            for (final Step step : traversal.getSteps()) {
                if (step.getClass().equals(stepClass))
                    steps.add((S) step);
            }
            return steps;
        }

        // contract expansion path: exact equality against any registered concrete classes
        for (final Step step : traversal.getSteps()) {
            final Class<?> sc = step.getClass();
            for (final Class<?> c : concrete) {
                if (sc.equals(c)) {
                    steps.add((S) step);
                    break;
                }
            }
        }
        return steps;
    }

    /**
     * Returns all steps in the given traversal that are instances of (i.e., assignable to) the supplied class.
     *
     * @param stepClass the class or interface to test with {@link Class#isAssignableFrom(Class)}
     * @param traversal the traversal to scan
     * @return a list of matching steps (preserving traversal order)
     */
    public static <S> List<S> getStepsOfAssignableClass(final Class<S> stepClass, final Traversal.Admin<?, ?> traversal) {
        final List<S> steps = new ArrayList<>();
        for (final Step step : traversal.getSteps()) {
            if (stepClass.isAssignableFrom(step.getClass()))
                steps.add((S) step);
        }
        return steps;
    }

    /**
     * Returns the last step in the traversal that is assignable to the supplied class, if present.
     *
     * @param stepClass the class or interface to test with {@link Class#isAssignableFrom(Class)}
     * @param traversal the traversal to scan
     * @return the last matching step or {@link Optional#empty()} if none found
     */
    public static <S> Optional<S> getLastStepOfAssignableClass(final Class<S> stepClass, final Traversal.Admin<?, ?> traversal) {
        final List<S> steps = TraversalHelper.getStepsOfAssignableClass(stepClass, traversal);
        return steps.size() == 0 ? Optional.empty() : Optional.of(steps.get(steps.size() - 1));
    }

    /**
     * Returns the first step in the traversal that is assignable to the supplied class, if present.
     *
     * @param stepClass the class or interface to test with {@link Class#isAssignableFrom(Class)}
     * @param traversal the traversal to scan
     * @return the first matching step or {@link Optional#empty()} if none found
     */
    public static <S> Optional<S> getFirstStepOfAssignableClass(final Class<S> stepClass, final Traversal.Admin<?, ?> traversal) {
        for (final Step step : traversal.getSteps()) {
            if (stepClass.isAssignableFrom(step.getClass()))
                return Optional.of((S) step);
        }
        return Optional.empty();
    }

    /**
     * Recursively collects steps assignable to the supplied class from the traversal and all its child traversals
     * (both local and global).
     *
     * @param stepClass the class or interface to test with {@link Class#isAssignableFrom(Class)}
     * @param traversal the root traversal to scan
     * @return a list of matching steps found anywhere in the traversal tree
     */
    public static <S> List<S> getStepsOfAssignableClassRecursively(final Class<S> stepClass, final Traversal.Admin<?, ?> traversal) {
        return getStepsOfAssignableClassRecursively(null, stepClass, traversal);
    }

    /**
     * Recursively collects steps assignable to the supplied class from the traversal and its child traversals scoped
     * by the given Scope.
     *
     * @param scope whether to include local, global, or both child traversals (null for both)
     * @param stepClass the class or interface to test with {@link Class#isAssignableFrom(Class)}
     * @param traversal the root traversal to scan
     * @return a list of matching steps found anywhere within the scoped traversal tree
     */
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
     * Recursively collects steps that are assignable to any of the supplied classes from the traversal and all its
     * child traversals (both local and global).
     *
     * @param traversal the root traversal to scan
     * @param stepClasses the classes or interfaces to test with {@link Class#isAssignableFrom(Class)}
     * @return a list of matching steps found anywhere in the traversal tree
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
     * Recursively collects steps assignable to any of the supplied classes from the traversal and children, then
     * orders the result by depth (deepest child steps first). Depth ordering is determined by {@link DepthComparator}.
     *
     * @param traversal the root traversal to scan
     * @param stepClasses the classes or interfaces to test with {@link Class#isAssignableFrom(Class)}
     * @return a list of matching steps ordered from deepest to shallowest
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

    /**
     * Determines whether the supplied traversal is a global child of its parent (as opposed to a local child).
     * Walks up the parent chain until the root to make this determination.
     *
     * @param traversal the traversal to test
     * @return true if the traversal is a global child; false if it is a local child
     */
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
     * Checks if the traversal only has steps that are equal to or assignable from the given step classes.
     *
     * @param stepClasses the collection of allowed step classes
     * @param traversal the traversal to check
     * @return true if all steps in the traversal are equal to or assignable from the given classes
     */
    public static boolean hasOnlyStepsOfAssignableClassesRecursively(final Collection<Class> stepClasses, final Traversal.Admin<?, ?> traversal) {
        for (final Step<?, ?> step : getStepsOfAssignableClassRecursively(Step.class, traversal)) {
            final boolean isSupported = stepClasses.stream()
                    .anyMatch(allowedClass -> allowedClass.isAssignableFrom(step.getClass()));
            if (!isSupported) {
                return false;
            }
        }
        return true;
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
     * @param consumer the function to apply
     * @param traversal the root traversal
     * @param applyToChildrenOnly if true, only child traversals receive the function (the root is skipped)
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

    /**
     * Adds the supplied element to the collection according to the provided bulk count. If the collection is a
     * {@link BulkSet}, the element is added with the given bulk. If it is a Set, the element is added once. Otherwise
     * the element is added repeatedly bulk times.
     *
     * @param collection the collection to mutate
     * @param s the element to add
     * @param bulk the bulk count
     */
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

    /**
     * Reassigns identifiers for every step in the supplied traversal using the provided StepPosition state object. The
     * StepPosition is mutated to reflect the current parent context (x/y/z/parentId) and each step receives a new id
     * via StepPosition#nextXId().
     *
     * @param stepPosition the position tracker to mutate and use to generate ids
     * @param traversal the traversal whose steps will be re-identified
     */
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

    /**
     * Returns the root traversal by walking up the parent chain until encountering an {@link EmptyStep} parent.
     *
     * @param traversal a traversal that may be nested
     * @return the root (top-most) traversal
     */
    public static Traversal.Admin<?, ?> getRootTraversal(Traversal.Admin<?, ?> traversal) {
        while (!((traversal.getParent()) instanceof EmptyStep)) {
            traversal = traversal.getParent().asStep().getTraversal();
        }
        return traversal;
    }

    /**
     * Determines whether any non-hidden labels are present anywhere in the traversal or its children.
     *
     * @param traversal the traversal to inspect
     * @return true if at least one non-hidden label exists; false otherwise
     */
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

    /**
     * Collects all labels (including hidden) present in the traversal and its child traversals.
     *
     * @param traversal the traversal to inspect
     * @return a set of labels
     */
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

    /**
     * Determines whether labels are referenced at the START and/or END of the traversal by inspecting the first and
     * last steps (and appropriate children for certain step types). Returned variables include START and/or END.
     *
     * @param traversal the traversal to inspect
     * @return a set containing zero, one, or both of {@link Scoping.Variable#START} and {@link Scoping.Variable#END}
     */
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

    /**
     * Determines if the traversal is executing on a {@link GraphComputer} by walking up the parent chain and checking
     * for a  {@link TraversalVertexProgramStep}.
     *
     * @param traversal the traversal to inspect
     * @return true if the traversal is under a TraversalVertexProgramStep; false otherwise
     */
    public static boolean onGraphComputer(Traversal.Admin<?, ?> traversal) {
        while (!(traversal.isRoot())) {
            if (traversal.getParent() instanceof TraversalVertexProgramStep)
                return true;
            traversal = traversal.getParent().asStep().getTraversal();
        }
        return false;
    }

    /**
     * Removes the specified step from the traversal and returns the traversal for chaining.
     *
     * @param stepToRemove the step to remove
     * @param traversal the traversal to mutate
     * @return the traversal argument for chaining
     */
    public static Traversal.Admin<?, ?> removeStep(final Step<?, ?> stepToRemove, final Traversal.Admin<?, ?> traversal) {
        return traversal.removeStep(stepToRemove);
    }

    /**
     * Removes all steps from the traversal.
     *
     * @param traversal the traversal to clear
     */
    public static void removeAllSteps(final Traversal.Admin<?, ?> traversal) {
        final int size = traversal.getSteps().size();
        for (int i = 0; i < size; i++) {
            traversal.removeStep(0);
        }
    }

    /**
     * Copies labels from one step to another. If moveLabels is true, labels are removed from the source after copying;
     * otherwise labels are left in place and only added to the target.
     *
     * @param fromStep the step to copy labels from
     * @param toStep the step to add labels to
     * @param moveLabels whether to remove labels from the source after copying
     */
    public static void copyLabels(final Step<?, ?> fromStep, final Step<?, ?> toStep, final boolean moveLabels) {
        if (!fromStep.getLabels().isEmpty()) {
            for (final String label : moveLabels ? new LinkedHashSet<>(fromStep.getLabels()) : fromStep.getLabels()) {
                toStep.addLabel(label);
                if (moveLabels)
                    fromStep.removeLabel(label);
            }
        }
    }

    /**
     * Tests whether every step in the traversal is an instance of at least one of the supplied classes.
     *
     * @param traversal the traversal to test
     * @param classesToCheck the classes to check with {@link Class#isInstance(Object)}
     * @return true if all steps match at least one class; false otherwise
     */
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

    /**
     * Tests whether any step in the traversal is an instance of any of the supplied classes.
     *
     * @param traversal the traversal to test
     * @param classesToCheck the classes to check with {@link Class#isInstance(Object)}
     * @return true if any step matches at least one class; false otherwise
     */
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
     * Used to left-fold a {@link HasContainer} to a {@link HasContainerHolder} if it exists. Else, append a {@link HasStep}.
     *
     * @param traversal    the traversal to fold or append.
     * @param hasContainer the container to add left or append.
     * @return the has container folded or appended traversal
     */
    public static <T extends Traversal.Admin<?, ?>> T addHasContainer(final T traversal, final HasContainer hasContainer) {
        if (hasContainer.getPredicate().isParameterized()) {
            traversal.getGValueManager().register(hasContainer.getPredicate().getGValues());
        }
        if (traversal.getEndStep() instanceof HasContainerHolder) {
            ((HasContainerHolder) traversal.getEndStep()).addHasContainer(hasContainer);
            if (hasContainer.getPredicate().isParameterized()) {
                traversal.getGValueManager().register(hasContainer.getPredicate().getGValues());
            }
            return traversal;
        } else {
            HasStep<?> step = new HasStep<>(traversal, hasContainer);
            if (hasContainer.getPredicate().isParameterized()) {
                traversal.getGValueManager().register(hasContainer.getPredicate().getGValues());
            }
            return (T) traversal.addStep(step);
        }
    }

    /**
     * Gathers all steps that implement {@link GValueHolder} and returns a map from step to the collection of
     * {@link GValue} instances they hold.
     *
     * @param traversal the traversal to scan recursively
     * @return a map of steps to their GValue collections
     */
    public static Map<Step, Collection<GValue<?>>> gatherStepGValues(final Traversal.Admin<?, ?> traversal) {
        Map<Step, Collection<GValue<?>>> gValues = new HashMap<>();
        applyTraversalRecursively(
                (t) -> {
                    for (final Step step : traversal.getSteps()) {
                        if (step instanceof GValueHolder) {
                            gValues.put(step, ((GValueHolder) step).getGValues());
                        }
                    }
                },
                traversal);
        return gValues;
    }

    /**
     * Gathers all steps that implement {@link GValueHolder} and returns them as a set. This is useful when only the
     * presence of placeholders is needed and not the values themselves.
     *
     * @param traversal the traversal to scan recursively
     * @return the set of steps that are {@link GValueHolder}s
     */
    public static Set<Step<?,?>> gatherGValuePlaceholders(final Traversal.Admin<?, ?> traversal) {
        final Set<Step<?,?>> steps = new HashSet<>();
        applyTraversalRecursively(
                (t) -> {
                    for (final Step step : traversal.getSteps()) {
                        if (step instanceof GValueHolder) {
                            steps.add(step);
                        }
                    }
                },
                traversal);
        return steps;
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

    /**
     * @param traversal the {@link Traversal.Admin} to check for a {@link RepeatStep} parent
     * @return true if a {@link RepeatStep} is a direct or indirect parent of the given {@link Traversal.Admin}, false otherwise
     */
    public static boolean hasRepeatStepParent(Traversal.Admin<?, ?> traversal) {
        while (!traversal.isRoot()) {
            if (traversal.getParent() instanceof RepeatStep) {
                return true;
            }
            traversal = traversal.getParent().asStep().getTraversal();
        }
        return false;
    }

}
