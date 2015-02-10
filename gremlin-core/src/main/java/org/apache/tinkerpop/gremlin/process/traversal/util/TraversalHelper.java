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

import org.apache.tinkerpop.gremlin.process.Step;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Reversible;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.StepPosition;
import org.apache.tinkerpop.gremlin.process.traversal.step.EmptyStep;
import org.apache.tinkerpop.gremlin.process.util.BulkSet;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalHelper {

    private TraversalHelper() {
    }

    public static boolean isReversible(final Traversal.Admin<?, ?> traversal) {
        return !traversal.getSteps().stream().filter(step -> !(step instanceof Reversible)).findAny().isPresent();
    }

    public static <S, E> Step<S, E> getStepByLabel(final String label, final Traversal.Admin<?, ?> traversal) {
        return traversal.getSteps().stream()
                .filter(step -> step.getLabel().isPresent())
                .filter(step -> label.equals(step.getLabel().get()))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("The provided step label does not exist: " + label));
    }

    public static <S, E> Step<S, E> getStepById(final String id, final Traversal.Admin<?, ?> traversal) {
        return traversal.getSteps().stream()
                .filter(step -> id.equals(step.getId()))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("The provided step id does not exist: " + id));
    }

    public static boolean hasLabel(final String label, final Traversal.Admin<?, ?> traversal) {
        return traversal.getSteps().stream()
                .filter(step -> step.getLabel().isPresent())
                .filter(step -> label.equals(step.getLabel().get()))
                .findAny().isPresent();
    }

    public static List<String> getLabelsUpTo(final Step<?, ?> step, final Traversal.Admin<?, ?> traversal) {
        final List<String> labels = new ArrayList<>();
        for (final Step<?, ?> temp : traversal.getSteps()) {
            if (temp == step) break;
            temp.getLabel().ifPresent(labels::add);
        }
        return labels;
    }

    public static List<Step<?, ?>> getStepsUpTo(final Step<?, ?> step, final Traversal.Admin<?, ?> traversal) {
        final List<Step<?, ?>> steps = new ArrayList<>();
        for (final Step temp : traversal.getSteps()) {
            if (temp == step) break;
            steps.add(temp);
        }
        return steps;
    }

    public static boolean isLocalStarGraph(final Traversal.Admin<?, ?> traversal) {
        char state = 'v';
        for (final Step step : traversal.getSteps()) {
            if (step instanceof PropertiesStep && state == 'u')
                return false;
            else if (step instanceof VertexStep) {
                if (Vertex.class.isAssignableFrom(((VertexStep) step).getReturnClass())) {
                    if (state == 'u') return false;
                    if (state == 'v') state = 'u';
                } else {
                    state = 'e';
                }
            } else if (step instanceof EdgeVertexStep) {
                if (state == 'e') state = 'u';
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

    public static <S, E> Step<?, E> insertTraversal(final Step<?, S> previousStep, final Traversal.Admin<S, E> insertTraversal, final Traversal.Admin<?, ?> traversal) {
        return TraversalHelper.insertTraversal(traversal.getSteps().indexOf(previousStep), insertTraversal, traversal);
    }

    public static <S, E> void insertBeforeStep(final Step<S, E> insertStep, final Step<E, ?> afterStep, final Traversal.Admin<?, ?> traversal) {
        traversal.addStep(traversal.getSteps().indexOf(afterStep), insertStep);
    }

    public static <S, E> void insertAfterStep(final Step<S, E> insertStep, final Step<?, S> beforeStep, final Traversal.Admin<?, ?> traversal) {
        traversal.addStep(traversal.getSteps().indexOf(beforeStep) + 1, insertStep);
    }

    public static <S, E> void replaceStep(final Step<S, E> removeStep, final Step<S, E> insertStep, final Traversal.Admin<?, ?> traversal) {
        traversal.addStep(traversal.getSteps().indexOf(removeStep), insertStep);
        traversal.removeStep(removeStep);
    }

    public static String makeStepString(final Step<?, ?> step, final Object... arguments) {
        final StringBuilder builder = new StringBuilder(step.getClass().getSimpleName());
        final List<String> strings = Stream.of(arguments)
                .filter(o -> null != o)
                .filter(o -> {
                    if (o instanceof TraversalRing) {
                        return ((TraversalRing) o).size() > 0;
                    } else if (o instanceof Collection) {
                        return ((Collection) o).size() > 0;
                    } else if (o instanceof Map) {
                        return ((Map) o).size() > 0;
                    } else {
                        return o.toString().length() > 0;
                    }
                })
                .map(o -> {
                    final String string = o.toString();
                    return string.contains("$") ? "lambda" : string;
                }).collect(Collectors.toList());
        if (strings.size() > 0) {
            builder.append('(');
            builder.append(String.join(",", strings));
            builder.append(')');
        }
        step.getLabel().ifPresent(label -> builder.append('@').append(label));
        // builder.append("^").append(step.getId());
        return builder.toString();
    }

    public static String makeTraversalString(final Traversal.Admin<?, ?> traversal) {
        return traversal.getSteps().toString();
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

    public static <S> List<S> getStepsOfAssignableClassRecurssively(final Class<S> stepClass, final Traversal.Admin<?, ?> traversal) {
        final List<S> list = new ArrayList<>();
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (stepClass.isAssignableFrom(step.getClass()))
                list.add((S) step);
            if (step instanceof TraversalParent) {
                for (final Traversal.Admin<?, ?> globalChild : ((TraversalParent) step).getGlobalChildren()) {
                    list.addAll(TraversalHelper.getStepsOfAssignableClassRecurssively(stepClass, globalChild));
                }
            }
        }
        return list;
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
            final TraversalParent holder = current.getParent();
            if (null == stepPosition.parentId && !(holder instanceof EmptyStep))
                stepPosition.parentId = holder.asStep().getId();
            if (-1 == stepPosition.z) {
                for (int i = 0; i < holder.getGlobalChildren().size(); i++) {
                    if (holder.getGlobalChildren().get(i) == current) {
                        stepPosition.z = i;
                    }
                }
            }
            current = holder.asStep().getTraversal();
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
}
