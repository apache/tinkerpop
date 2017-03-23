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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A Path denotes a particular walk through a {@link Graph} as defined by a {@link Traversal}.
 * In abstraction, any Path implementation maintains two lists: a list of sets of labels and a list of objects.
 * The list of labels are the labels of the steps traversed. The list of objects are the objects traversed.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Path extends Cloneable, Iterable<Object> {

    /**
     * Get the number of step in the path.
     *
     * @return the size of the path
     */
    public default int size() {
        return this.objects().size();
    }

    /**
     * Determine if the path is empty or not.
     *
     * @return whether the path is empty or not.
     */
    public default boolean isEmpty() {
        return this.size() == 0;
    }

    /**
     * Get the head of the path.
     *
     * @param <A> the type of the head of the path
     * @return the head of the path
     */
    public default <A> A head() {
        return (A) this.objects().get(this.size() - 1);
    }

    /**
     * Add a new step to the path with an object and any number of associated labels.
     *
     * @param object the new head of the path
     * @param labels the labels at the head of the path
     * @return the extended path
     */
    public Path extend(final Object object, final Set<String> labels);

    /**
     * Add labels to the head of the path.
     *
     * @param labels the labels at the head of the path
     * @return the path with added labels
     */
    public Path extend(final Set<String> labels);

    /**
     * Remove labels from path.
     *
     * @param labels the labels to remove
     * @return the path with removed labels
     */
    public Path retract(final Set<String> labels);

    /**
     * Get the object associated with the particular label of the path.
     * If the path as multiple labels of the type, then return a {@link List} of those objects.
     *
     * @param label the label of the path
     * @param <A>   the type of the object associated with the label
     * @return the object associated with the label of the path
     * @throws IllegalArgumentException if the path does not contain the label
     */
    public default <A> A get(final String label) throws IllegalArgumentException {
        final List<Object> objects = this.objects();
        final List<Set<String>> labels = this.labels();
        Object object = null;
        for (int i = 0; i < labels.size(); i++) {
            if (labels.get(i).contains(label)) {
                if (null == object) {
                    object = objects.get(i);
                } else if (object instanceof List) {
                    ((List) object).add(objects.get(i));
                } else {
                    final List list = new ArrayList(2);
                    list.add(object);
                    list.add(objects.get(i));
                    object = list;
                }
            }
        }
        if (null == object)
            throw Path.Exceptions.stepWithProvidedLabelDoesNotExist(label);
        return (A) object;
    }

    /**
     * Pop the object(s) associated with the label of the path.
     *
     * @param pop   first for least recent, last for most recent, and all for all in a list
     * @param label the label of the path
     * @param <A>   the type of the object associated with the label
     * @return the object associated with the label of the path
     * @throws IllegalArgumentException if the path does not contain the label
     */
    public default <A> A get(final Pop pop, final String label) throws IllegalArgumentException {
        if (Pop.all == pop) {
            if (this.hasLabel(label)) {
                final Object object = this.get(label);
                if (object instanceof List)
                    return (A) object;
                else
                    return (A) Collections.singletonList(object);
            } else {
                return (A) Collections.emptyList();
            }
        } else {
            final Object object = this.get(label);
            if (object instanceof List) {
                return Pop.last == pop ? ((List<A>) object).get(((List) object).size() - 1) : ((List<A>) object).get(0);
            } else
                return (A) object;
        }
    }

    /**
     * Get the object associated with the specified index into the path.
     *
     * @param index the index of the path
     * @param <A>   the type of the object associated with the index
     * @return the object associated with the index of the path
     */
    public default <A> A get(final int index) {
        return (A) this.objects().get(index);
    }

    /**
     * Return true if the path has the specified label, else return false.
     *
     * @param label the label to search for
     * @return true if the label exists in the path
     */
    public default boolean hasLabel(final String label) {
        return this.labels().stream().filter(labels -> labels.contains(label)).findAny().isPresent();
    }

    /**
     * An ordered list of the objects in the path.
     *
     * @return the objects of the path
     */
    public List<Object> objects();

    /**
     * An ordered list of the labels associated with the path
     * The set of labels for a particular step are ordered by the order in which {@link Path#extend(Object, Set)} was called.
     *
     * @return the labels of the path
     */
    public List<Set<String>> labels();

    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    public Path clone();

    /**
     * Determines whether the path is a simple or not.
     * A simple path has no cycles and thus, no repeated objects.
     *
     * @return Whether the path is simple or not
     */
    public default boolean isSimple() {
        final List<Object> objects = this.objects();
        for (int i = 0; i < objects.size() - 1; i++) {
            for (int j = i + 1; j < objects.size(); j++) {
                if (objects.get(i).equals(objects.get(j)))
                    return false;
            }
        }
        return true;
    }

    public default Iterator<Object> iterator() {
        return this.objects().iterator();
    }

    public default void forEach(final BiConsumer<Object, Set<String>> consumer) {
        final List<Object> objects = this.objects();
        final List<Set<String>> labels = this.labels();
        for (int i = 0; i < objects.size(); i++) {
            consumer.accept(objects.get(i), labels.get(i));
        }
    }

    public default Stream<Pair<Object, Set<String>>> stream() {
        final List<Set<String>> labels = this.labels();
        final List<Object> objects = this.objects();
        return IntStream.range(0, this.size()).mapToObj(i -> Pair.with(objects.get(i), labels.get(i)));
    }

    public default boolean popEquals(final Pop pop, final Object other) {
        if (!(other instanceof Path))
            return false;
        final Path otherPath = (Path) other;
        return !this.labels().stream().
                flatMap(Set::stream).
                filter(label -> !otherPath.hasLabel(label) || !otherPath.get(pop, label).equals(this.get(pop, label))).
                findAny().
                isPresent();
    }

    /**
     * Isolate a sub-path from the path object. The isolation is based solely on the path labels.
     * The to-label is inclusive. Thus, from "b" to "c" would isolate the example path as follows {@code a,[b,c],d}.
     * Note that if there are multiple path segments with the same label, then its the last occurrence that is isolated.
     * For instance, from "b" to "c" would be {@code a,b,[b,c,d,c]}.
     *
     * @param fromLabel The label to start recording the sub-path from.
     * @param toLabel   The label to end recording the sub-path to.
     * @return the isolated sub-path.
     */
    public default Path subPath(final String fromLabel, final String toLabel) {
        if (null == fromLabel && null == toLabel)
            return this;
        else {
            Path subPath = MutablePath.make();
            final int size = this.size();
            int fromIndex = -1;
            int toIndex = -1;
            for (int i = size - 1; i >= 0; i--) {
                final Set<String> labels = this.labels().get(i);
                if (-1 == fromIndex && labels.contains(fromLabel))
                    fromIndex = i;
                if (-1 == toIndex && labels.contains(toLabel))
                    toIndex = i;
            }
            if (null != fromLabel && -1 == fromIndex)
                throw Path.Exceptions.couldNotLocatePathFromLabel(fromLabel);
            if (null != toLabel && -1 == toIndex)
                throw Path.Exceptions.couldNotLocatePathToLabel(toLabel);

            if (fromIndex == -1)
                fromIndex = 0;
            if (toIndex == -1)
                toIndex = size-1;
            if (fromIndex > toIndex)
                throw Path.Exceptions.couldNotIsolatedSubPath(fromLabel, toLabel);
            for (int i = fromIndex; i <= toIndex; i++) {
                final Set<String> labels = this.labels().get(i);
                subPath.extend(this.get(i), labels);
            }
            return subPath;
        }
    }

    public static class Exceptions {

        public static IllegalArgumentException stepWithProvidedLabelDoesNotExist(final String label) {
            return new IllegalArgumentException("The step with label " + label + " does not exist");
        }

        public static IllegalArgumentException couldNotLocatePathFromLabel(final String fromLabel) {
            return new IllegalArgumentException("Could not locate path from-label: " + fromLabel);
        }

        public static IllegalArgumentException couldNotLocatePathToLabel(final String toLabel) {
            return new IllegalArgumentException("Could not locate path to-label: " + toLabel);
        }

        public static IllegalArgumentException couldNotIsolatedSubPath(final String fromLabel, final String toLabel) {
            return new IllegalArgumentException("Could not isolate path because from comes after to: " + fromLabel + "->" + toLabel);
        }
    }
}
