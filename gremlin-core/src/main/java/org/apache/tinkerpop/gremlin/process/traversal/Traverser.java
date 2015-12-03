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

import org.apache.tinkerpop.gremlin.structure.util.Attachable;

import java.io.Serializable;
import java.util.Set;
import java.util.function.Function;

/**
 * A {@code Traverser} represents the current state of an object flowing through a {@link Traversal}.
 * A traverser maintains a reference to the current object, a traverser-local "sack", a traversal-global sideEffect,
 * a bulk count, and a path history.
 * <p/>
 * Different types of traversers can exist depending on the semantics of the traversal and the desire for
 * space/time optimizations of the developer.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Traverser<T> extends Serializable, Comparable<Traverser<T>>, Cloneable {

    /**
     * Get the object that the traverser is current at.
     *
     * @return The current object of the traverser
     */
    public T get();

    /**
     * Get the sack local sack object of this traverser.
     *
     * @param <S> the type of the sack object
     * @return the sack object
     */
    public <S> S sack();

    /**
     * Set the traversers sack object to the provided value ("sack the value").
     *
     * @param object the new value of the traverser's sack
     * @param <S>    the type of the object
     */
    public <S> void sack(final S object);

    /**
     * Get the current path of the traverser.
     *
     * @return The path of the traverser
     */
    public Path path();

    /**
     * Get the object associated with the specified step-label in the traverser's path history.
     *
     * @param stepLabel the step-label in the path to access
     * @param <A>       the type of the object
     * @return the object associated with that path label (if more than one object occurs at that step, a list is returned)
     */
    public default <A> A path(final String stepLabel) {
        return this.path().get(stepLabel);
    }

    public default <A> A path(final Pop pop, final String stepLabel) {
        return this.path().get(pop, stepLabel);
    }

    /**
     * Return the number of times the traverser has gone through a looping section of a traversal.
     *
     * @return The number of times the traverser has gone through a loop
     */
    public int loops();

    /**
     * A traverser may represent a grouping of traversers to allow for more efficient data propagation.
     *
     * @return the number of traversers represented in this traverser.
     */
    public long bulk();

    /**
     * Get a particular value from the sideEffects of the traverser.
     *
     * @param sideEffectKey the key of the value to get from the sideEffects
     * @param <A>           the type of the returned object
     * @return the object in the sideEffects of the respective key
     */
    public default <A> A sideEffects(final String sideEffectKey) {
        return this.asAdmin().getSideEffects().<A>get(sideEffectKey).get();
    }

    /**
     * Set a particular value in the sideEffects of the traverser.
     *
     * @param sideEffectKey   the key of the value to set int the sideEffects
     * @param sideEffectValue the value to set for the sideEffect key
     */
    public default void sideEffects(final String sideEffectKey, final Object sideEffectValue) {
        this.asAdmin().getSideEffects().set(sideEffectKey, sideEffectValue);
    }

    /**
     * If the underlying object of the traverser is comparable, compare it with the other traverser.
     *
     * @param other the other traverser that presumably has a comparable internal object
     * @return the comparison of the two objects of the traversers
     * @throws ClassCastException if the object of the traverser is not comparable
     */
    @Override
    public default int compareTo(final Traverser<T> other) throws ClassCastException {
        return ((Comparable) this.get()).compareTo(other.get());
    }

    /**
     * Typecast the traverser to a "system traverser" so {@link Traverser.Admin} methods can be accessed.
     * This is used as a helper method to avoid the awkwardness of <code>((Traverser.Administrative)traverser)</code>.
     * The default implementation simply returns "this" type casted to {@link Traverser.Admin}.
     *
     * @return The type-casted traverser
     */
    public default Admin<T> asAdmin() {
        return (Admin<T>) this;
    }

    /**
     * Traverser cloning is important when splitting a traverser at a bifurcation point in a traversal.
     */
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    public Traverser<T> clone();

    /**
     * The methods in System.Traverser are useful to underlying Step and Traversal implementations.
     * They should not be accessed by the user during lambda-based manipulations.
     */
    public interface Admin<T> extends Traverser<T>, Attachable<T> {

        public static final String HALT = "halt";

        /**
         * When two traversers are have equality with each other, then they can be merged.
         * This method is used to merge the traversers into a single traverser.
         * This is used for optimization where instead of enumerating all traversers, they can be counted.
         *
         * @param other the other traverser to merge into this traverser. Once merged, the other can be garbage collected.
         */
        public void merge(final Admin<?> other);

        /**
         * Generate a child traverser of the current traverser for current as step and new object location.
         * The child has the path history, future, and loop information of the parent.
         * The child extends that path history with the current as and provided R-object.
         *
         * @param r    The current object of the child
         * @param step The step yielding the split
         * @param <R>  The current object type of the child
         * @return The split traverser
         */
        public <R> Admin<R> split(final R r, final Step<T, R> step);

        /**
         * Generate a sibling traverser of the current traverser with a full copy of all state within the sibling.
         *
         * @return The split traverser
         */
        public Admin<T> split();

        public void addLabels(final Set<String> labels);

        /**
         * Set the current object location of the traverser.
         *
         * @param t The current object of the traverser
         */
        public void set(final T t);

        /**
         * Increment the number of times the traverser has gone through a looping section of traversal.
         * The step label is important to create a stack of loop counters when within a nested context.
         * If the provided label is not the same as the current label on the stack, add a new loop counter.
         * If the provided label is the same as the current label on the stack, increment the loop counter.
         *
         * @param stepLabel the label of the step that is doing the incrementing
         */
        public void incrLoops(final String stepLabel);

        /**
         * Set the number of times the traverser has gone through a loop back to 0.
         * When a traverser exits a looping construct, this method should be called.
         * In a nested loop context, the highest stack loop counter should be removed.
         */
        public void resetLoops();

        /**
         * Get the step id of where the traverser is located.
         * This is typically used in multi-machine systems that require the movement of
         * traversers between different traversal instances.
         *
         * @return The future step for the traverser
         */
        public String getStepId();

        /**
         * Set the step id of where the traverser is located.
         * If the future is {@link Traverser.Admin#HALT}, then {@link Traverser.Admin#isHalted()} is true.
         *
         * @param stepId The future step of the traverser
         */
        public void setStepId(final String stepId);

        /**
         * If the traverser has "no future" then it is done with its lifecycle.
         * This does not mean that the traverser is "dead," only that it has successfully passed through a
         * {@link Traversal}.
         *
         * @return Whether the traverser is done executing or not
         */
        public default boolean isHalted() {
            return getStepId().equals(HALT);
        }

        /**
         * Set the number of traversers represented by this traverser.
         *
         * @param count the number of traversers
         */
        public void setBulk(final long count);

        /**
         * Prepare the traverser for migration across a JVM boundary.
         *
         * @return The deflated traverser
         */
        public Admin<T> detach();

        /**
         * Regenerate the detached traverser given its location at a particular vertex.
         *
         * @param method The method by which to attach a {@code Traverser} to an vertex.
         * @return The inflated traverser
         */
        @Override
        public T attach(final Function<Attachable<T>, T> method);

        /**
         * Set the sideEffects of the {@link Traversal}. Given that traversers can move between machines,
         * it may be important to re-set this when the traverser crosses machine boundaries.
         *
         * @param sideEffects the sideEffects of the traversal.
         */
        public void setSideEffects(final TraversalSideEffects sideEffects);

        /**
         * Get the sideEffects associated with the traversal of the traverser.
         *
         * @return the traversal sideEffects of the traverser
         */
        public TraversalSideEffects getSideEffects();

        /**
         * Get the tags associated with the traverser.
         * Tags are used to categorize historic behavior of a traverser.
         * The returned set is mutable.
         *
         * @return the set of tags associated with the traverser.
         */
        public Set<String> getTags();
    }
}
