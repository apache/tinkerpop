package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;

/**
 * A {@link Traverser} represents the current state of an object flowing through a {@link Traversal}.
 * Different types of traverser can exist depending on the semantics of the traversal and the desire for
 * space/time optimizations of the developer.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Traverser<T> extends Serializable {

    /**
     * Get the object that the traverser is current at.
     *
     * @return The current object of the traverser
     */
    public T get();

    /**
     * Get the current path of the traverser.
     *
     * @return The path of the traverser
     */
    public Path getPath();

    /**
     * Return the number of times the traverser has gone through a looping section of a traversal.
     *
     * @return The number of times the traverser has gone through a loop
     */
    public int getLoops();

    public Traversal.SideEffects getSideEffects();

    public default <A> A get(final String sideEffectKey) {
        return this.getSideEffects().get(sideEffectKey);
    }

    /**
     * The methods in System.Traverser are useful to underlying Step and Traversal implementations.
     * They should not be accessed by the user during lambda-based manipulations.
     */
    public interface System<T> extends Traverser<T> {

        public static final String NO_FUTURE = "noFuture";

        /**
         * Set the current object location of the traverser.
         *
         * @param t The current object of the traverser
         */
        public void set(final T t);

        /**
         * Set the path of the traverser.
         * Typically used for serialization reasons when propagating traversers across machine boundaries.
         *
         * @param path The new path of the traverser
         */
        public void setPath(final Path path);

        /**
         * Increment the number of times the traverser has gone through a looping section of traversal.
         */
        public void incrLoops();

        /**
         * Set the number of times the traverser has gone through a loop back to 0.
         * When a traverser exists a looping contruct, this method should be called.
         */
        public void resetLoops();

        /**
         * Return the future step of the traverser as signified by the steps as-label.
         * This is typically used in multi-machine systems that require the movement of
         * traversers between different traversal instances.
         *
         * @return The future step for the traverser
         */
        public String getFuture();

        /**
         * Set the future of the traverser as signified by the step's label.
         *
         * @param label The future labeled step of the traverser
         */
        public void setFuture(final String label);

        /**
         * If the traverser has "no future" then it is done with its lifecycle.
         *
         * @return Whether the traverser is done executing or not
         */
        public default boolean isDone() {
            return getFuture().equals(NO_FUTURE);
        }

        /**
         * Generate a child traverser of the current traverser for current as step and new object location.
         * The child has the path history, future, and loop information of the parent.
         * The child extends that path history with the current as and provided R-object.
         *
         * @param label The current label of the child
         * @param r     The current object of the child
         * @param <R>   The current object type of the child
         * @return The newly created traverser
         */
        public <R> Traverser.System<R> makeChild(final String label, final R r);

        /**
         * Generate a sibling traverser of the current traverser with a full copy of all state within the sibling.
         *
         * @return The sibling traverser
         */
        public Traverser.System<T> makeSibling();

        /**
         * Prepare the traverser for migration across a JVM boundary.
         *
         * @return The deflated traverser
         */
        public Traverser.System<T> deflate();

        /**
         * Regenerate the deflated traverser given its location at a particular vertex.
         *
         * @param hostVertex The vertex that is hosting the traverser
         * @return The inflated traverser
         */
        public Traverser.System<T> inflate(final Vertex hostVertex, final Traversal traversal);

        public void setSideEffects(final Traversal.SideEffects sideEffects);
    }
}
