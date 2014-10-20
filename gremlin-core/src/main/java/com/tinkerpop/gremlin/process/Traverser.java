package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.detached.Attachable;

import java.io.Serializable;

/**
 * A {@link Traverser} represents the current state of an object flowing through a {@link Traversal}.
 * Different types of traverser can exist depending on the semantics of the traversal and the desire for
 * space/time optimizations of the developer.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Traverser<T> extends Serializable, Comparable<Traverser<T>> {

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
    public Path path();

    /**
     * Determine whether the traverser has path information.
     *
     * @return whether the traverser has path information
     */
    public boolean hasPath();

    /**
     * Return the number of times the traverser has gone through a looping section of a traversal.
     *
     * @return The number of times the traverser has gone through a loop
     */
    public short loops();

    /**
     * A traverser may represent a grouping of traversers to allow for more efficient data propagation.
     *
     * @return the number of traversers represented in this traverser.
     */
    public long bulk();

    /**
     * Get the sideEffects associated with the traversal of the traverser.
     *
     * @return the traversal sideEffects of the traverser
     */
    public Traversal.SideEffects sideEffects();

    /**
     * Get a particular value from the sideEffects of the traverser.
     *
     * @param sideEffectKey the key of the value to get from the sideEffects
     * @param <A>           the type of the returned object
     * @return the object in the sideEffects of the respective key
     */
    public default <A> A get(final String sideEffectKey) {
        return this.sideEffects().get(sideEffectKey);
    }

    /**
     * If the underlying object of the traverser is comparable, compare it with the other traverser.
     *
     * @param other the other traverser that presumably has a comparable internal object
     * @return the comparison of the two objects of the traversers
     * @throws ClassCastException if the object of the traverser is not comparable
     */
    public default int compareTo(final Traverser<T> other) throws ClassCastException {
        final T a = this.get();
        if (a instanceof Comparable)
            return ((Comparable) a).compareTo(other.get());
        else
            throw new ClassCastException("The traverser does not contain a comparable object: " + a.getClass());
    }

    /**
     * Typecast the traverser to a "system traverser" so {@link com.tinkerpop.gremlin.process.Traverser.Admin} methods can be accessed.
     * Used as a helper method to avoid the awkwardness of <code>((Traverser.Administrative)traverser)</code>.
     *
     * @return The type-casted traverser
     */
    public default Admin<T> asAdmin() {
        return (Admin<T>) this;
    }

    /**
     * The methods in System.Traverser are useful to underlying Step and Traversal implementations.
     * They should not be accessed by the user during lambda-based manipulations.
     */
    public interface Admin<T> extends Traverser<T>, Attachable<Admin<T>> {

        public static final String DONE = Graph.System.system("done");

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
         * Set the number of traversers represented by this traverser.
         *
         * @param count the number of traversers
         */
        public void setBulk(final long count);

        /**
         * If the traverser has "no future" then it is done with its lifecycle.
         *
         * @return Whether the traverser is done executing or not
         */
        public default boolean isDone() {
            return getFuture().equals(DONE);
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
        public <R> Admin<R> makeChild(final String label, final R r);

        /**
         * Generate a sibling traverser of the current traverser with a full copy of all state within the sibling.
         *
         * @return The sibling traverser
         */
        public Admin<T> makeSibling();

        /**
         * Prepare the traverser for migration across a JVM boundary.
         *
         * @return The deflated traverser
         */
        public Admin<T> detach();

        /**
         * Regenerate the detached traverser given its location at a particular vertex.
         *
         * @param hostVertex The vertex that is hosting the traverser
         * @return The inflated traverser
         */
        public Admin<T> attach(final Vertex hostVertex);

        /**
         * Traversers can not attach to graphs and thus, an {@link UnsupportedOperationException} is thrown.
         *
         * @param graph the graph to attach the traverser to, which it can't.
         * @return nothing as an exception is thrown
         * @throws UnsupportedOperationException is always thrown as it makes no sense to attach a traverser to a graph
         */
        @Override
        public default Admin<T> attach(final Graph graph) throws UnsupportedOperationException {
            throw new UnsupportedOperationException("A traverser can only exist a vertices, not the graph");
        }

        /**
         * Set the sideEffects of the {@link Traversal}. Given that traversers can move between machines,
         * it may be important to re-set this when the traverser cross machine boundaries and typically during inflate().
         *
         * @param sideEffects the sideEffects of the traversal.
         */
        public void setSideEffects(final Traversal.SideEffects sideEffects);
    }
}
