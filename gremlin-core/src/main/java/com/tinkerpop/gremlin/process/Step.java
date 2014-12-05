package com.tinkerpop.gremlin.process;

import java.util.Iterator;

/**
 * A {@link Step} denotes a unit of computation within a {@link Traversal}.
 * A step takes an incoming object and yields an outgoing object.
 * Steps are chained together in a {@link Traversal} to yield a lazy function chain of computation.
 * <p/>
 * In the constructor of a Step, never store explicit sideEffect objects in {@link com.tinkerpop.gremlin.process.Traversal.SideEffects}.
 * If a sideEffect needs to be registered with the {@link Traversal}, use SideEffects.registerSupplier().
 *
 * @param <S> The incoming object type of the step
 * @param <E> The outgoing object type of the step
 */
public interface Step<S, E> extends Iterator<Traverser<E>>, Cloneable {

    /**
     * A token object that denotes "nothing" and is used to declare an empty "spot" in a {@link Traversal}
     */
    public static final NoObject NO_OBJECT = new NoObject();

    /**
     * Add a iterator of {@link Traverser} objects of type S to the step.
     *
     * @param starts The iterator of objects to add
     */
    public void addStarts(final Iterator<Traverser<S>> starts);

    /**
     * Add a single {@link Traverser} to the step.
     *
     * @param start The traverser to add
     */
    public void addStart(final Traverser<S> start);

    /**
     * Set the step that is previous to the current step.
     * Used for linking steps together to form a function chain.
     *
     * @param step the previous step of this step
     */
    public void setPreviousStep(final Step<?, S> step);

    /**
     * Get the step prior to the current step.
     *
     * @return The previous step
     */
    public Step<?, S> getPreviousStep();

    /**
     * Set the step that is next to the current step.
     * Used for linking steps together to form a function chain.
     *
     * @param step the next step of this step
     */
    public void setNextStep(final Step<E, ?> step);

    /**
     * Get the next step to the current step.
     *
     * @return The next step
     */
    public Step<E, ?> getNextStep();

    /**
     * Get the {@link Traversal} that this step is contained within.
     *
     * @param <A> The incoming object type of the traversal
     * @param <B> The outgoing object type of the traversal
     * @return The traversal of this step
     */
    public <A, B> Traversal<A, B> getTraversal();

    /**
     * Set the {@link Traversal} that this step is contained within.
     *
     * @param traversal the new traversal for this step
     */
    public void setTraversal(final Traversal<?, ?> traversal);

    /**
     * Reset the state of the step such that it has no incoming starts.
     * Internal states are to be reset, but any sideEffect data structures are not to be recreated.
     */
    public void reset();

    /**
     * Cloning is used to duplicate steps for the purpose of traversal optimization and OLTP replication.
     * When cloning a step, it is important that the steps, the cloned step is equivalent to the state of the step when reset() is called.
     * Moreover, the previous and next steps should be set to {@link com.tinkerpop.gremlin.process.util.EmptyStep}.
     *
     * @return The cloned step
     * @throws CloneNotSupportedException
     */
    public Step<S, E> clone() throws CloneNotSupportedException;

    /**
     * Get the label of this step.
     *
     * @return the label of the step
     */
    public String getLabel();

    /**
     * Set the label of this step.
     *
     * @param label the label for this step
     */
    public void setLabel(final String label);

    /**
     * A static singleton denoting that the current "spot" in the Step contains no object.
     */
    static final class NoObject {

        private NoObject() {
        }

        public boolean equals(final Object object) {
            return object instanceof NoObject;
        }

        public int hashCode() {
            return 1212121212;
        }
    }
}
