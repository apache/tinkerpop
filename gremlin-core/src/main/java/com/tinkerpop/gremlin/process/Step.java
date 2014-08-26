package com.tinkerpop.gremlin.process;

import java.io.Serializable;
import java.util.Iterator;

/**
 * A {@link Step} denotes a unit of computation within a {@link Traversal}.
 * A step takes an incoming object and yields an outgoing object.
 * Steps are chained together in a {@link Traversal} to yield a lazy function chain of computation.
 *
 * @param <S> The incoming object type of the step
 * @param <E> The outgoing object type of the step
 */
public interface Step<S, E> extends Iterator<Traverser<E>>, Serializable, Cloneable {

    /**
     * A token object that denotes "nothing" and is used to declare an empty spot in a {@link Traversal}
     */
    public static final NoObject NO_OBJECT = new NoObject();

    /**
     * Add a collection of {@link Traverser} objects of type S to the head of the step.
     *
     * @param iterator The iterator of objects to add
     */
    public void addStarts(final Iterator<Traverser<S>> iterator);

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
     * Reset the state of the step such that it has no incoming starts.
     */
    public void reset();

    /**
     * Cloning is used to duplicate steps for the purpose of traversal optimization.
     *
     * @return The cloned step
     * @throws CloneNotSupportedException
     */
    public Object clone() throws CloneNotSupportedException;

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
