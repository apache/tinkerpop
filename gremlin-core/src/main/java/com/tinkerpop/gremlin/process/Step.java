package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

/**
 * A {@link Step} denotes a unit of computation within a {@link Traversal}.
 * A step takes an incoming object and yields an outgoing object.
 * Steps are chained together in a {@link Traversal} to yield a lazy function chain of computation.
 * <p/>
 * In the constructor of a Step, never store explicit sideEffect objects in {@link TraversalSideEffects}.
 * If a sideEffect needs to be registered with the {@link Traversal}, use SideEffects.registerSupplier().
 *
 * @param <S> The incoming object type of the step
 * @param <E> The outgoing object type of the step
 */
public interface Step<S, E> extends Iterator<Traverser<E>>, Cloneable {

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
     * If the step is  not labeled, then an {@link Optional#empty} is returned.
     *
     * @return the optional label of the step
     */
    public Optional<String> getLabel();

    /**
     * Set the label of this step.
     *
     * @param label the label for this step
     */
    public void setLabel(final String label);

    /**
     * Get the unique id of the step.
     * These ids can change when strategies are applied and anonymous traversals are embedded in the parent traversal.
     * A developer should typically not need to call this method.
     *
     * @param id the unique id of the step
     */
    public void setId(final String id);

    /**
     * Get the unique id of this step.
     *
     * @return the unique id of the step
     */
    public String getId();

    /**
     * Provide the necessary {@link com.tinkerpop.gremlin.process.traverser.TraverserRequirement} that must be met by the traverser in order for the step to function properly.
     * The provided default implements returns an empty set.
     *
     * @return the set of requirements
     */
    public default Set<TraverserRequirement> getRequirements() {
        return Collections.emptySet();
    }
}
