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

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.remote.traversal.step.map.RemoteStep;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DiscardStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.function.TraverserSetSupplier;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A {@link Traversal} represents a directed walk over a {@link Graph}.
 * This is the base interface for all traversal's, where each extending interface is seen as a domain specific language.
 * For example, {@link GraphTraversal} is a domain specific language for traversing a graph using "graph concepts" (e.g. vertices, edges).
 * Another example may represent the graph using "social concepts" (e.g. people, cities, artifacts).
 * A {@link Traversal} is evaluated in one of two ways: iterator-based OLTP or {@link GraphComputer}-based OLAP.
 * OLTP traversals leverage an iterator and are executed within a single JVM (with data access allowed to be remote).
 * OLAP traversals leverage {@link GraphComputer} and are executed between multiple JVMs (and/or cores).
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Traversal<S, E> extends Iterator<E>, Serializable, Cloneable, AutoCloseable {

    public static class Symbols {
        private Symbols() {
            // static fields only
        }

        public static final String profile = "profile";
        public static final String discard = "discard";
    }

    /**
     * Get access to administrative methods of the traversal via its accompanying {@link Traversal.Admin}.
     *
     * @return the admin of this traversal
     */
    public default Traversal.Admin<S, E> asAdmin() {
        return (Traversal.Admin<S, E>) this;
    }

    /**
     * Return an {@link Optional} of the next E object in the traversal.
     * If the traversal is empty, then an {@link Optional#empty()} is returned.
     *
     * @return an optional of the next object in the traversal
     */
    public default Optional<E> tryNext() {
        return this.hasNext() ? Optional.of(this.next()) : Optional.empty();
    }

    /**
     * Get the next n-number of results from the traversal.
     * If the traversal has less than n-results, then only that number of results are returned.
     *
     * @param amount the number of results to get
     * @return the n-results in a {@link List}
     */
    public default List<E> next(final int amount) {
        final List<E> result = new ArrayList<>();
        int counter = 0;
        while (counter++ < amount && this.hasNext()) {
            result.add(this.next());
        }
        return result;
    }

    /**
     * Put all the results into an {@link ArrayList}.
     *
     * @return the results in a list
     */
    public default List<E> toList() {
        return this.fill(new ArrayList<>());
    }

    /**
     * Put all the results into a {@link HashSet}.
     *
     * @return the results in a set
     */
    public default Set<E> toSet() {
        return this.fill(new HashSet<>());
    }

    /**
     * Put all the results into a {@link BulkSet}.
     * This can reduce both time and space when aggregating results by ensuring a weighted set.
     *
     * @return the results in a bulk set
     */
    public default BulkSet<E> toBulkSet() {
        return this.fill(new BulkSet<>());
    }

    /**
     * Return the traversal as a {@link Stream}.
     *
     * @return the traversal as a stream.
     */
    public default Stream<E> toStream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this, Spliterator.IMMUTABLE | Spliterator.SIZED), false);
    }

    /**
     * Starts a promise to execute a function on the current {@code Traversal} that will be completed in the future.
     * Note that this method can only be used if the {@code Traversal} is constructed using
     * {@link AnonymousTraversalSource#withRemote(Configuration)}. Calling this method otherwise will yield an
     * {@code IllegalStateException}.
     */
    public default <T> CompletableFuture<T> promise(final Function<Traversal<S, E>, T> traversalFunction) {
        // apply strategies to see if RemoteStrategy has any effect (i.e. add RemoteStep)
        if (!this.asAdmin().isLocked()) this.asAdmin().applyStrategies();

        // use the end step so the results are bulked
        final Step<?, E> endStep = this.asAdmin().getEndStep();
        if (endStep instanceof RemoteStep) {
            return ((RemoteStep) endStep).promise().thenApply(traversalFunction);
        } else {
            throw new IllegalStateException("Only traversals created using withRemote() can be used in an async way");
        }
    }

    /**
     * Add all the results of the traversal to the provided collection.
     *
     * @param collection the collection to fill
     * @return the collection now filled
     */
    public default <C extends Collection<E>> C fill(final C collection) {
        try {
            if (!this.asAdmin().isLocked()) this.asAdmin().applyStrategies();
            // use the end step so the results are bulked
            final Step<?, E> endStep = this.asAdmin().getEndStep();
            while (true) {
                final Traverser<E> traverser = endStep.next();
                TraversalHelper.addToCollection(collection, traverser.get(), traverser.bulk());
            }
        } catch (final NoSuchElementException ignored) {
        } finally {
            CloseableIterator.closeIterator(this);
        }
        return collection;
    }

    /**
     * Iterate all the {@link Traverser} instances in the traversal.
     * What is returned is the empty traversal.
     * It is assumed that what is desired from the computation is are the sideEffects yielded by the traversal.
     *
     * @return the fully drained traversal
     */
    public default <A, B> Traversal<A, B> iterate() {
        try {
            if (!this.asAdmin().isLocked()) {
                this.discard();
                this.asAdmin().applyStrategies();
            }
            // use the end step so the results are bulked
            final Step<?, E> endStep = this.asAdmin().getEndStep();
            while (true) {
                endStep.next();
            }
        } catch (final NoSuchElementException ignored) {
        } finally {
            CloseableIterator.closeIterator(this);
        }
        return (Traversal<A, B>) this;
    }

    /**
     * Filter all traversers in the traversal. This step has narrow use cases and is primarily intended for use as a
     * signal to remote servers that {@link #iterate()} was called. While it may be directly used, it is often a sign
     * that a traversal should be re-written in another form.
     *
     * @return the updated traversal with respective {@link DiscardStep}.
     */
    public default Traversal<S, E> discard() {
        this.asAdmin().getGremlinLang().addStep(Symbols.discard);
        return this.asAdmin().addStep(new DiscardStep<>(this.asAdmin()));
    }

    /**
     * Profile the traversal.
     *
     * @return the updated traversal with respective {@link ProfileSideEffectStep}.
     */
    public default Traversal<S, TraversalMetrics> profile() {
        this.asAdmin().getGremlinLang().addStep(Symbols.profile);
        return this.asAdmin()
                .addStep(new ProfileSideEffectStep<>(this.asAdmin(), ProfileSideEffectStep.DEFAULT_METRICS_KEY))
                .addStep(new SideEffectCapStep<Object, TraversalMetrics>(this.asAdmin(), ProfileSideEffectStep.DEFAULT_METRICS_KEY));
    }

    /**
     * Return a {@link TraversalExplanation} that shows how this traversal will mutate with each applied {@link TraversalStrategy}.
     *
     * @return a traversal explanation
     */
    public default TraversalExplanation explain() {
        if (this.asAdmin().isLocked())
            throw new IllegalStateException("The traversal is locked and can not be explained on a strategy-by-strategy basis");
        return new TraversalExplanation(this.asAdmin());
    }

    /**
     * A traversal can be rewritten such that its defined end type E may yield objects of a different type.
     * This helper method allows for the casting of the output to the known the type.
     *
     * @param endType  the true output type of the traversal
     * @param consumer a {@link Consumer} to process each output
     * @param <E2>     the known output type of the traversal
     */
    public default <E2> void forEachRemaining(final Class<E2> endType, final Consumer<E2> consumer) {
        try {
            while (true) {
                consumer.accept((E2) next());
            }
        } catch (final NoSuchElementException ignore) {

        }  finally {
            CloseableIterator.closeIterator(this);
        }
    }

    @Override
    public default void forEachRemaining(final Consumer<? super E> action) {
        try {
            while (true) {
                action.accept(next());
            }
        } catch (final NoSuchElementException ignore) {

        }  finally {
            CloseableIterator.closeIterator(this);
        }
    }

    /**
     * Releases resources opened in any steps that implement {@link AutoCloseable}. If this method is overridden,the
     * implementer should invoke {@link #notifyClose()}.
     */
    @Override
    public default void close() throws Exception {
        for (final Step<?, ?> step : this.asAdmin().getSteps()) {
            if (step instanceof AutoCloseable)
                ((AutoCloseable) step).close();
        }

        notifyClose();
    }

    /**
     * Gets a callback from {@link #close()} for additional operations specific to the {@link Traversal} implementation.
     * A good implementation will use {@link #close()} to release resources in steps and this method to release
     * resources specific to the {@link Traversal} implementations.
     */
    public default void notifyClose() {
        // do nothing by default
    }

    /**
     * A collection of {@link Exception} types associated with Traversal execution.
     */
    public static class Exceptions {

        public static IllegalStateException traversalIsLocked() {
            return new IllegalStateException("The traversal strategies are complete and the traversal can no longer be modulated");
        }

        public static IllegalStateException traversalIsNotReversible() {
            return new IllegalStateException("The traversal is not reversible as it contains steps that are not reversible");
        }
    }

    public interface Admin<S, E> extends Traversal<S, E> {

        /**
         * Get the {@link GremlinLang} associated with the construction of this traversal.
         *
         * @return the byte code representation of the traversal
         */
        public GremlinLang getGremlinLang();

        /**
         * Add an iterator of {@link Traverser.Admin} objects to the head/start of the traversal. Users should
         * typically not need to call this method. For dynamic inject of data, they should use {@link InjectStep}.
         *
         * @param starts an iterators of traversers
         */
        public default void addStarts(final Iterator<Traverser.Admin<S>> starts) {
            if (!this.isLocked()) this.applyStrategies();
            this.getStartStep().addStarts(starts);
        }

        /**
         * Add a single {@link Traverser.Admin} object to the head of the traversal. Users should typically not need
         * to call this method. For dynamic inject of data, they should use {@link InjectStep}.
         *
         * @param start a traverser to add to the traversal
         */
        public default void addStart(final Traverser.Admin<S> start) {
            if (!this.isLocked()) this.applyStrategies();
            this.getStartStep().addStart(start);
        }

        /**
         * Get the {@link Step} instances associated with this traversal.
         * The steps are ordered according to their linked list structure as defined by {@link Step#getPreviousStep()} and {@link Step#getNextStep()}.
         *
         * @return the ordered steps of the traversal
         */
        public List<Step> getSteps();

        /**
         * Add a {@link Step} to the end of the traversal. This method should link the step to its next and previous step accordingly.
         *
         * @param step the step to add
         * @param <E2> the output of the step
         * @return the updated traversal
         * @throws IllegalStateException if the {@link TraversalStrategies} have already been applied
         */
        public default <E2> Traversal.Admin<S, E2> addStep(final Step<?, E2> step) throws IllegalStateException {
            return this.addStep(this.getSteps().size(), step);
        }

        /**
         * Add a {@link Step} to an arbitrary point in the traversal.
         *
         * @param index the location in the traversal to insert the step
         * @param step  the step to add
         * @param <S2>  the new start type of the traversal (if the added step was a start step)
         * @param <E2>  the new end type of the traversal (if the added step was an end step)
         * @return the newly modulated traversal
         * @throws IllegalStateException if the {@link TraversalStrategies} have already been applied
         */
        public <S2, E2> Traversal.Admin<S2, E2> addStep(final int index, final Step<?, ?> step) throws IllegalStateException;

        /**
         * Remove a {@link Step} from the traversal.
         *
         * @param step the step to remove
         * @param <S2> the new start type of the traversal (if the removed step was a start step)
         * @param <E2> the new end type of the traversal (if the removed step was an end step)
         * @return the newly modulated traversal
         * @throws IllegalStateException if the {@link TraversalStrategies} have already been applied
         */
        public default <S2, E2> Traversal.Admin<S2, E2> removeStep(final Step<?, ?> step) throws IllegalStateException {
            return this.removeStep(TraversalHelper.stepIndex(step, this));
        }

        /**
         * Remove a {@link Step} from the traversal.
         *
         * @param index the location in the traversal of the step to be evicted
         * @param <S2>  the new start type of the traversal (if the removed step was a start step)
         * @param <E2>  the new end type of the traversal (if the removed step was an end step)
         * @return the newly modulated traversal
         * @throws IllegalStateException if the {@link TraversalStrategies} have already been applied
         */
        public <S2, E2> Traversal.Admin<S2, E2> removeStep(final int index) throws IllegalStateException;

        /**
         * Get the start/head of the traversal. If the traversal is empty, then an {@link EmptyStep} instance is returned.
         *
         * @return the start step of the traversal
         */
        public default Step<S, ?> getStartStep() {
            final List<Step> steps = this.getSteps();
            return steps.isEmpty() ? EmptyStep.instance() : steps.get(0);
        }

        /**
         * Get the end/tail of the traversal. If the traversal is empty, then an {@link EmptyStep} instance is returned.
         *
         * @return the end step of the traversal
         */
        public default Step<?, E> getEndStep() {
            final List<Step> steps = this.getSteps();
            return steps.isEmpty() ? EmptyStep.instance() : steps.get(steps.size() - 1);
        }

        /**
         * Apply the registered {@link TraversalStrategies} to the traversal.
         * Once the strategies are applied, the traversal is "locked" and can no longer have steps added to it.
         * The order of operations for strategy applications should be: globally id steps, apply each strategy in turn
         * to root traversal, then recursively to nested traversals.
         *
         * @throws IllegalStateException if the {@link TraversalStrategies} have already been applied
         */
        public void applyStrategies() throws IllegalStateException;

        /**
         * Get the {@link TraverserGenerator} associated with this traversal.
         * The traversal generator creates {@link Traverser} instances that are respective of the traversal's
         * {@link TraverserRequirement}.
         *
         * @return the generator of traversers
         */
        public TraverserGenerator getTraverserGenerator();

        /**
         * Gets a generator that creates new {@link TraverserSet} instances for steps in the traversal. Providers may
         * override this default implementation to provider their own {@link TraverserSet}.
         */
        public default Supplier<TraverserSet<S>> getTraverserSetSupplier() {
            return TraverserSetSupplier.instance();
        }

        /**
         * Get the set of all {@link TraverserRequirement}s for this traversal.
         *
         * @return the features of a traverser that are required to execute properly in this traversal
         */
        public Set<TraverserRequirement> getTraverserRequirements();

        /**
         * Call the {@link Step#reset} method on every step in the traversal.
         */
        public default void reset() {
            this.getSteps().forEach(Step::reset);
        }

        /**
         * Set the {@link TraversalSideEffects} of this traversal.
         *
         * @param sideEffects the sideEffects to set for this traversal.
         */
        public void setSideEffects(final TraversalSideEffects sideEffects);

        /**
         * Get the {@link TraversalSideEffects} associated with the traversal. This method should not be called
         * externally for purposes of retrieving side-effects as traversal results. Traversal results should only be
         * returned by way of the execution of the traversal itself. Should a side-effect of a traversal be needed it
         * should only be obtained by using {@link GraphTraversal#cap(String, String...)} so that the side-effect can
         * be included as part of the traversal iteration. Relying on this method to get side-effects in these
         * situations may not result in consistent behavior across all types of executions and environments (e.g.
         * remoting).
         *
         * @return The traversal sideEffects
         */
        public TraversalSideEffects getSideEffects();

        /**
         * Set the {@link TraversalStrategies} to be used by this traversal at evaluation time.
         *
         * @param strategies the strategies to use on this traversal
         */
        public void setStrategies(final TraversalStrategies strategies);

        /**
         * Get the {@link TraversalStrategies} associated with this traversal.
         *
         * @return the strategies associated with this traversal
         */
        public TraversalStrategies getStrategies();

        /**
         * Set the {@link TraversalParent} {@link Step} that is the parent of this traversal. Traversals can be nested
         * and this is the means by which the traversal tree is connected. If there is no parent, then it should be a
         * {@link EmptyStep}.
         *
         * @param step the traversal holder parent step or {@link EmptyStep} if it has no parent
         */
        public void setParent(final TraversalParent step);

        /**
         * Get the {@link TraversalParent} {@link Step} that is the parent of this traversal. Traversals can be nested
         * and this is the means by which the traversal tree is walked.
         *
         * @return the traversal holder parent step or {@link EmptyStep} if it has no parent.
         */
        public TraversalParent getParent();

        /**
         * Determines if the traversal is at the root level.
         */
        public default boolean isRoot() {
            return getParent() instanceof EmptyStep;
        }

        /**
         * Cloning is used to duplicate the traversal typically in OLAP environments.
         *
         * @return The cloned traversal
         */
        @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
        public Traversal.Admin<S, E> clone();

        /**
         * When the traversal has had its {@link TraversalStrategies} applied to it, it is locked.
         *
         * @return whether the traversal is locked
         */
        public boolean isLocked();

        /**
         * Lock the traversal and perform any final adjustments to it after strategy application.
         */
        public void lock();

        /**
         * Gets the {@link Graph} instance associated directly to this {@link Traversal} or through its parent.
         */
        public Optional<Graph> getGraph();

        /**
         * Gets the {@link TraversalSource} that spawned the {@link Traversal} instance initially if present. This
         * {@link TraversalSource} should have spawned from the associated {@link Graph} returned from
         * {@link #getGraph()}.
         */
        public default Optional<TraversalSource> getTraversalSource() {
            return Optional.empty();
        }

        public void setGraph(final Graph graph);

        public default boolean equals(final Traversal.Admin<S, E> other) {
            if (this.getClass().equals(other.getClass())) {
                final List<Step> steps = this.getSteps();
                final List<Step> otherSteps = other.getSteps();
                if (steps.size() == otherSteps.size()) {
                    for (int i = 0; i < steps.size(); i++) {
                        if (!steps.get(i).equals(otherSteps.get(i))) {
                            return false;
                        }
                    }
                    return true;
                }
            }
            return false;
        }

        public default Traverser.Admin<E> nextTraverser() {
            return this.getEndStep().next();
        }

    }

}
