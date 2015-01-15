package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.process.computer.ComputerResult;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.BulkSet;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/**
 * A {@link Traversal} represents a directed walk over a {@link Graph}.
 * This is the base interface for all traversal's, where each extending interface is seen as a domain specific language.
 * For example, {@link GraphTraversal} is a domain specific language for traversing a graph using "graph concepts" (e.g. vertices, edges).
 * Another example may represent the graph using "social concepts" (e.g. people, cities, artifacts).
 * A {@link Traversal} is evaluated in one of two ways: {@link TraversalEngine#STANDARD} (OLTP) and {@link TraversalEngine#COMPUTER} (OLAP).
 * OLTP traversals leverage an iterator and are executed within a single JVM (with data access allowed to be remote).
 * OLAP traversals leverage {@link GraphComputer} and are executed between multiple JVMs (and/or cores).
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Traversal<S, E> extends Iterator<E>, Cloneable {

    /**
     * Used for reflection based access to the static "of" method of a Traversal.
     */
    public static final String OF = "of";

    /**
     * Get access to administrative methods of the traversal via its accompanying {@link Traversal.Admin}.
     *
     * @return the admin of this traversal
     */
    public Admin<S, E> asAdmin();

    /**
     * Submit the traversal to a {@link GraphComputer} for OLAP execution.
     * This method should apply the traversal strategies for {@link TraversalEngine#COMPUTER}.
     * Then register and execute the traversal via {@link TraversalVertexProgram}.
     * Then wrap the {@link ComputerResult} in a new {@link Traversal} containing a {@link ComputerResultStep}.
     *
     * @param computer the GraphComputer to execute the traversal on
     * @return a new traversal with the starts being the results of the TraversalVertexProgram
     */
    public default Traversal<S, E> submit(final GraphComputer computer) {
        try {
            this.asAdmin().applyStrategies(TraversalEngine.COMPUTER);
            final TraversalVertexProgram vertexProgram = TraversalVertexProgram.build().traversal(() -> this).create();
            final ComputerResult result = computer.program(vertexProgram).submit().get();
            final GraphTraversal<S, S> traversal = new DefaultGraphTraversal<>(result.graph().getClass());
            return traversal.asAdmin().addStep(new ComputerResultStep<>(traversal, result, vertexProgram, true));
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
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
     * Add all the results of the traversal to the provided collection.
     *
     * @param collection the collection to fill
     * @return the collection now filled
     */
    public default <C extends Collection<E>> C fill(final C collection) {
        try {
            this.asAdmin().applyStrategies(TraversalEngine.STANDARD);
            // use the end step so the results are bulked
            final Step<?, E> endStep = TraversalHelper.getEnd(this);
            while (true) {
                final Traverser<E> traverser = endStep.next();
                TraversalHelper.addToCollection(collection, traverser.get(), traverser.bulk());
            }
        } catch (final NoSuchElementException ignored) {
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
    public default Traversal iterate() {
        try {
            this.asAdmin().applyStrategies(TraversalEngine.STANDARD);
            // use the end step so the results are bulked
            final Step<?, E> endStep = TraversalHelper.getEnd(this);
            while (true) {
                endStep.next();
            }
        } catch (final NoSuchElementException ignored) {
        }
        return this;
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

        }
    }

    /**
     * Cloning is used to duplicate the traversal typically in OLAP environments.
     *
     * @return The cloned traversal
     */
    public Traversal<S, E> clone() throws CloneNotSupportedException;

    /**
     * A collection of {@link Exception} types associated with Traversal execution.
     */
    public static class Exceptions {

        public static IllegalStateException traversalIsLocked() {
            return new IllegalStateException("The traversal strategies are complete and the traversal can no longer have steps added to it");
        }

        public static IllegalStateException traversalIsNotReversible() {
            return new IllegalStateException("The traversal is not reversible as it contains steps that are not reversible");
        }
    }

    public interface Admin<S, E> extends Traversal<S, E> {

        /**
         * Add an iterator of {@link Traverser} objects to the head/start of the traversal.
         * Users should typically not need to call this method. For dynamic inject of data, they should use {@link com.tinkerpop.gremlin.process.graph.step.sideEffect.InjectStep}.
         *
         * @param starts an iterators of traversers
         */
        public void addStarts(final Iterator<Traverser<S>> starts);

        /**
         * Add a single {@link Traverser} object to the head of the traversal.
         * Users should typically not need to call this method. For dynamic inject of data, they should use {@link com.tinkerpop.gremlin.process.graph.step.sideEffect.InjectStep}.
         *
         * @param start a traverser to add to the traversal
         */
        public default void addStart(final Traverser<S> start) {
            this.addStarts(IteratorUtils.of(start));
        }

        /**
         * Get the {@link Step} instances associated with this traversal.
         * The steps are ordered according to their linked list structure as defined by {@link Step#getPreviousStep()} and {@link Step#getNextStep()}.
         *
         * @return the ordered steps of the traversal
         */
        public List<Step> getSteps();

        /**
         * Add a {@link Step} to the end of the traversal. This method should automatically re-link the step accordingly (see {@link TraversalHelper#reLinkSteps}).
         * If the {@link TraversalStrategies} have already been applied, then an {@link IllegalStateException} is throw stating the traversal is locked.
         *
         * @param step the step to add
         * @param <E2> the output of the step
         * @return the updated traversal
         */
        public default <E2> Traversal<S, E2> addStep(final Step<?, E2> step) throws IllegalStateException {
            return this.addStep(this.getSteps().size(), step);
        }

        public <S2, E2> Traversal<S2, E2> addStep(final int index, final Step<?, ?> step) throws IllegalStateException;

        public default <S2, E2> Traversal<S2, E2> removeStep(final Step<?, ?> step) throws IllegalStateException {
            return this.removeStep(this.getSteps().indexOf(step));
        }

        public <S2, E2> Traversal<S2, E2> removeStep(final int index) throws IllegalStateException;

        /**
         * Apply the registered {@link TraversalStrategies} to the traversal.
         * Once the strategies are applied, the traversal is "locked" and can no longer have steps added to it.
         *
         * @param engine the engine that will ultimately execute the traversal.
         */
        public void applyStrategies(final TraversalEngine engine);

        /**
         * When the {@link TraversalStrategies} have been applied, the destined {@link TraversalEngine} has been declared.
         * Once a traversal engine has been declared, the traversal can no longer be extended, only executed.
         *
         * @return whether the traversal engine has been defined or not.
         */
        public Optional<TraversalEngine> getTraversalEngine();

        /**
         * Get the {@link TraverserGenerator} associated with this traversal.
         * The traversal generator creates {@link Traverser} instances that are respective of the traversal definition.
         *
         * @return the generator of traversers
         */
        public default TraverserGenerator getTraverserGenerator() {
            return this.getStrategies().getTraverserGenerator(this);
        }

        /**
         * Call the {@link Step#reset} method on every step in the traversal.
         */
        public default void reset() {
            this.getSteps().forEach(Step::reset);
        }

        /**
         * Assume the every {@link Step} implements {@link Reversible} and call {@link Reversible#reverse()} for each.
         *
         * @return the traversal with its steps reversed
         */
        public default Traversal<S, E> reverse() throws IllegalStateException {
            if (!TraversalHelper.isReversible(this)) throw Exceptions.traversalIsNotReversible();
            this.getSteps().stream().forEach(step -> ((Reversible) step).reverse());
            return this;
        }

        /**
         * Set the {@link TraversalSideEffects} of this traversal.
         *
         * @param sideEffects the sideEffects to set for this traversal.
         */
        public void setSideEffects(final TraversalSideEffects sideEffects);

        /**
         * Get the {@link TraversalSideEffects} associated with the traversal.
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

        public void setTraversalHolder(final TraversalHolder<?,?> step);

        public TraversalHolder<?,?> getTraversalHolder();

    }

}
