package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.process.computer.ComputerResult;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.graph.step.util.PathIdentityStep;
import com.tinkerpop.gremlin.process.graph.strategy.GraphComputerStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.TraverserSourceStrategy;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Traversal<S, E> extends Iterator<E>, Cloneable {

    public static final String OF = "of";

    public SideEffects sideEffects();

    public Strategies strategies();

    public void addStarts(final Iterator<Traverser<S>> starts);

    public default <E2> Traversal<S, E2> addStep(final Step<?, E2> step) {
        TraversalHelper.insertStep(step, this.getSteps().size(), this);
        return (Traversal) this;
    }

    public List<Step> getSteps();

    public default void prepareForGraphComputer() {
        this.sideEffects().removeGraph();
        this.strategies().unregister(TraverserSourceStrategy.class);
        this.strategies().register(GraphComputerStrategy.instance());
    }

    public default Traversal<S, E> submit(final GraphComputer computer) {
        try {
            this.prepareForGraphComputer();
            this.strategies().apply();
            final TraversalVertexProgram vertexProgram = TraversalVertexProgram.build().traversal(() -> {
                try {
                    return this.clone();
                } catch (final CloneNotSupportedException e) {
                    throw new IllegalStateException(e.getMessage(), e);
                }
            }).create();
            final ComputerResult result = computer.program(vertexProgram).submit().get();
            final GraphTraversal<S, S> traversal = result.getGraph().of();
            return traversal.addStep(new ComputerResultStep<>(traversal, result, vertexProgram, true));
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public default void reset() {
        this.getSteps().forEach(Step::reset);
    }

    /**
     * Cloning is used to duplicate traversal typically in OLAP environments.
     *
     * @return The cloned traversal
     * @throws CloneNotSupportedException
     */
    public Traversal<S, E> clone() throws CloneNotSupportedException;

    public interface Strategies {

        public void register(final TraversalStrategy traversalStrategy);

        public void unregister(final Class<? extends TraversalStrategy> optimizerClass);

        public void clear();

        public void apply();

        public boolean complete();
    }

    public interface SideEffects {

        public default boolean exists(final String key) {
            return this.keys().contains(key);
        }

        public <V> void set(final String key, final V value);

        public <V> V get(final String key) throws IllegalArgumentException;

        public void remove(final String key);

        public Set<String> keys();

        public default boolean graphExists() {
            return this.exists(Graph.Key.hide("g"));
        }

        public default void setGraph(final Graph graph) {
            this.set(Graph.Key.hide("g"), graph);
        }

        public default Graph getGraph() {
            if (this.exists(Graph.Key.hide("g")))
                return this.<Graph>get(Graph.Key.hide("g"));
            else
                throw new IllegalStateException("There is no graph stored in these side effects");
        }

        public default void removeGraph() {
            this.remove(Graph.Key.hide("g"));
        }

        public default <V> V getOrCreate(final String key, final Supplier<V> orCreate) {
            if (this.exists(key))
                return this.<V>get(key);
            else {
                V t = orCreate.get();
                this.set(key, t);
                return t;
            }
        }

        public static class Exceptions {

            public static IllegalArgumentException sideEffectKeyValuesMustBeAMultipleOfTwo() {
                return new IllegalArgumentException("The provided side effect key/value array must be a multiple of two");
            }

            public static IllegalArgumentException sideEffectKeyValuesMustHaveALegalKeyOnEvenIndices() {
                return new IllegalArgumentException("The provided side effect key/value array must have a String key on even array indices");
            }

            public static IllegalArgumentException sideEffectKeyCanNotBeEmpty() {
                return new IllegalArgumentException("Side effect key can not be the empty string");
            }

            public static IllegalArgumentException sideEffectKeyCanNotBeNull() {
                return new IllegalArgumentException("Side effect key can not be null");
            }

            public static IllegalArgumentException sideEffectValueCanNotBeNull() {
                return new IllegalArgumentException("Side effect value can not be null");
            }

            public static IllegalArgumentException sideEffectDoesNotExist(final String key) {
                return new IllegalArgumentException("Side effects do not have a value for provided key: " + key);
            }

            public static UnsupportedOperationException dataTypeOfSideEffectValueNotSupported(final Object val) {
                return new UnsupportedOperationException(String.format("Side effect value [%s] is of type %s is not supported", val, val.getClass()));
            }
        }
    }

    /////////

    public default Traversal<S, E> trackPaths() {
        return (Traversal) this.addStep(new PathIdentityStep<>(this));
    }

    public default <E2> Traversal<S, E2> cap(final String sideEffecyKey) {
        return (Traversal) this.addStep(new SideEffectCapStep<>(this, sideEffecyKey));
    }

    public default <E2> Traversal<S, E2> cap() {
        return this.cap(TraversalHelper.getEnd(this).getLabel());
    }

    public default Traversal<S, Long> count() {
        return (Traversal) this.addStep(new CountStep<>(this));
    }

    public default Traversal<S, E> reverse() {
        this.getSteps().stream().filter(step -> step instanceof Reversible).forEach(step -> ((Reversible) step).reverse());
        return this;
    }

    public default List<E> next(final int amount) {
        final List<E> result = new ArrayList<>();
        int counter = 0;
        while (counter++ < amount && this.hasNext()) {
            result.add(this.next());
        }
        return result;
    }

    public default List<E> toList() {
        return (List<E>) this.fill(new ArrayList<>());
    }

    public default Set<E> toSet() {
        return (Set<E>) this.fill(new HashSet<>());
    }

    public default Collection<E> fill(final Collection<E> collection) {
        try {
            while (this.hasNext()) {
                collection.add(this.next());
            }
        } catch (final NoSuchElementException ignored) {
        }
        return collection;
    }

    public default Traversal iterate() {
        try {
            while (true) {
                this.next();
            }
        } catch (final NoSuchElementException ignored) {
        }
        return this;
    }

    public default void forEach(final Consumer<E> consumer) {
        try {
            while (this.hasNext()) {
                consumer.accept(this.next());
            }
        } catch (final NoSuchElementException ignored) {

        }
    }
}
