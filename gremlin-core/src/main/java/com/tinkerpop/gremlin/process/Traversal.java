package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.process.computer.ComputerResult;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.graph.step.util.PathIdentityStep;
import com.tinkerpop.gremlin.process.util.DefaultTraversal;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;

import java.io.Serializable;
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
public interface Traversal<S, E> extends Iterator<E>, Serializable {

    public static final String OF = "of";

    public SideEffects sideEffects();

    public Strategies strategies();

    public void addStarts(final Iterator<Traverser<S>> starts);

    public <S, E, T extends Traversal<S, E>> T addStep(final Step<?, E> step);

    public List<Step> getSteps();

    public default Traversal<S, E> submit(final GraphComputer computer) {
        try {
            final ComputerResult result = computer.program(TraversalVertexProgram.build().traversal(() -> this).create()).submit().get();
            final Traversal traversal = new DefaultTraversal<>();
            traversal.addStarts(new SingleIterator(result.getMemory()));
            return traversal;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public interface Strategies extends Serializable {

        public void register(final TraversalStrategy traversalStrategy);

        public void unregister(final Class<? extends TraversalStrategy> optimizerClass);

        public void clear();

        public void apply();

        public boolean complete();
    }

    public interface SideEffects extends Serializable {

        public default boolean exists(final String key) {
            return this.keys().contains(key);
        }

        public <V> void set(final String key, final V value);

        public <V> V get(final String key) throws IllegalArgumentException;

        public void remove(final String key);

        public Set<String> keys();

        public default void setGraph(final Graph graph) {
            this.set(Graph.Key.hide("g"), graph);
        }

        public default Graph getGraph() {
            return this.<Graph>get(Graph.Key.hide("g"));
        }

        public default void removeGraph() {
            this.remove(Graph.Key.hide("g"));
        }

        public default <V> V getOrCreate(final String key, final Supplier<V> orCreate) {
            if (this.keys().contains(key))
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
                return new IllegalArgumentException("The sideEffects do not have a value for provided key: " + key);
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

    public default <E2> Traversal<S, E2> cap(final String variable) {
        return (Traversal) this.addStep(new SideEffectCapStep<>(this, variable));
    }

    public default <E2> Traversal<S, E2> cap() {
        return this.cap(TraversalHelper.getEnd(this).getAs());
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

    public default Traversal<S, E> getTraversal() {
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
