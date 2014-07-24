package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.SideEffects;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.filter.PathIdentityStep;
import com.tinkerpop.gremlin.process.graph.step.map.MapStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.util.DefaultTraversal;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.structure.Graph;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
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

    public Memory memory();

    public TraversalStrategies strategies();

    public void addStarts(final Iterator<Traverser<S>> starts);

    public <S, E, T extends Traversal<S, E>> T addStep(final Step<?, E> step);

    public List<Step> getSteps();

    public default Traversal<S, E> submit(final GraphComputer computer) {
        try {
            final Pair<Graph, SideEffects> result = computer.program(TraversalVertexProgram.create().traversal(() -> this).getConfiguration()).submit().get();
            final Traversal traversal = new DefaultTraversal<>();
            traversal.addStarts(new SingleIterator(result.getValue1()));
            return traversal;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public interface Memory extends Serializable {

        public <T> void set(final String key, final T value);

        public <T> T get(final String key);

        public <T> T remove(final String key);

        public Set<String> keys();

        public default <T> T getOrCreate(final String key, final Supplier<T> orCreate) {
            if (this.keys().contains(key))
                return this.get(key);
            else {
                T t = orCreate.get();
                this.set(key, t);
                return t;
            }
        }

        public static class Exceptions {

            public static IllegalArgumentException variableKeyCanNotBeEmpty() {
                return new IllegalArgumentException("Memory variable key can not be the empty string");
            }

            public static IllegalArgumentException variableKeyCanNotBeNull() {
                return new IllegalArgumentException("Memory variable key can not be null");
            }

            public static IllegalArgumentException variableValueCanNotBeNull() {
                return new IllegalArgumentException("Memory variable value can not be null");
            }

            public static IllegalArgumentException variableValueDoesNotExist(final String variable) {
                return new IllegalArgumentException("The memory does not have a value for provided variable: " + variable);
            }

            public static UnsupportedOperationException dataTypeOfVariableValueNotSupported(final Object val) {
                return new UnsupportedOperationException(String.format("Memory variable value [%s] is of type %s is not supported", val, val.getClass()));
            }
        }
    }

    /////////

    public default <E2> Traversal<S, E2> memory(final String key) {
        final MapStep<S, E2> mapStep = new MapStep<>(this);
        mapStep.setFunction(t -> this.memory().get(key));
        this.addStep(mapStep);
        return (Traversal) this;
    }

    public default Traversal<S, E> trackPaths() {
        return (Traversal) this.addStep(new PathIdentityStep<>(this));
    }

    public default <E2> Traversal<S, E2> cap(final String variable) {
        return (Traversal) this.addStep(new SideEffectCapStep<>(this, variable));
    }

    public default <E2> Traversal<S, E2> cap() {
        return this.cap(SideEffectCapable.CAP_KEY);
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
            while (this.hasNext()) {
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
