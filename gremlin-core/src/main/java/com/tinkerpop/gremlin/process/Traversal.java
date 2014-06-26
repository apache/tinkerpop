package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.filter.PathIdentityStep;
import com.tinkerpop.gremlin.process.graph.step.map.StartStep;
import com.tinkerpop.gremlin.process.util.DefaultTraversal;

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

    public default Traversal<S, E> submit(final TraversalEngine engine) {
        final Traversal<S, E> traversal = new DefaultTraversal<>();
        traversal.addStep(new StartStep<>(traversal, engine.execute(this)));
        return traversal;
    }

    /*public static Traversal of() {
        Traversal traversal = new DefaultTraversal<>();
        traversal.addStep(new IdentityStep(traversal));
        return traversal;
    }*/

    public interface Memory extends Serializable {

        public <T> void set(final String key, final T value);

        public <T> T get(final String key);

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
    }

    /////////

    public default Traversal<S, E> trackPaths() {
        return (Traversal) this.addStep(new PathIdentityStep<>(this));
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
        } catch (final NoSuchElementException e) {
        }
        return collection;
    }

    public default Traversal iterate() {
        try {
            while (this.hasNext()) {
                this.next();
            }
        } catch (final NoSuchElementException e) {
        }
        return this;
    }

    public default long count() {
        long counter = 0;
        try {
            while (this.hasNext()) {
                this.next();
                counter++;
            }
        } catch (final NoSuchElementException e) {
        }
        return counter;
    }

    public default Traversal<S, E> getTraversal() {
        return this;
    }

    public default void forEach(final Consumer<E> consumer) {
        try {
            while (this.hasNext()) {
                consumer.accept(this.next());
            }
        } catch (final NoSuchElementException e) {

        }
    }
}
