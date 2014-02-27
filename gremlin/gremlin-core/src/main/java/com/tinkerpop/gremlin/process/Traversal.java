package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.process.graph.map.IdentityStep;
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

    public Optimizers optimizers();

    public void addStarts(final Iterator<Holder<S>> starts);

    public <S, E, T extends Traversal<S, E>> T addStep(final Step<?, E> step);

    public List<Step> getSteps();

    public Iterator<E> submit(final TraversalEngine engine);

    public static Traversal of() {
        Traversal traversal = new DefaultTraversal<>();
        traversal.addStep(new IdentityStep(traversal));
        return traversal;
    }

    public interface Memory {

        public static class Variable {

            private static final String HIDDEN_PREFIX = "%&%";

            public static String hidden(final String key) {
                return HIDDEN_PREFIX.concat(key);
            }
        }

        public <T> void set(final String variable, final T value);

        public <T> T get(final String variable);

        public Set<String> getVariables();

        public default <T> T getOrCreate(final String variable, final Supplier<T> orCreate) {
            if (this.getVariables().contains(variable))
                return this.get(variable);
            else {
                T t = orCreate.get();
                this.set(variable, t);
                return t;
            }
        }
    }

    /////////

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
            while (true) {
                collection.add(this.next());
            }
        } catch (final NoSuchElementException e) {
        }
        return collection;
    }

    public default Traversal iterate() {
        try {
            while (true) {
                this.next();
            }
        } catch (final NoSuchElementException e) {
        }
        return this;
    }

    public default long count() {
        long counter = 0;
        try {
            while (true) {
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
            while (true) {
                consumer.accept(this.next());
            }
        } catch (final NoSuchElementException e) {

        }
    }

}
