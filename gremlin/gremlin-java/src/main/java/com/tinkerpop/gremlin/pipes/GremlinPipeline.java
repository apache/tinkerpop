package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GremlinPipeline<S, E> extends Pipeline<S, E> {

    default GremlinPipeline<S, Vertex> inOutBoth(final Direction direction, final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Vertex>(v -> v.query().direction(direction).labels(labels).vertices().iterator()));
    }

    public default GremlinPipeline<S, Vertex> out(final String... labels) {
        return this.inOutBoth(Direction.OUT, labels);
    }

    public default GremlinPipeline<S, Vertex> in(final String... labels) {
        return this.inOutBoth(Direction.IN, labels);
    }

    public default GremlinPipeline<S, Vertex> both(final String... labels) {
        return this.inOutBoth(Direction.BOTH, labels);
    }

    public default GremlinPipeline<S, Property> property(final String key) {
        return this.addPipe(new MapPipe<Element, Property>(e -> e.getProperty(key)));
    }

    public default <R> GremlinPipeline<S, R> value(final String key) {
        return this.addPipe(new MapPipe<Element, R>(e -> e.getValue(key)));
    }

    public default GremlinPipeline<S, E> has(final String key) {
        return this.addPipe(new FilterPipe<Element>(e -> e.getProperty(key).isPresent()));
    }

    public default GremlinPipeline<S, E> hasNot(final String key) {
        return this.addPipe(new FilterPipe<Element>(e -> !e.getProperty(key).isPresent()));
    }

    public default GremlinPipeline<S, E> has(final String key, final Object value) {
        return this.addPipe(new FilterPipe<Element>(e -> {
            final Property x = e.getProperty(key);
            return x.isPresent() && Compare.EQUAL.test(x.getValue(), value);
        }));
    }

    public default GremlinPipeline<S, E> dedup() {
        final Set<E> set = new HashSet<>();
        return this.addPipe(new FilterPipe<E>(o -> {
            if (set.contains(o)) return false;
            else set.add(o);
            return true;
        }));
    }

    public default GremlinPipeline<S, E> filter(final Predicate<E> predicate) {
        return this.addPipe(new FilterPipe<>(predicate));
    }

    public default GremlinPipeline<S, E> map(final Function<?, E> function) {
        return this.addPipe(new MapPipe<>(function));
    }

    public default GremlinPipeline<S, E> flatMap(final Function<?, Iterator<E>> function) {
        return this.addPipe(new FlatMapPipe<>(function));
    }

    public default GremlinPipeline<S, E> sideEffect(final Consumer<E> consumer) {
        return this.addPipe(new MapPipe<E, E>(o -> {
            consumer.accept(o);
            return o;
        }));
    }

    public default void iterate() {
        try {
            while (true) {
                lastPipe().next();
            }
        } catch (final NoSuchElementException e) {
        }
    }

    public GremlinPipeline<S, E> as(final String key);

    public GremlinPipeline<S, ?> back(final String key);

    public GremlinPipeline<S, ?> loop(final String key, final Predicate<Object> doWhile, final Predicate<Object> emitPredicate);

    public default GremlinPipeline<S, E> identity() {
        return this.addPipe(new MapPipe<E, E>(o -> o));
    }

}
