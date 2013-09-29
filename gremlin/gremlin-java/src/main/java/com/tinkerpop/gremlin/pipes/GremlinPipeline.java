package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GremlinPipeline<S, E> extends Pipeline<S, E> {

    default <R extends GremlinPipeline> R inOutBoth(final Direction direction, final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Vertex>(v -> v.<Vertex>get().query().direction(direction).labels(labels).vertices().iterator()));
    }

    public default <R extends GremlinPipeline> R out(final String... labels) {
        return this.inOutBoth(Direction.OUT, labels);
    }

    public default <R extends GremlinPipeline> R in(final String... labels) {
        return this.inOutBoth(Direction.IN, labels);
    }

    public default <R extends GremlinPipeline> R both(final String... labels) {
        return this.inOutBoth(Direction.BOTH, labels);
    }

    public default <R extends GremlinPipeline> R property(final String key) {
        return this.addPipe(new MapPipe<Element, Property>(e -> e.<Element>get().getProperty(key)));
    }

    public default <R extends GremlinPipeline> R value(final String key) {
        return this.addPipe(new MapPipe<Element, Object>(e -> e.<Element>get().getValue(key)));
    }

    public default <R extends GremlinPipeline> R has(final String key) {
        return this.addPipe(new FilterPipe<Element>(e -> e.<Element>get().getProperty(key).isPresent()));
    }

    public default <R extends GremlinPipeline> R hasNot(final String key) {
        return this.addPipe(new FilterPipe<Element>(e -> !e.<Element>get().getProperty(key).isPresent()));
    }

    public default <R extends GremlinPipeline> R has(final String key, final Object value) {
        return this.addPipe(new FilterPipe<Element>(e -> {
            final Property x = e.<Element>get().getProperty(key);
            return x.isPresent() && Compare.EQUAL.test(x.getValue(), value);
        }));
    }

    public default <R extends GremlinPipeline> R path() {
        return this.addPipe(new MapPipe<Object, List>(o -> {
            final List path = o.getHistory();
            path.add(o.get());
            return path;
        }));
    }

    /*public default GremlinPipeline dedup() {
        final Set set = new LinkedHashSet<>();
        return this.addPipe(new FilterPipe<Object>(set::add));
    }

    public default GremlinPipeline filter(final Predicate predicate) {
        return this.addPipe(new FilterPipe(predicate));
    }

    public default GremlinPipeline map(final Function function) {
        return this.addPipe(new MapPipe(function));
    }

    public default GremlinPipeline flatMap(final Function<Object, Iterator> function) {
        return this.addPipe(new FlatMapPipe<>(function));
    }*/

    public default <R extends GremlinPipeline> R sideEffect(final Consumer consumer) {
        return this.addPipe(new MapPipe<E, E>(o -> {
            consumer.accept(o);
            return o.get();
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

    /*public GremlinPipeline as(final String key);

    public GremlinPipeline back(final String key);

    public GremlinPipeline loop(final String key, final Predicate<Object> doWhile, final Predicate<Object> emitPredicate);*/

    public default <R extends GremlinPipeline> R identity() {
        return this.addPipe(new MapPipe<>(Function.identity()));
    }

}
