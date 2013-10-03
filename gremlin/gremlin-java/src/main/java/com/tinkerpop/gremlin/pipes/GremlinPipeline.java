package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.pipes.util.Holder;
import com.tinkerpop.gremlin.pipes.util.MapHelper;
import com.tinkerpop.gremlin.pipes.util.Path;
import com.tinkerpop.gremlin.pipes.util.PipelineHelper;
import com.tinkerpop.gremlin.pipes.util.SingleIterator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GremlinPipeline<S, E> extends Pipeline<S, E> {

    default <P extends GremlinPipeline> P inOutBoth(final Direction direction, final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Vertex>(this, v -> v.<Vertex>get().query().direction(direction).labels(labels).vertices().iterator()));
    }

    public default <P extends GremlinPipeline> P out(final String... labels) {
        return this.inOutBoth(Direction.OUT, labels);
    }

    public default <P extends GremlinPipeline> P in(final String... labels) {
        return this.inOutBoth(Direction.IN, labels);
    }

    public default <P extends GremlinPipeline> P both(final String... labels) {
        return this.inOutBoth(Direction.BOTH, labels);
    }

    public default <P extends GremlinPipeline> P property(final String key) {
        return this.addPipe(new MapPipe<Element, Property>(this, e -> e.<Element>get().getProperty(key)));
    }

    public default <P extends GremlinPipeline> P value(final String key) {
        return this.addPipe(new MapPipe<Element, Object>(this, e -> e.<Element>get().getValue(key)));
    }

    public default <P extends GremlinPipeline> P has(final String key) {
        return this.addPipe(new FilterPipe<Element>(this, e -> e.<Element>get().getProperty(key).isPresent()));
    }

    public default <P extends GremlinPipeline> P hasNot(final String key) {
        return this.addPipe(new FilterPipe<Element>(this, e -> !e.<Element>get().getProperty(key).isPresent()));
    }

    public default <P extends GremlinPipeline> P has(final String key, final Object value) {
        return this.addPipe(new FilterPipe<Element>(this, e -> {
            final Property x = e.<Element>get().getProperty(key);
            return x.isPresent() && Compare.EQUAL.test(x.getValue(), value);
        }));
    }

    public default <P extends GremlinPipeline> P path() {
        return this.addPipe(new MapPipe<Object, List>(this, o -> {
            final Path path = new Path(o.getPath());
            path.add(o.get());
            return path;
        }));
    }

    public default <P extends GremlinPipeline> P dedup() {
        final Set<Object> set = new LinkedHashSet<>();
        return this.addPipe(new FilterPipe<E>(this, o -> set.add(o.get())));
    }

    public default <P extends GremlinPipeline> P dedup(final Function<Holder, Object> uniqueFunction) {
        final Set<Object> set = new LinkedHashSet<>();
        return this.addPipe(new FilterPipe<E>(this, o -> set.add(uniqueFunction.apply(o))));
    }

    public default <P extends GremlinPipeline> P filter(final Predicate<Holder<E>> predicate) {
        return this.addPipe(new FilterPipe<>(this, predicate));
    }

    public default <P extends GremlinPipeline, E2> P map(final Function<Holder<E>, E2> function) {
        return this.addPipe(new MapPipe<>(this, function));
    }

    public default <P extends GremlinPipeline, E2> P flatMap(final Function<Holder<E>, Iterator<E2>> function) {
        return this.addPipe(new FlatMapPipe<>(this, function));
    }

    public default <P extends GremlinPipeline> P sideEffect(final Consumer<Holder> consumer) {
        return this.addPipe(new FilterPipe<E>(this, o -> {
            consumer.accept(o);
            return true;
        }));
    }

    public default Map<E, Long> groupCount() {
        final Map<E, Long> map = new HashMap<>();
        try {
            while (true) {
                MapHelper.incr(map, (E) this.next().get(), 1l);
            }
        } catch (final NoSuchElementException e) {
            return map;
        }
    }

    public default void iterate() {
        try {
            while (true) {
                this.next();
            }
        } catch (final NoSuchElementException e) {
        }
    }

    public default <P extends GremlinPipeline> P as(final String name) {
        if (null != PipelineHelper.getAs(name, this))
            throw new IllegalStateException("The named pipe already exists");
        final List<Pipe> pipes = this.getPipes();
        pipes.get(pipes.size() - 1).setName(name);
        return (P) this;

    }

    public default <P extends GremlinPipeline> P back(final String name) {
        final List<Pipe> pipes = this.getPipes();
        for (int i = 0; i < pipes.size(); i++) {
            if (name.equals(pipes.get(i).getName())) {
                final int temp = i;
                return this.addPipe(new MapPipe<E, Object>(this, o -> o.getPath().get(temp)));
            }
        }
        throw new IllegalStateException("The named pipe does not exist");
    }

    public default <P extends GremlinPipeline> P loop(final String name, final Predicate<Holder> whilePredicate, final Predicate<Holder> emitPredicate) {
        final Pipe loopStartPipe = PipelineHelper.getAs(name, getPipeline());
        return this.addPipe(new MapPipe<E, Object>(this, o -> {
            o.incrLoops();
            if (whilePredicate.test(o)) {
                final Holder<Object> holder = o.makeSibling(o.get());
                loopStartPipe.addStarts(new SingleIterator<Holder>(holder));
                if (emitPredicate.test(o))
                    return o.get();
                else
                    return NO_OBJECT;
            } else {
                return o.get();
            }
        }));
    }

    public default <P extends GremlinPipeline> P match(final String inAs, final String outAs, final Pipeline... pipelines) {
        return this.addPipe(new MatchPipe(inAs, outAs, this, pipelines));
    }


    public default <P extends GremlinPipeline> P identity() {
        return this.addPipe(new MapPipe<E, E>(this, o -> o.get()));
    }
}
