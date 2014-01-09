package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.pipes.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.pipes.util.Holder;
import com.tinkerpop.gremlin.pipes.util.MapHelper;
import com.tinkerpop.gremlin.pipes.util.Path;
import com.tinkerpop.gremlin.pipes.util.PipelineHelper;
import com.tinkerpop.gremlin.pipes.util.SingleIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GremlinPipeline<S, E> extends Pipeline<S, E> {

    public default GremlinPipeline<S, E> identity() {
        return this.addPipe(new MapPipe<E, E>(this, Holder::get));
    }

    ///////////////////// TRANSFORM STEPS /////////////////////

    public default <E2> GremlinPipeline<S, E2> map(final Function<Holder<E>, E2> function) {
        return this.addPipe(new MapPipe<>(this, function));
    }

    public default <E2> GremlinPipeline<S, E2> flatMap(final Function<Holder<E>, Iterator<E2>> function) {
        return this.addPipe(new FlatMapPipe<>(this, function));
    }

    public GremlinPipeline<Vertex, Vertex> V();

    public GremlinPipeline<Vertex, Vertex> v(final Object... ids);

    public default GremlinPipeline<S, Vertex> out(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Vertex>(this, v -> v.get().query().direction(Direction.OUT).labels(labels).vertices().iterator()));
    }

    public default GremlinPipeline<S, Vertex> in(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Vertex>(this, v -> v.get().query().direction(Direction.IN).labels(labels).vertices().iterator()));
    }

    public default GremlinPipeline<S, Vertex> both(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Vertex>(this, v -> v.get().query().direction(Direction.BOTH).labels(labels).vertices().iterator()));
    }

    public default GremlinPipeline<S, Edge> outE(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Edge>(this, v -> v.get().query().direction(Direction.OUT).labels(labels).edges().iterator())
        /*{
            public String toString() {
                return "FlatMapPipe[out," + Arrays.asList(labels) + "]";
            }
        }*/);
    }

    public default GremlinPipeline<S, Edge> inE(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Edge>(this, v -> v.get().query().direction(Direction.IN).labels(labels).edges().iterator()));
    }

    public default GremlinPipeline<S, Edge> bothE(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Edge>(this, v -> v.get().query().direction(Direction.BOTH).labels(labels).edges().iterator()));
    }

    public default GremlinPipeline<S, Vertex> inV() {
        return this.addPipe(new MapPipe<Edge, Vertex>(this, e -> e.get().getVertex(Direction.IN)));
    }

    public default GremlinPipeline<S, Vertex> outV() {
        return this.addPipe(new MapPipe<Edge, Vertex>(this, e -> e.get().getVertex(Direction.OUT)));
    }

    public default GremlinPipeline<S, Vertex> bothV() {
        return this.addPipe(new FlatMapPipe<Edge, Vertex>(this, e -> Arrays.asList(e.get().getVertex(Direction.OUT), e.get().getVertex(Direction.IN)).iterator()));
    }

    public default GremlinPipeline<S, Property> property(final String key) {
        return this.addPipe(new MapPipe<Element, Property>(this, e -> e.get().getProperty(key)));
    }

    public default <E2> GremlinPipeline<S, E2> value(final String key) {
        return this.addPipe(new MapPipe<Element, Object>(this, e -> e.get().getValue(key)));
    }

    public default <E2> GremlinPipeline<S, E2> value(final String key, final Object defaultValue) {
        return this.addPipe(new MapPipe<Element, Object>(this, e -> e.get().getProperty(key).orElse(defaultValue)));
    }

    public default <E2> GremlinPipeline<S, E2> value(final String key, final Supplier defaultSupplier) {
        return this.addPipe(new MapPipe<Element, Object>(this, e -> e.get().getProperty(key).orElseGet(defaultSupplier)));
    }

    public default GremlinPipeline<S, Path> path() {
        return this.addPipe(new MapPipe<Object, Path>(this, Holder::getPath));
    }

    public default <E2> GremlinPipeline<S, E2> back(final String name) {
        return this.addPipe(new MapPipe<E, Object>(this, o -> o.getPath().get(name)));
    }

    public default <E2> GremlinPipeline<S, E2> match(final String inAs, final String outAs, final Pipeline... pipelines) {
        return this.addPipe(new MatchPipe(inAs, outAs, this, pipelines));
    }

    public default GremlinPipeline<S, List> select(final String... names) {
        return this.addPipe(new MapPipe<Object, List>(this, h -> {
            final Path path = h.getPath();
            return names.length == 0 ?
                    path.getAsSteps().stream().map(s -> path.get(s)).collect(Collectors.toList()) :
                    Stream.of(names).map(s -> path.get(s)).collect(Collectors.toList());
        }));
    }

    ///////////////////// FILTER STEPS /////////////////////

    public default GremlinPipeline<S, E> filter(final Predicate<Holder<E>> predicate) {
        return this.addPipe(new FilterPipe<>(this, predicate));
    }

    public default GremlinPipeline<S, E> simplePath() {
        return this.addPipe(new FilterPipe<Object>(this, o -> o.getPath().isSimple()));
    }

    public default GremlinPipeline<S, E> has(final String key) {
        return this.addPipe(new FilterPipe<Element>(this, e -> e.get().getProperty(key).isPresent()));
    }

    public default GremlinPipeline<S, E> hasNot(final String key) {
        return this.addPipe(new FilterPipe<Element>(this, e -> !e.get().getProperty(key).isPresent()));
    }

    public default GremlinPipeline<S, E> has(final String key, final Object value) {
        return this.addPipe(new FilterPipe<Element>(this, e -> {
            final Property x = e.get().getProperty(key);
            return x.isPresent() && Compare.EQUAL.test(x.getValue(), value);
        }));
    }

    public default GremlinPipeline<S, E> dedup() {
        final Set<Object> set = new LinkedHashSet<>();
        return this.addPipe(new FilterPipe<E>(this, o -> set.add(o.get())));
    }

    public default GremlinPipeline<S, E> dedup(final Function<Holder<E>, Object> uniqueFunction) {
        final Set<Object> set = new LinkedHashSet<>();
        return this.addPipe(new FilterPipe<E>(this, o -> set.add(uniqueFunction.apply(o))));
    }

    public default GremlinPipeline<S, E> range(final int low, final int high) {
        if (low != -1 && high != -1 && low > high) {
            throw new IllegalArgumentException("Not a legal range: [" + low + ", " + high + "]");
        }
        final AtomicInteger counter = new AtomicInteger(-1);
        return this.addPipe(new FilterPipe<E>(this, o -> {
            int newCounter = counter.incrementAndGet();
            if ((low == -1 || newCounter >= low) && (high == -1 || newCounter <= high)) {
                return true;
            }
            if (high != -1 && newCounter > high) {
                throw FastNoSuchElementException.instance();
            }
            return true;
        }));
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public default GremlinPipeline<S, E> sideEffect(final Consumer<Holder<E>> consumer) {
        return this.addPipe(new FilterPipe<E>(this, o -> {
            consumer.accept(o);
            return true;
        }));
    }

    // TODO: What is the state of groupCount/groupBy --- sideEffects/endPoints (the cap() dilema ensues).

    public default Map<Object, Long> groupCount() {
        final Map<Object, Long> map = new HashMap<>();
        try {
            while (true) {
                MapHelper.incr(map, this.next().get(), 1l);
            }
        } catch (final NoSuchElementException e) {
            return map;
        }
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public default GremlinPipeline<S, E> loop(final String as, final Predicate<Holder<E>> whilePredicate, final Predicate<Holder<E>> emitPredicate) {
        final Pipe<?, ?> loopStartPipe = PipelineHelper.getAs(as, getPipeline());
        return this.addPipe(new MapPipe<E, Object>(this, holder -> {
            holder.incrLoops();
            if (whilePredicate.test(holder)) {
                holder.setFuture(as);
                loopStartPipe.addStarts((Iterator) new SingleIterator<>(holder));
                return emitPredicate.test(holder) ? holder.get() : NO_OBJECT;
            } else {
                return holder.get();
            }
        }));
    }

    public default GremlinPipeline<S, E> loop(final String as, final Predicate<Holder<E>> whilePredicate) {
        return this.loop(as, whilePredicate, o -> false);
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public default GremlinPipeline<S, E> as(final String as) {
        if (PipelineHelper.asExists(as, this))
            throw new IllegalStateException("The named pipe already exists");
        final List<Pipe<?, ?>> pipes = this.getPipes();
        pipes.get(pipes.size() - 1).setAs(as);
        return this;

    }

    public default List<Holder> next(final int amount) {
        final List<Holder> result = new ArrayList<>();
        int counter = 0;
        while (counter++ < amount && this.hasNext()) {
            result.add(this.next());
        }
        return result;
    }

    public default List<Holder> toList() {
        return (List<Holder>) this.fill(new ArrayList<>());
    }

    public default Collection<Holder> fill(final Collection<Holder> collection) {
        try {
            while (true) {
                collection.add(this.next());
            }
        } catch (final NoSuchElementException e) {
        }
        return collection;
    }

    public default void iterate() {
        try {
            while (true) {
                this.next();
            }
        } catch (final NoSuchElementException e) {
        }
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
}
