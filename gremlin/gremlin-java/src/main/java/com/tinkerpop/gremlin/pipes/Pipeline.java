package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Path;
import com.tinkerpop.gremlin.pipes.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.pipes.util.GremlinHelper;
import com.tinkerpop.gremlin.pipes.util.MapHelper;
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
public interface Pipeline<S, E> extends Iterator<E> {

    public Pipeline<Vertex, Vertex> V();

    public Pipeline<Vertex, Vertex> v(final Object... ids);

    public Pipeline trackPaths(final boolean trackPaths);

    public boolean getTrackPaths();

    public void addStarts(final Iterator<Holder<S>> starts);


    ///////////////////// TRANSFORM STEPS /////////////////////

    public default <E2> Pipeline<S, E2> map(final Function<Holder<E>, E2> function) {
        return this.addPipe(new MapPipe<>(this, function));
    }

    public default <E2> Pipeline<S, E2> flatMap(final Function<Holder<E>, Iterator<E2>> function) {
        return this.addPipe(new FlatMapPipe<>(this, function));
    }

    public default Pipeline<S, E> identity() {
        return this.addPipe(new MapPipe<E, E>(this, Holder::get));
    }

    public default Pipeline<S, Vertex> out(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Vertex>(this, v -> v.get().query().direction(Direction.OUT).labels(labels).vertices().iterator()));
    }

    public default Pipeline<S, Vertex> in(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Vertex>(this, v -> v.get().query().direction(Direction.IN).labels(labels).vertices().iterator()));
    }

    public default Pipeline<S, Vertex> both(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Vertex>(this, v -> v.get().query().direction(Direction.BOTH).labels(labels).vertices().iterator()));
    }

    public default Pipeline<S, Edge> outE(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Edge>(this, v -> v.get().query().direction(Direction.OUT).labels(labels).edges().iterator())
        /*{
            public String toString() {
                return "FlatMapPipe[out," + Arrays.asList(labels) + "]";
            }
        }*/);
    }

    public default Pipeline<S, Edge> inE(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Edge>(this, v -> v.get().query().direction(Direction.IN).labels(labels).edges().iterator()));
    }

    public default Pipeline<S, Edge> bothE(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Edge>(this, v -> v.get().query().direction(Direction.BOTH).labels(labels).edges().iterator()));
    }

    public default Pipeline<S, Vertex> inV() {
        return this.addPipe(new MapPipe<Edge, Vertex>(this, e -> e.get().getVertex(Direction.IN)));
    }

    public default Pipeline<S, Vertex> outV() {
        return this.addPipe(new MapPipe<Edge, Vertex>(this, e -> e.get().getVertex(Direction.OUT)));
    }

    public default Pipeline<S, Vertex> bothV() {
        return this.addPipe(new FlatMapPipe<Edge, Vertex>(this, e -> Arrays.asList(e.get().getVertex(Direction.OUT), e.get().getVertex(Direction.IN)).iterator()));
    }

    public default <E2> Pipeline<S, Property<E2>> property(final String key) {
        return this.addPipe(new MapPipe<Element, Property>(this, e -> e.get().<E2>getProperty(key)));
    }

    public default <E2> Pipeline<S, E2> value() {
        return this.addPipe(new MapPipe<Property, E2>(this, p -> (E2) (p.get()).get()));
    }

    public default <E2> Pipeline<S, E2> value(final String key) {
        return this.addPipe(new MapPipe<Element, E2>(this, e -> e.get().<E2>getValue(key)));
    }

    public default <E2> Pipeline<S, E2> value(final String key, final E2 defaultValue) {
        return this.addPipe(new MapPipe<Element, E2>(this, e -> e.get().<E2>getProperty(key).orElse(defaultValue)));
    }

    public default <E2> Pipeline<S, E2> value(final String key, final Supplier<E2> defaultSupplier) {
        return this.addPipe(new MapPipe<Element, Object>(this, e -> e.get().<E2>getProperty(key).orElseGet(defaultSupplier)));
    }

    public default Pipeline<S, Path> path(final Function... pathFunctions) {
        this.trackPaths(true);
        if (null == pathFunctions || pathFunctions.length == 0)
            return this.addPipe(new MapPipe<S, Path>(this, Holder::getPath));
        else {
            final AtomicInteger nextFunction = new AtomicInteger(0);
            return this.addPipe(new MapPipe<S, Path>(this, o -> {
                final Path path = new Path();
                o.getPath().forEach((a, b) -> {
                    path.add(a, pathFunctions[nextFunction.get()].apply(b));
                    nextFunction.set((nextFunction.get() + 1) % pathFunctions.length);
                });
                return path;
            }));
        }
    }

    public default <E2> Pipeline<S, E2> back(final String name) {
        this.trackPaths(true);
        return this.addPipe(new MapPipe<E, Object>(this, o -> o.getPath().get(name)));
    }

    public default <E2> Pipeline<S, E2> match(final String inAs, final String outAs, final Pipeline... pipelines) {
        this.trackPaths(true);
        return this.addPipe(new MatchPipe(inAs, outAs, this, pipelines));
    }

    public default Pipeline<S, List> select(final String... names) {
        this.trackPaths(true);
        return this.addPipe(new MapPipe<Object, List>(this, h -> {
            final Path path = h.getPath();
            return names.length == 0 ?
                    path.getAsSteps().stream().map(s -> path.get(s)).collect(Collectors.toList()) :
                    Stream.of(names).map(s -> path.get(s)).collect(Collectors.toList());
        }));
    }

    ///////////////////// FILTER STEPS /////////////////////

    public default Pipeline<S, E> filter(final Predicate<Holder<E>> predicate) {
        return this.addPipe(new FilterPipe<>(this, predicate));
    }

    public default Pipeline<S, E> simplePath() {
        this.trackPaths(true);
        return this.addPipe(new FilterPipe<Object>(this, o -> o.getPath().isSimple()));
    }

    public default Pipeline<S, E> has(final String key) {
        return this.addPipe(new FilterPipe<Element>(this, e -> e.get().getProperty(key).isPresent()));
    }

    public default Pipeline<S, E> hasNot(final String key) {
        return this.addPipe(new FilterPipe<Element>(this, e -> !e.get().getProperty(key).isPresent()));
    }

    public default Pipeline<S, E> has(final String key, final Object value) {
        return this.addPipe(new FilterPipe<Element>(this, e -> {
            final Property x = e.get().getProperty(key);
            return x.isPresent() && Compare.EQUAL.test(x.get(), value);
        }));
    }

    public default Pipeline<S, E> dedup() {
        final Set<Object> set = new LinkedHashSet<>();
        return this.addPipe(new FilterPipe<E>(this, o -> set.add(o.get())));
    }

    public default Pipeline<S, E> dedup(final Function<Holder<E>, Object> uniqueFunction) {
        final Set<Object> set = new LinkedHashSet<>();
        return this.addPipe(new FilterPipe<E>(this, o -> set.add(uniqueFunction.apply(o))));
    }

    public default Pipeline<S, E> range(final int low, final int high) {
        if (low != -1 && high != -1 && low > high) {
            throw new IllegalArgumentException("Not a legal range: [" + low + ", " + high + "]");
        }
        final AtomicInteger counter = new AtomicInteger(-1);
        return this.addPipe(new FilterPipe<E>(this, o -> {
            counter.incrementAndGet();
            if ((low == -1 || counter.get() >= low) && (high == -1 || counter.get() <= high))
                return true;
            else if (high != -1 && counter.get() > high)
                throw FastNoSuchElementException.instance();
            else
                return false;
        }));
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public default Pipeline<S, E> sideEffect(final Consumer<Holder<E>> consumer) {
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
                MapHelper.incr(map, this.next(), 1l);
            }
        } catch (final NoSuchElementException e) {
            return map;
        }
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public default Pipeline<S, E> loop(final String as, final Predicate<Holder<E>> whilePredicate, final Predicate<Holder<E>> emitPredicate) {
        this.trackPaths(true);
        final Pipe<?, ?> loopStartPipe = GremlinHelper.getAs(as, getPipeline());
        return this.addPipe(new MapPipe<E, Object>(this, holder -> {
            holder.incrLoops();
            if (whilePredicate.test(holder)) {
                holder.setFuture(as);
                loopStartPipe.addStarts((Iterator) new SingleIterator<>(holder));
                return emitPredicate.test(holder) ? holder.get() : Pipe.NO_OBJECT;
            } else {
                return holder.get();
            }
        }));
    }

    public default Pipeline<S, E> loop(final String as, final Predicate<Holder<E>> whilePredicate) {
        return this.loop(as, whilePredicate, o -> false);
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public default Pipeline<S, E> as(final String as) {
        if (GremlinHelper.asExists(as, this))
            throw new IllegalStateException("The named pipe already exists");
        final List<Pipe<?, ?>> pipes = this.getPipes();
        pipes.get(pipes.size() - 1).setAs(as);
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

    public default List<Holder> toList() {
        return (List<Holder>) this.fill(new ArrayList<>());
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

    public <P extends Pipeline> P addPipe(final Pipe pipe);

    public List<Pipe<?, ?>> getPipes();

    public default <P extends Pipeline<?, ?>> P getPipeline() {
        return (P) this;
    }

    public default void forEach(final Consumer<Pipe<?, ?>> consumer) {
        for (int i = 0; i < this.getPipes().size(); i++) {
            consumer.accept(this.getPipes().get(i));
        }
    }
}
