package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.util.HasContainer;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Path;
import com.tinkerpop.gremlin.T;
import com.tinkerpop.gremlin.pipes.named.RangePipe;
import com.tinkerpop.gremlin.pipes.named.VertexVertexPipe;
import com.tinkerpop.gremlin.pipes.util.GremlinHelper;
import com.tinkerpop.gremlin.pipes.util.MapHelper;
import com.tinkerpop.gremlin.pipes.util.SingleIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
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

    public <T> Optional<T> get(final String variable);

    public <T> void put(final String variable, T t);

    public Pipeline<Vertex, Vertex> V();

    public Pipeline<Edge, Edge> E();

    public Pipeline<Vertex, Vertex> v(final Object... ids);

    public Pipeline<Edge, Edge> e(final Object... ids);

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

    /*public default Pipeline<S, Vertex> out(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Vertex>(this, v -> v.get().query().direction(Direction.OUT).labels(labels).vertices().iterator()));
    }*/
    public default Pipeline<S, Vertex> out(final String... labels) {
        return this.addPipe(new VertexVertexPipe(this, Direction.OUT, Integer.MAX_VALUE, labels));
    }

    public default Pipeline<S, Vertex> in(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Vertex>(this, v -> v.get().query().direction(Direction.IN).labels(labels).vertices().iterator()));
    }

    public default Pipeline<S, Vertex> both(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Vertex>(this, v -> v.get().query().direction(Direction.BOTH).labels(labels).vertices().iterator()));
    }

    public default Pipeline<S, Edge> outE(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Edge>(this, v -> v.get().query().direction(Direction.OUT).labels(labels).edges().iterator()));
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

    public default Pipeline<S, Map<String, Object>> values(final String... keys) {
        return this.addPipe(new MapPipe<Element, Map>(this, e -> {
            final Map<String, Object> values = new HashMap<>();
            final Element element = e.get();
            if (null == keys || keys.length == 0) {
                element.getPropertyKeys().forEach(key -> values.put(key, element.getValue(key)));
            } else {
                for (final String key : keys) {
                    if (key.equals(Property.Key.ID))
                        values.put(Property.Key.ID, element.getId());
                    else if (key.equals(Property.Key.LABEL))
                        values.put(Property.Key.LABEL, element.getLabel());
                    else
                        element.getProperty(key).ifPresent(v -> values.put(key, v));
                }
            }
            return values;
        }));
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
                    path.getAsSteps().stream().map(path::get).collect(Collectors.toList()) :
                    Stream.of(names).map(path::get).collect(Collectors.toList());
        }));
    }

    public default <E2> Pipeline<S, E2> unroll() {
        return this.addPipe(new FlatMapPipe<List, Object>(this, l -> l.get().iterator()));
    }

    /*public default <E2> Pipeline<S, E2> rollup() {
        return this.addPipe(new Map<Object, List>(this, o -> o);
    }*/

    ///////////////////// FILTER STEPS /////////////////////

    public default Pipeline<S, E> filter(final Predicate<Holder<E>> predicate) {
        return this.addPipe(new FilterPipe<>(this, predicate));
    }

    public default Pipeline<S, E> dedup() {
        final Set<Object> set = new LinkedHashSet<>();
        return this.addPipe(new FilterPipe<E>(this, o -> set.add(o.get())));
    }

    public default Pipeline<S, E> dedup(final Function<Holder<E>, Object> uniqueFunction) {
        final Set<Object> set = new LinkedHashSet<>();
        return this.addPipe(new FilterPipe<E>(this, o -> set.add(uniqueFunction.apply(o))));
    }

    public default Pipeline<S, E> except(final String variable) {
        final Collection<E> collection = this.<Collection<E>>get(variable).get();
        return this.addPipe(new FilterPipe<E>(this, o -> !collection.contains(o.get())));
    }

    public default Pipeline<S, E> has(final String key) {
        return this.addPipe(new FilterPipe<Element>(this, e -> e.get().getProperty(key).isPresent()));
    }

    public default Pipeline<S, E> has(final String key, final Object value) {
        return has(key, Compare.EQUAL, value);
    }

    public default Pipeline<S, E> has(final String key, final T t, final Object value) {
        return has(key, T.convert(t), value);
    }

    public default Pipeline<S, E> hasNot(final String key) {
        return this.addPipe(new FilterPipe<Element>(this, e -> !e.get().getProperty(key).isPresent()));
    }

    public default Pipeline<S, E> has(final String key, final BiPredicate predicate, final Object value) {
        final HasContainer hasContainer = new HasContainer(key, predicate, value);
        return this.addPipe(new FilterPipe<Element>(this, e -> hasContainer.test(e.get())));
    }

    public default Pipeline<S, E> interval(final String key, final Comparable startValue, final Comparable endValue) {
        final HasContainer start = new HasContainer(key, Compare.GREATER_THAN_EQUAL, startValue);
        final HasContainer end = new HasContainer(key, Compare.LESS_THAN, endValue);
        return this.addPipe(new FilterPipe<Element>(this, e -> start.test(e.get()) && end.test(e.get())));
    }

    /*public default Pipeline<S, E> range(final int low, final int high) {
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
    }*/

    public default Pipeline<S, E> range(final int low, final int high) {
        return this.addPipe(new RangePipe<>(this, low, high));
    }

    public default Pipeline<S, E> simplePath() {
        this.trackPaths(true);
        return this.addPipe(new FilterPipe<Object>(this, o -> o.getPath().isSimple()));
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public default Pipeline<S, E> sideEffect(final Consumer<Holder<E>> consumer) {
        return this.addPipe(new FilterPipe<E>(this, o -> {
            consumer.accept(o);
            return true;
        }));
    }

    public default Pipeline<S, E> aggregate(final String variable) {
        final Set<E> set = this.<Set<E>>get(variable).orElse(new HashSet<>());
        this.put(variable, set);
        final List<E> list = new ArrayList<>();
        final String pipeName = GremlinHelper.getEnd(this).getAs();
        return this.addPipe(new FlatMapPipe<E, E>(this, o -> {
            set.add(o.get());
            list.add(o.get());
            while (GremlinHelper.getAs(pipeName, this).hasNext()) {
                E e = (E) GremlinHelper.getAs(pipeName, this).next().get();
                set.add(e);
                list.add(e);
            }

            return list.iterator();
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

    public default Pipeline<S, E> jump(final String as) {
        return this.jump(as, h -> true, h -> false);
    }

    public default Pipeline<S, E> jump(final String as, final Predicate<Holder<E>> ifPredicate) {
        return this.jump(as, ifPredicate, h -> false);
    }

    public default Pipeline<S, E> jump(final String as, final Predicate<Holder<E>> ifPredicate, final Predicate<Holder<E>> emitPredicate) {
        final Pipe<?, ?> jumpPipe = GremlinHelper.asExists(as, this) ? GremlinHelper.getAs(as, this) : null;
        return this.addPipe(new MapPipe<E, Object>(this, holder -> {
            if (null != jumpPipe)
                holder.incrLoops();
            if (ifPredicate.test(holder)) {
                holder.setFuture(as);
                if (null == jumpPipe)
                    GremlinHelper.getAs(as, getPipeline()).addStarts((Iterator) new SingleIterator<>(holder));
                else
                    jumpPipe.addStarts((Iterator) new SingleIterator<>(holder));
                return emitPredicate.test(holder) ? holder.get() : Pipe.NO_OBJECT;
            } else {
                return holder.get();
            }
        }));
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

    public default void forEach(final Consumer<E> consumer) {
        try {
            while (true) {
                consumer.accept(this.next());
            }
        } catch (final NoSuchElementException e) {

        }
    }
}
