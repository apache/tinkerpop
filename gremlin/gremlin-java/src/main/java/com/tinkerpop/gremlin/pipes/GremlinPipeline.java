package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.pipes.util.Holder;
import com.tinkerpop.gremlin.pipes.util.MapHelper;
import com.tinkerpop.gremlin.pipes.util.Path;
import com.tinkerpop.gremlin.pipes.util.PipelineHelper;
import com.tinkerpop.gremlin.pipes.util.SingleIterator;

import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GremlinPipeline<S, E> extends Pipeline<S, E> {

    public default <P extends GremlinPipeline> P identity() {
        return this.addPipe(new MapPipe<E, E>(this, o -> o.get()));
    }

    ///////////////////// TRANSFORM STEPS /////////////////////

    public default <P extends GremlinPipeline, E2> P transform(final Function<Holder<E>, E2> function) {
        return this.addPipe(new MapPipe<>(this, function));
    }

    public default <P extends GremlinPipeline, E2> P flatTransform(final Function<Holder<E>, Iterator<E2>> function) {
        return this.addPipe(new FlatMapPipe<>(this, function));
    }

    public <P extends GremlinPipeline> P V();

    public default <P extends GremlinPipeline> P out(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Vertex>(this, v -> v.<Vertex>get().query().direction(Direction.OUT).labels(labels).vertices().iterator()));
    }

    public default <P extends GremlinPipeline> P in(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Vertex>(this, v -> v.<Vertex>get().query().direction(Direction.IN).labels(labels).vertices().iterator()));
    }

    public default <P extends GremlinPipeline> P both(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Vertex>(this, v -> v.<Vertex>get().query().direction(Direction.BOTH).labels(labels).vertices().iterator()));
    }

    public default <P extends GremlinPipeline> P outE(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Edge>(this, v -> v.<Vertex>get().query().direction(Direction.OUT).labels(labels).edges().iterator()));
    }

    public default <P extends GremlinPipeline> P inE(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Edge>(this, v -> v.<Vertex>get().query().direction(Direction.IN).labels(labels).edges().iterator()));
    }

    public default <P extends GremlinPipeline> P bothE(final String... labels) {
        return this.addPipe(new FlatMapPipe<Vertex, Edge>(this, v -> v.<Vertex>get().query().direction(Direction.BOTH).labels(labels).edges().iterator()));
    }

    public default <P extends GremlinPipeline> P inV() {
        return this.addPipe(new MapPipe<Edge, Vertex>(this, e -> e.<Edge>get().getVertex(Direction.IN)));
    }

    public default <P extends GremlinPipeline> P outV() {
        return this.addPipe(new MapPipe<Edge, Vertex>(this, e -> e.<Edge>get().getVertex(Direction.OUT)));
    }

    public default <P extends GremlinPipeline> P bothV() {
        return this.addPipe(new FlatMapPipe<Edge, Vertex>(this, e -> Arrays.asList(e.<Edge>get().getVertex(Direction.OUT), e.<Edge>get().getVertex(Direction.IN)).iterator()));
    }

    public default <P extends GremlinPipeline> P property(final String key) {
        return this.addPipe(new MapPipe<Element, Property>(this, e -> e.<Element>get().getProperty(key)));
    }

    public default <P extends GremlinPipeline> P value(final String key) {
        return this.addPipe(new MapPipe<Element, Object>(this, e -> e.<Element>get().getValue(key)));
    }

    public default <P extends GremlinPipeline> P path() {
        return this.addPipe(new MapPipe<Object, Path>(this, o -> o.getPath()));
    }

    public default <P extends GremlinPipeline> P back(final String name) {
        return this.addPipe(new MapPipe<E, Object>(this, o -> o.getPath().get(name)));
    }

    public default <P extends GremlinPipeline> P match(final String inAs, final String outAs, final Pipeline... pipelines) {
        return this.addPipe(new MatchPipe(inAs, outAs, this, pipelines));
    }

    ///////////////////// FILTER STEPS /////////////////////

    public default <P extends GremlinPipeline> P filter(final Predicate<Holder<E>> predicate) {
        return this.addPipe(new FilterPipe<>(this, predicate));
    }

    public default <P extends GremlinPipeline> P simplePath() {
        return this.addPipe(new FilterPipe<Element>(this, o -> o.getPath().isSimple()));
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

    public default <P extends GremlinPipeline> P dedup() {
        final Set<Object> set = new LinkedHashSet<>();
        return this.addPipe(new FilterPipe<E>(this, o -> set.add(o.get())));
    }

    public default <P extends GremlinPipeline> P dedup(final Function<Holder, Object> uniqueFunction) {
        final Set<Object> set = new LinkedHashSet<>();
        return this.addPipe(new FilterPipe<E>(this, o -> set.add(uniqueFunction.apply(o))));
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public default <P extends GremlinPipeline> P sideEffect(final Consumer<Holder> consumer) {
        return this.addPipe(new FilterPipe<E>(this, o -> {
            consumer.accept(o);
            return true;
        }));
    }

    public default <P extends GremlinPipeline> P select(final String... names) {
        return this.addPipe(new MapPipe<Object, List>(this, h -> {
            final Path path = h.getPath();
            return names.length == 0 ?
                    path.getNamedSteps().stream().map(s -> path.get(s)).collect(Collectors.toList()) :
                    Stream.of(names).map(s -> path.get(s)).collect(Collectors.toList());
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

    ///////////////////// BRANCH STEPS /////////////////////

    public default <P extends GremlinPipeline> P loop(final String name, final Predicate<Holder> whilePredicate, final Predicate<Holder> emitPredicate) {
        final Pipe loopStartPipe = PipelineHelper.getAs(name, getPipeline());
        return this.addPipe(new MapPipe<E, Object>(this, o -> {
            o.incrLoops();
            if (whilePredicate.test(o)) {
                final Holder holder = o.makeSibling();
                loopStartPipe.addStarts(new SingleIterator<>(holder));
                if (emitPredicate.test(o))
                    return o.get();
                else
                    return NO_OBJECT;
            } else {
                return o.get();
            }
        }));
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public default <P extends GremlinPipeline> P as(final String name) {
        if (null != PipelineHelper.getAs(name, this))
            throw new IllegalStateException("The named pipe already exists");
        final List<Pipe> pipes = this.getPipes();
        pipes.get(pipes.size() - 1).setName(name);
        return (P) this;

    }

    public default List<Holder> next(final int amount) {
        final List<Holder> result = new ArrayList<>();
        int counter = 0;
        while (counter++ < amount && this.hasNext()) {
            result.add(this.next());
        }
        return result;
    }

    public default void iterate() {
        try {
            while (true) {
                this.next();
            }
        } catch (final NoSuchElementException e) {
        }
    }
}
