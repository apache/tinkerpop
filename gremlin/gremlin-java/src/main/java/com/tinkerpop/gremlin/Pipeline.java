package com.tinkerpop.gremlin;

import com.tinkerpop.blueprints.AnnotatedValue;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Contains;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.util.AnnotatedListQueryBuilder;
import com.tinkerpop.blueprints.query.util.HasContainer;
import com.tinkerpop.blueprints.query.util.VertexQueryBuilder;
import com.tinkerpop.gremlin.oltp.filter.DedupPipe;
import com.tinkerpop.gremlin.oltp.filter.ExceptPipe;
import com.tinkerpop.gremlin.oltp.filter.FilterPipe;
import com.tinkerpop.gremlin.oltp.filter.HasPipe;
import com.tinkerpop.gremlin.oltp.filter.IntervalPipe;
import com.tinkerpop.gremlin.oltp.filter.RangePipe;
import com.tinkerpop.gremlin.oltp.filter.RetainPipe;
import com.tinkerpop.gremlin.oltp.filter.SimplePathPipe;
import com.tinkerpop.gremlin.oltp.map.AnnotatedListQueryPipe;
import com.tinkerpop.gremlin.oltp.map.AnnotationPipe;
import com.tinkerpop.gremlin.oltp.map.AnnotationsPipe;
import com.tinkerpop.gremlin.oltp.map.BackPipe;
import com.tinkerpop.gremlin.oltp.map.EdgeVertexPipe;
import com.tinkerpop.gremlin.oltp.map.ElementPropertyValuePipe;
import com.tinkerpop.gremlin.oltp.map.FlatMapPipe;
import com.tinkerpop.gremlin.oltp.map.IdentityPipe;
import com.tinkerpop.gremlin.oltp.map.JumpPipe;
import com.tinkerpop.gremlin.oltp.map.MapPipe;
import com.tinkerpop.gremlin.oltp.map.MatchPipe;
import com.tinkerpop.gremlin.oltp.map.OrderPipe;
import com.tinkerpop.gremlin.oltp.map.PathPipe;
import com.tinkerpop.gremlin.oltp.map.PropertyPipe;
import com.tinkerpop.gremlin.oltp.map.PropertyValuesPipe;
import com.tinkerpop.gremlin.oltp.map.SelectPipe;
import com.tinkerpop.gremlin.oltp.map.ShufflePipe;
import com.tinkerpop.gremlin.oltp.map.UnionPipe;
import com.tinkerpop.gremlin.oltp.map.ValuePipe;
import com.tinkerpop.gremlin.oltp.map.VertexQueryPipe;
import com.tinkerpop.gremlin.oltp.sideeffect.AggregatePipe;
import com.tinkerpop.gremlin.oltp.sideeffect.GroupByPipe;
import com.tinkerpop.gremlin.oltp.sideeffect.GroupCountPipe;
import com.tinkerpop.gremlin.oltp.sideeffect.LinkPipe;
import com.tinkerpop.gremlin.oltp.sideeffect.SideEffectPipe;
import com.tinkerpop.gremlin.util.FunctionRing;
import com.tinkerpop.gremlin.util.GremlinHelper;
import com.tinkerpop.gremlin.util.MapHelper;
import com.tinkerpop.gremlin.util.optimizers.HolderOptimizer;
import com.tinkerpop.gremlin.util.structures.Tree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Pipeline<S, E> extends Iterator<E> {

    public Memory memory();

    public Optimizers optimizers();

    public Pipeline<Vertex, Vertex> V();

    public Pipeline<Edge, Edge> E();

    public Pipeline<Vertex, Vertex> v(final Object... ids);

    public Pipeline<Edge, Edge> e(final Object... ids);

    public void addStarts(final Iterator<Holder<S>> starts);

    public <S, E> Pipeline<S, E> addPipe(final Pipe<?, E> pipe);

    public List<Pipe> getPipes();

    ///////////////////// TRANSFORM STEPS /////////////////////

    public default <E2> Pipeline<S, E2> map(final Function<Holder<E>, E2> function) {
        return this.addPipe(new MapPipe<>(this, function));
    }

    public default <E2> Pipeline<S, E2> flatMap(final Function<Holder<E>, Iterator<E2>> function) {
        return this.addPipe(new FlatMapPipe<>(this, function));
    }

    public default Pipeline<S, E> identity() {
        return this.addPipe(new IdentityPipe(this));
    }

    public default <E2> Pipeline<S, E2> annotation(final String annotationKey) {
        return this.addPipe(new AnnotationPipe<>(this, annotationKey));
    }

    public default Pipeline<S, Map<String, Object>> annotations(final String... annotationKeys) {
        return this.addPipe(new AnnotationsPipe(this, annotationKeys));
    }

    public default Pipeline<S, Vertex> out(final int branchFactor, final String... labels) {
        return this.addPipe(new VertexQueryPipe<>(this, new VertexQueryBuilder().direction(Direction.OUT).limit(branchFactor).labels(labels), Vertex.class));
    }

    public default Pipeline<S, Vertex> out(final String... labels) {
        return this.out(Integer.MAX_VALUE, labels);
    }

    public default Pipeline<S, Vertex> in(final int branchFactor, final String... labels) {
        return this.addPipe(new VertexQueryPipe<>(this, new VertexQueryBuilder().direction(Direction.IN).limit(branchFactor).labels(labels), Vertex.class));
    }

    public default Pipeline<S, Vertex> in(final String... labels) {
        return this.in(Integer.MAX_VALUE, labels);
    }

    public default Pipeline<S, Vertex> both(final int branchFactor, final String... labels) {
        return this.addPipe(new VertexQueryPipe<>(this, new VertexQueryBuilder().direction(Direction.BOTH).limit(branchFactor).labels(labels), Vertex.class));
    }

    public default Pipeline<S, Vertex> both(final String... labels) {
        return this.both(Integer.MAX_VALUE, labels);
    }

    public default Pipeline<S, Edge> outE(final int branchFactor, final String... labels) {
        return this.addPipe(new VertexQueryPipe<>(this, new VertexQueryBuilder().direction(Direction.OUT).limit(branchFactor).labels(labels), Edge.class));
    }

    public default Pipeline<S, Edge> outE(final String... labels) {
        return this.outE(Integer.MAX_VALUE, labels);
    }

    public default Pipeline<S, Edge> inE(final int branchFactor, final String... labels) {
        return this.addPipe(new VertexQueryPipe<>(this, new VertexQueryBuilder().direction(Direction.IN).limit(branchFactor).labels(labels), Edge.class));
    }

    public default Pipeline<S, Edge> inE(final String... labels) {
        return this.inE(Integer.MAX_VALUE, labels);
    }

    public default Pipeline<S, Edge> bothE(final int branchFactor, final String... labels) {
        return this.addPipe(new VertexQueryPipe<>(this, new VertexQueryBuilder().direction(Direction.BOTH).limit(branchFactor).labels(labels), Edge.class));
    }

    public default Pipeline<S, Edge> bothE(final String... labels) {
        return this.bothE(Integer.MAX_VALUE, labels);
    }

    public default Pipeline<S, Vertex> inV() {
        return this.addPipe(new EdgeVertexPipe(this, Direction.IN));
    }

    public default Pipeline<S, Vertex> outV() {
        return this.addPipe(new EdgeVertexPipe(this, Direction.OUT));
    }

    public default Pipeline<S, Vertex> bothV() {
        return this.addPipe(new FlatMapPipe<Edge, Vertex>(this, edge -> Arrays.asList(edge.get().getVertex(Direction.OUT), edge.get().getVertex(Direction.IN)).iterator()));
    }

    public default Pipeline<S, E> order() {
        return this.addPipe(new OrderPipe<E>(this, (a, b) -> ((Comparable) a.get()).compareTo(b.get())));
    }

    public default Pipeline<S, E> order(final Comparator<Holder<E>> comparator) {
        return this.addPipe(new OrderPipe<>(this, comparator));
    }

    public default <E2> Pipeline<S, Property<E2>> property(final String propertyKey) {
        return this.addPipe(new PropertyPipe<>(this, propertyKey));
    }

    public default Pipeline<S, E> shuffle() {
        return this.addPipe(new ShufflePipe<>(this));
    }

    public default <E2> Pipeline<S, E2> value() {
        return this.addPipe(new ValuePipe<>(this));
    }

    public default <E2> Pipeline<S, E2> value(final String propertyKey) {
        return this.addPipe(new ElementPropertyValuePipe<>(this, propertyKey));
    }

    public default <E2> Pipeline<S, E2> value(final String propertyKey, final E2 defaultValue) {
        return this.addPipe(new ElementPropertyValuePipe<>(this, propertyKey, defaultValue));
    }

    public default <E2> Pipeline<S, E2> value(final String propertyKey, final Supplier<E2> defaultSupplier) {
        return this.addPipe(new ElementPropertyValuePipe<>(this, propertyKey, defaultSupplier));
    }

    public default <E2> Pipeline<S, AnnotatedValue<E2>> annotatedValues(final String propertyKey) {
        return this.addPipe(new AnnotatedListQueryPipe<>(this, propertyKey, new AnnotatedListQueryBuilder()));
    }

    public default Pipeline<S, Map<String, Object>> values(final String... propertyKeys) {
        return this.addPipe(new PropertyValuesPipe(this, propertyKeys));
    }

    public default Pipeline<S, Path> path(final Function... pathFunctions) {
        return this.addPipe(new PathPipe(this, pathFunctions));
    }

    public default <E2> Pipeline<S, E2> back(final String as) {
        return this.addPipe(new BackPipe(this, as));
    }

    public default <E2> Pipeline<S, E2> match(final String inAs, final String outAs, final Pipeline... pipelines) {
        return this.addPipe(new MatchPipe(this, inAs, outAs, pipelines));
    }

    public default Pipeline<S, Path> select(final List<String> asLabels, Function... stepFunctions) {
        return this.addPipe(new SelectPipe(this, asLabels, stepFunctions));
    }

    public default Pipeline<S, Path> select(final Function... stepFunctions) {
        return this.addPipe(new SelectPipe(this, Arrays.asList(), stepFunctions));
    }

    public default <E2> Pipeline<S, E2> union(final Pipeline<?, E2>... pipelines) {
        return this.addPipe(new UnionPipe(this, pipelines));
    }

    /*public default <E2> Pipeline<S, E2> unroll() {
        return this.addPipe(new FlatMapPipe<List, Object>(this, l -> l.get().iterator()));
    }

    public default <E2> Pipeline<S, E2> rollup() {
        return this.addPipe(new Map<Object, List>(this, o -> o);
    }*/

    ///////////////////// FILTER STEPS /////////////////////

    public default Pipeline<S, E> filter(final Predicate<Holder<E>> predicate) {
        return this.addPipe(new FilterPipe<>(this, predicate));
    }

    public default Pipeline<S, E> dedup() {
        return this.addPipe(new DedupPipe<>(this));
    }

    public default Pipeline<S, E> dedup(final Function<E, ?> uniqueFunction) {
        return this.addPipe(new DedupPipe<>(this, uniqueFunction));
    }

    public default Pipeline<S, E> except(final String variable) {
        return this.addPipe(new ExceptPipe<>(this, variable));
    }

    public default Pipeline<S, E> except(final E exceptionObject) {
        return this.addPipe(new ExceptPipe<>(this, exceptionObject));
    }

    public default Pipeline<S, E> except(final Collection<E> exceptionCollection) {
        return this.addPipe(new ExceptPipe<>(this, exceptionCollection));
    }

    public default <E2> Pipeline<S, E2> has(final String key) {
        return this.addPipe(new HasPipe(this, new HasContainer(key, Contains.IN)));
    }

    public default <E2> Pipeline<S, E2> has(final String key, final Object value) {
        return has(key, Compare.EQUAL, value);
    }

    public default <E2> Pipeline<S, E2> has(final String key, final T t, final Object value) {
        return this.has(key, T.convert(t), value);
    }

    public default <E2> Pipeline<S, E2> hasNot(final String key) {
        return this.addPipe(new HasPipe(this, new HasContainer(key, Contains.NOT_IN)));
    }

    public default <E2> Pipeline<S, E2> has(final String key, final BiPredicate predicate, final Object value) {
        return this.addPipe(new HasPipe(this, new HasContainer(key, predicate, value)));
    }

    public default <E2> Pipeline<S, E2> interval(final String key, final Comparable startValue, final Comparable endValue) {
        return this.addPipe(new IntervalPipe(this,
                new HasContainer(key, Compare.GREATER_THAN_EQUAL, startValue),
                new HasContainer(key, Compare.LESS_THAN, endValue)));
    }

    public default Pipeline<S, E> range(final int low, final int high) {
        return this.addPipe(new RangePipe<>(this, low, high));
    }

    public default Pipeline<S, E> retain(final String variable) {
        return this.addPipe(new RetainPipe<>(this, variable));
    }

    public default Pipeline<S, E> retain(final E retainObject) {
        return this.addPipe(new RetainPipe<>(this, retainObject));
    }

    public default Pipeline<S, E> retain(final Collection<E> retainCollection) {
        return this.addPipe(new RetainPipe<>(this, retainCollection));
    }

    public default Pipeline<S, E> simplePath() {
        return this.addPipe(new SimplePathPipe<>(this));
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public default Pipeline<S, E> sideEffect(final Consumer<Holder<E>> consumer) {
        return this.addPipe(new SideEffectPipe<>(this, consumer));
    }

    public default Pipeline<S, E> aggregate(final String variable, final Function<E, ?>... preAggregateFunctions) {
        return this.addPipe(new AggregatePipe<>(this, variable, preAggregateFunctions));
    }

    public default Pipeline<S, E> groupBy(final String variable, final Function<E, ?> keyFunction, final Function<E, ?> valueFunction, final Function<Collection, ?> reduceFunction) {
        return this.addPipe(new GroupByPipe(this, variable, keyFunction, valueFunction, reduceFunction));
    }

    public default Pipeline<S, E> groupBy(final String variable, final Function<E, ?> keyFunction, final Function<E, ?> valueFunction) {
        return this.addPipe(new GroupByPipe(this, variable, keyFunction, valueFunction));
    }

    public default Pipeline<S, E> groupBy(final String variable, final Function<E, ?> keyFunction) {
        return this.addPipe(new GroupByPipe(this, variable, keyFunction));
    }

    public default Pipeline<S, E> groupCount(final String variable, final Function<E, ?>... preGroupFunctions) {
        return this.addPipe(new GroupCountPipe<>(this, variable, preGroupFunctions));
    }

    public default Pipeline<S, Vertex> linkIn(final String label, final String as) {
        return this.addPipe(new LinkPipe(this, Direction.IN, label, as));
    }

    public default Pipeline<S, Vertex> linkOut(final String label, final String as) {
        return this.addPipe(new LinkPipe(this, Direction.OUT, label, as));
    }

    public default Pipeline<S, Vertex> linkBoth(final String label, final String as) {
        return this.addPipe(new LinkPipe(this, Direction.BOTH, label, as));
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public default Pipeline<S, E> jump(final String as) {
        return this.jump(as, h -> true, h -> false);
    }

    public default Pipeline<S, E> jump(final String as, final Predicate<Holder<E>> ifPredicate) {
        return this.jump(as, ifPredicate, h -> false);
    }

    public default Pipeline<S, E> jump(final String as, final Predicate<Holder<E>> ifPredicate, final Predicate<Holder<E>> emitPredicate) {
        return this.addPipe(new JumpPipe<>(this, as, ifPredicate, emitPredicate));
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public default Pipeline<S, E> as(final String as) {
        if (GremlinHelper.asExists(as, this))
            throw new IllegalStateException("The named pipe already exists");
        final List<Pipe> pipes = this.getPipes();
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

    public default Pipeline iterate() {
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

    public default Pipeline<S, E> getPipeline() {
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

    public default <K> Map<K, Long> groupCount(final Function<E, ?>... preGroupFunctions) {
        final Map<Object, Long> map = new HashMap<>();
        final FunctionRing functionRing = new FunctionRing(preGroupFunctions);
        try {
            while (true) {
                MapHelper.incr(map, functionRing.next().apply(this.next()), 1l);
            }
        } catch (final NoSuchElementException e) {
        }
        return (Map<K, Long>) map;
    }

    public default <K, V> Map<K, V> groupBy(final Function<E, ?> keyFunction) {
        return (Map<K, V>) groupBy(keyFunction, s -> s, null);
    }

    public default <K, V> Map<K, V> groupBy(final Function<E, ?> keyFunction, final Function<E, ?> valueFunction) {
        return this.groupBy(keyFunction, valueFunction, null);
    }

    public default <K, V> Map<K, V> groupBy(final Function<E, ?> keyFunction, final Function<E, ?> valueFunction, final Function<Collection, ?> reduceFunction) {
        final Map<Object, Collection<Object>> groupMap = new HashMap<>();
        final Map<Object, Object> reduceMap = new HashMap<>();
        try {
            while (true) {
                GroupByPipe.doGroup(this.next(), groupMap, (Function) keyFunction, (Function) valueFunction);
            }
        } catch (final NoSuchElementException e) {
        }
        if (null != reduceFunction) {
            GroupByPipe.doReduce(groupMap, reduceMap, (Function) reduceFunction);
            return (Map<K, V>) reduceMap;
        } else {
            return (Map<K, V>) groupMap;
        }
    }

    public default <T> Tree<T> tree(final Function... branchFunctions) {
        final Tree<Object> tree = new Tree<>();
        Tree<Object> depth = tree;
        HolderOptimizer.doPathTracking(this);
        final Pipe endPipe = GremlinHelper.getEnd(this);
        final FunctionRing functionRing = new FunctionRing(branchFunctions);
        try {
            while (true) {
                final Path path = ((Holder) endPipe.next()).getPath();
                for (int i = 0; i < path.size(); i++) {
                    final Object object = functionRing.next().apply(path.get(i));
                    if (!depth.containsKey(object))
                        depth.put(object, new Tree<>());
                    depth = depth.get(object);
                }
                depth = tree;
            }
        } catch (final NoSuchElementException e) {
        }
        return (Tree) tree;
    }

    public default void remove() {
        try {
            while (true) {
                final Object object = this.next();
                if (object instanceof Element)
                    ((Element) object).remove();
                else if (object instanceof Property)
                    ((Property) object).remove();
                else if (object instanceof AnnotatedValue)
                    ((AnnotatedValue) object).remove();
                else {
                    throw new IllegalStateException("The following object does not have a remove() method: " + object);
                }
            }
        } catch (final NoSuchElementException e) {

        }
    }
}
