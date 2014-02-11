package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.process.oltp.filter.DedupPipe;
import com.tinkerpop.gremlin.process.oltp.filter.ExceptPipe;
import com.tinkerpop.gremlin.process.oltp.filter.FilterPipe;
import com.tinkerpop.gremlin.process.oltp.filter.HasAnnotationPipe;
import com.tinkerpop.gremlin.process.oltp.filter.HasPipe;
import com.tinkerpop.gremlin.process.oltp.filter.IntervalPipe;
import com.tinkerpop.gremlin.process.oltp.filter.RangePipe;
import com.tinkerpop.gremlin.process.oltp.filter.RetainPipe;
import com.tinkerpop.gremlin.process.oltp.filter.SimplePathPipe;
import com.tinkerpop.gremlin.process.oltp.map.AnnotatedListQueryPipe;
import com.tinkerpop.gremlin.process.oltp.map.AnnotatedValueAnnotationValuePipe;
import com.tinkerpop.gremlin.process.oltp.map.AnnotationsPipe;
import com.tinkerpop.gremlin.process.oltp.map.BackPipe;
import com.tinkerpop.gremlin.process.oltp.map.EdgeVertexPipe;
import com.tinkerpop.gremlin.process.oltp.map.ElementPropertyValuePipe;
import com.tinkerpop.gremlin.process.oltp.map.FlatMapPipe;
import com.tinkerpop.gremlin.process.oltp.map.IdentityPipe;
import com.tinkerpop.gremlin.process.oltp.map.IntersectPipe;
import com.tinkerpop.gremlin.process.oltp.map.JumpPipe;
import com.tinkerpop.gremlin.process.oltp.map.MapPipe;
import com.tinkerpop.gremlin.process.oltp.map.MatchPipe;
import com.tinkerpop.gremlin.process.oltp.map.OrderPipe;
import com.tinkerpop.gremlin.process.oltp.map.PathPipe;
import com.tinkerpop.gremlin.process.oltp.map.PropertyPipe;
import com.tinkerpop.gremlin.process.oltp.map.PropertyValuesPipe;
import com.tinkerpop.gremlin.process.oltp.map.SelectPipe;
import com.tinkerpop.gremlin.process.oltp.map.ShufflePipe;
import com.tinkerpop.gremlin.process.oltp.map.UnionPipe;
import com.tinkerpop.gremlin.process.oltp.map.ValuePipe;
import com.tinkerpop.gremlin.process.oltp.map.VertexQueryPipe;
import com.tinkerpop.gremlin.process.oltp.sideeffect.AggregatePipe;
import com.tinkerpop.gremlin.process.oltp.sideeffect.GroupByPipe;
import com.tinkerpop.gremlin.process.oltp.sideeffect.GroupCountPipe;
import com.tinkerpop.gremlin.process.oltp.sideeffect.LinkPipe;
import com.tinkerpop.gremlin.process.oltp.sideeffect.SideEffectPipe;
import com.tinkerpop.gremlin.process.oltp.util.FunctionRing;
import com.tinkerpop.gremlin.process.oltp.util.GremlinHelper;
import com.tinkerpop.gremlin.process.oltp.util.MapHelper;
import com.tinkerpop.gremlin.process.oltp.util.optimizers.HolderOptimizer;
import com.tinkerpop.gremlin.process.oltp.util.structures.Tree;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Contains;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.query.util.AnnotatedListQueryBuilder;
import com.tinkerpop.gremlin.structure.query.util.HasContainer;
import com.tinkerpop.gremlin.structure.query.util.VertexQueryBuilder;

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
public interface Traversal<S, E> extends Iterator<E> {

    public Memory memory();

    public Optimizers optimizers();

    public void addStarts(final Iterator<Holder<S>> starts);

    public <S, E> Traversal<S, E> addPipe(final Pipe<?, E> pipe);

    public List<Pipe> getPipes();

    ///////////////////// TRANSFORM STEPS /////////////////////

    public default <E2> Traversal<S, E2> map(final Function<Holder<E>, E2> function) {
        return this.addPipe(new MapPipe<>(this, function));
    }

    public default <E2> Traversal<S, E2> flatMap(final Function<Holder<E>, Iterator<E2>> function) {
        return this.addPipe(new FlatMapPipe<>(this, function));
    }

    public default Traversal<S, E> identity() {
        return this.addPipe(new IdentityPipe<>(this));
    }

    public default <E2> Traversal<S, E2> annotation(final String annotationKey) {
        return this.addPipe(new AnnotatedValueAnnotationValuePipe<>(this, annotationKey));
    }

    public default Traversal<S, Map<String, Object>> annotations(final String... annotationKeys) {
        return this.addPipe(new AnnotationsPipe(this, annotationKeys));
    }

    public default Traversal<S, Vertex> out(final int branchFactor, final String... labels) {
        return this.addPipe(new VertexQueryPipe<>(this, new VertexQueryBuilder().direction(Direction.OUT).limit(branchFactor).labels(labels), Vertex.class));
    }

    public default Traversal<S, Vertex> out(final String... labels) {
        return this.out(Integer.MAX_VALUE, labels);
    }

    public default Traversal<S, Vertex> in(final int branchFactor, final String... labels) {
        return this.addPipe(new VertexQueryPipe<>(this, new VertexQueryBuilder().direction(Direction.IN).limit(branchFactor).labels(labels), Vertex.class));
    }

    public default Traversal<S, Vertex> in(final String... labels) {
        return this.in(Integer.MAX_VALUE, labels);
    }

    public default Traversal<S, Vertex> both(final int branchFactor, final String... labels) {
        return this.addPipe(new VertexQueryPipe<>(this, new VertexQueryBuilder().direction(Direction.BOTH).limit(branchFactor).labels(labels), Vertex.class));
    }

    public default Traversal<S, Vertex> both(final String... labels) {
        return this.both(Integer.MAX_VALUE, labels);
    }

    public default Traversal<S, Edge> outE(final int branchFactor, final String... labels) {
        return this.addPipe(new VertexQueryPipe<>(this, new VertexQueryBuilder().direction(Direction.OUT).limit(branchFactor).labels(labels), Edge.class));
    }

    public default Traversal<S, Edge> outE(final String... labels) {
        return this.outE(Integer.MAX_VALUE, labels);
    }

    public default Traversal<S, Edge> inE(final int branchFactor, final String... labels) {
        return this.addPipe(new VertexQueryPipe<>(this, new VertexQueryBuilder().direction(Direction.IN).limit(branchFactor).labels(labels), Edge.class));
    }

    public default Traversal<S, Edge> inE(final String... labels) {
        return this.inE(Integer.MAX_VALUE, labels);
    }

    public default Traversal<S, Edge> bothE(final int branchFactor, final String... labels) {
        return this.addPipe(new VertexQueryPipe<>(this, new VertexQueryBuilder().direction(Direction.BOTH).limit(branchFactor).labels(labels), Edge.class));
    }

    public default Traversal<S, Edge> bothE(final String... labels) {
        return this.bothE(Integer.MAX_VALUE, labels);
    }

    public default Traversal<S, Vertex> inV() {
        return this.addPipe(new EdgeVertexPipe(this, Direction.IN));
    }

    public default Traversal<S, Vertex> outV() {
        return this.addPipe(new EdgeVertexPipe(this, Direction.OUT));
    }

    public default Traversal<S, Vertex> bothV() {
        return this.addPipe(new EdgeVertexPipe(this, Direction.BOTH));
    }

    public default Traversal<S, E> order() {
        return this.addPipe(new OrderPipe<E>(this, (a, b) -> ((Comparable<E>) a.get()).compareTo(b.get())));
    }

    public default Traversal<S, E> order(final Comparator<Holder<E>> comparator) {
        return this.addPipe(new OrderPipe<>(this, comparator));
    }

    public default <E2> Traversal<S, Property<E2>> property(final String propertyKey) {
        return this.addPipe(new PropertyPipe<>(this, propertyKey));
    }

    public default Traversal<S, E> shuffle() {
        return this.addPipe(new ShufflePipe<>(this));
    }

    public default <E2> Traversal<S, E2> value() {
        return this.addPipe(new ValuePipe<>(this));
    }

    public default <E2> Traversal<S, E2> value(final String propertyKey) {
        return this.addPipe(new ElementPropertyValuePipe<>(this, propertyKey));
    }

    public default <E2> Traversal<S, E2> value(final String propertyKey, final E2 defaultValue) {
        return this.addPipe(new ElementPropertyValuePipe<>(this, propertyKey, defaultValue));
    }

    public default <E2> Traversal<S, E2> value(final String propertyKey, final Supplier<E2> defaultSupplier) {
        return this.addPipe(new ElementPropertyValuePipe<>(this, propertyKey, defaultSupplier));
    }

    public default <E2> Traversal<S, AnnotatedValue<E2>> annotatedValues(final String propertyKey) {
        return this.addPipe(new AnnotatedListQueryPipe<>(this, propertyKey, new AnnotatedListQueryBuilder()));
    }

    public default Traversal<S, Map<String, Object>> values(final String... propertyKeys) {
        return this.addPipe(new PropertyValuesPipe(this, propertyKeys));
    }

    public default Traversal<S, Path> path(final Function... pathFunctions) {
        return this.addPipe(new PathPipe<>(this, pathFunctions));
    }

    public default <E2> Traversal<S, E2> back(final String as) {
        return this.addPipe(new BackPipe<>(this, as));
    }

    public default <E2> Traversal<S, E2> match(final String inAs, final String outAs, final Traversal... pipelines) {
        return this.addPipe(new MatchPipe<>(this, inAs, outAs, pipelines));
    }

    public default Traversal<S, Path> select(final List<String> asLabels, Function... stepFunctions) {
        return this.addPipe(new SelectPipe(this, asLabels, stepFunctions));
    }

    public default Traversal<S, Path> select(final Function... stepFunctions) {
        return this.addPipe(new SelectPipe(this, Arrays.asList(), stepFunctions));
    }

    public default <E2> Traversal<S, E2> union(final Traversal<?, E2>... pipelines) {
        return this.addPipe(new UnionPipe(this, pipelines));
    }

    public default <E2> Traversal<S, E2> intersect(final Traversal<?, E2>... pipelines) {
        return this.addPipe(new IntersectPipe(this, pipelines));
    }

    /*public default <E2> Traversal<S, E2> unroll() {
        return this.addPipe(new FlatMapPipe<List, Object>(this, l -> l.get().iterator()));
    }

    public default <E2> Traversal<S, E2> rollup() {
        return this.addPipe(new Map<Object, List>(this, o -> o);
    }*/

    ///////////////////// FILTER STEPS /////////////////////

    public default Traversal<S, E> filter(final Predicate<Holder<E>> predicate) {
        return this.addPipe(new FilterPipe<>(this, predicate));
    }

    public default Traversal<S, E> dedup() {
        return this.addPipe(new DedupPipe<>(this));
    }

    public default Traversal<S, E> dedup(final Function<E, ?> uniqueFunction) {
        return this.addPipe(new DedupPipe<>(this, uniqueFunction));
    }

    public default Traversal<S, E> except(final String variable) {
        return this.addPipe(new ExceptPipe<E>(this, variable));
    }

    public default Traversal<S, E> except(final E exceptionObject) {
        return this.addPipe(new ExceptPipe<>(this, exceptionObject));
    }

    public default Traversal<S, E> except(final Collection<E> exceptionCollection) {
        return this.addPipe(new ExceptPipe<>(this, exceptionCollection));
    }

    public default <E2> Traversal<S, E2> has(final String key) {
        return this.addPipe(new HasPipe<>(this, new HasContainer(key, Contains.IN)));
    }

    public default <E2> Traversal<S, E2> has(final String key, final Object value) {
        return has(key, Compare.EQUAL, value);
    }

    public default <E2> Traversal<S, E2> has(final String key, final T t, final Object value) {
        return this.has(key, T.convert(t), value);
    }

    public default <E2> Traversal<S, E2> hasNot(final String key) {
        return this.addPipe(new HasPipe<>(this, new HasContainer(key, Contains.NOT_IN)));
    }

    public default <E2> Traversal<S, E2> has(final String key, final BiPredicate predicate, final Object value) {
        return this.addPipe(new HasPipe<>(this, new HasContainer(key, predicate, value)));
    }

    ///////////

    /*public default Traversal<S, Element> has(final String propertyKey, final String annotationKey) {
        return this.addPipe(new HasAnnotationPipe(this, propertyKey, new HasContainer(annotationKey, Contains.IN)));
    }*/

    public default Traversal<S, Element> has(final String propertyKey, final String annotationKey, final BiPredicate biPredicate, final Object annotationValue) {
        return this.addPipe(new HasAnnotationPipe(this, propertyKey, new HasContainer(annotationKey, biPredicate, annotationValue)));
    }

    public default Traversal<S, Element> has(final String propertyKey, final String annotationKey, final T t, final Object annotationValue) {
        return this.has(propertyKey, annotationKey, T.convert(t), annotationValue);
    }

    public default Traversal<S, Element> has(final String propertyKey, final String annotationKey, final Object annotationValue) {
        return this.has(propertyKey, annotationKey, Compare.EQUAL, annotationValue);
    }

    /*public default Traversal<S, Element> hasNot(final String propertyKey, final String annotationKey) {
        return this.addPipe(new HasAnnotationPipe(this, propertyKey, new HasContainer(annotationKey, Contains.NOT_IN)));
    }*/
    //////////

    public default <E2> Traversal<S, E2> interval(final String key, final Comparable startValue, final Comparable endValue) {
        return this.addPipe(new IntervalPipe(this,
                new HasContainer(key, Compare.GREATER_THAN_EQUAL, startValue),
                new HasContainer(key, Compare.LESS_THAN, endValue)));
    }

    public default Traversal<S, E> range(final int low, final int high) {
        return this.addPipe(new RangePipe<>(this, low, high));
    }

    public default Traversal<S, E> retain(final String variable) {
        return this.addPipe(new RetainPipe<>(this, variable));
    }

    public default Traversal<S, E> retain(final E retainObject) {
        return this.addPipe(new RetainPipe<>(this, retainObject));
    }

    public default Traversal<S, E> retain(final Collection<E> retainCollection) {
        return this.addPipe(new RetainPipe<>(this, retainCollection));
    }

    public default Traversal<S, E> simplePath() {
        return this.addPipe(new SimplePathPipe<>(this));
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public default Traversal<S, E> sideEffect(final Consumer<Holder<E>> consumer) {
        return this.addPipe(new SideEffectPipe<>(this, consumer));
    }

    public default Traversal<S, E> aggregate(final String variable, final Function<E, ?>... preAggregateFunctions) {
        return this.addPipe(new AggregatePipe<>(this, variable, preAggregateFunctions));
    }

    public default Traversal<S, E> groupBy(final String variable, final Function<E, ?> keyFunction, final Function<E, ?> valueFunction, final Function<Collection, ?> reduceFunction) {
        return this.addPipe(new GroupByPipe(this, variable, keyFunction, valueFunction, reduceFunction));
    }

    public default Traversal<S, E> groupBy(final String variable, final Function<E, ?> keyFunction, final Function<E, ?> valueFunction) {
        return this.addPipe(new GroupByPipe(this, variable, keyFunction, valueFunction));
    }

    public default Traversal<S, E> groupBy(final String variable, final Function<E, ?> keyFunction) {
        return this.addPipe(new GroupByPipe(this, variable, keyFunction));
    }

    public default Traversal<S, E> groupCount(final String variable, final Function<E, ?>... preGroupFunctions) {
        return this.addPipe(new GroupCountPipe<>(this, variable, preGroupFunctions));
    }

    public default Traversal<S, Vertex> linkIn(final String label, final String as) {
        return this.addPipe(new LinkPipe(this, Direction.IN, label, as));
    }

    public default Traversal<S, Vertex> linkOut(final String label, final String as) {
        return this.addPipe(new LinkPipe(this, Direction.OUT, label, as));
    }

    public default Traversal<S, Vertex> linkBoth(final String label, final String as) {
        return this.addPipe(new LinkPipe(this, Direction.BOTH, label, as));
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public default Traversal<S, E> jump(final String as) {
        return this.jump(as, h -> true, h -> false);
    }

    public default Traversal<S, E> jump(final String as, final Predicate<Holder<E>> ifPredicate) {
        return this.jump(as, ifPredicate, h -> false);
    }

    public default Traversal<S, E> jump(final String as, final Predicate<Holder<E>> ifPredicate, final Predicate<Holder<E>> emitPredicate) {
        return this.addPipe(new JumpPipe<>(this, as, ifPredicate, emitPredicate));
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public default Traversal<S, E> as(final String as) {
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
