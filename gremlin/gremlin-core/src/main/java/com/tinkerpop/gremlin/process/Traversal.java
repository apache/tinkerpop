package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.process.oltp.filter.DedupStep;
import com.tinkerpop.gremlin.process.oltp.filter.ExceptStep;
import com.tinkerpop.gremlin.process.oltp.filter.FilterStep;
import com.tinkerpop.gremlin.process.oltp.filter.HasAnnotationStep;
import com.tinkerpop.gremlin.process.oltp.filter.HasStep;
import com.tinkerpop.gremlin.process.oltp.filter.IntervalStep;
import com.tinkerpop.gremlin.process.oltp.filter.RangeStep;
import com.tinkerpop.gremlin.process.oltp.filter.RetainStep;
import com.tinkerpop.gremlin.process.oltp.filter.SimplePathStep;
import com.tinkerpop.gremlin.process.oltp.map.AnnotatedListQueryStep;
import com.tinkerpop.gremlin.process.oltp.map.AnnotatedValueAnnotationValueStep;
import com.tinkerpop.gremlin.process.oltp.map.AnnotationsStep;
import com.tinkerpop.gremlin.process.oltp.map.BackStep;
import com.tinkerpop.gremlin.process.oltp.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.oltp.map.ElementPropertyValueStep;
import com.tinkerpop.gremlin.process.oltp.map.FlatMapStep;
import com.tinkerpop.gremlin.process.oltp.map.IdentityStep;
import com.tinkerpop.gremlin.process.oltp.map.IntersectStep;
import com.tinkerpop.gremlin.process.oltp.map.JumpStep;
import com.tinkerpop.gremlin.process.oltp.map.MapStep;
import com.tinkerpop.gremlin.process.oltp.map.MatchStep;
import com.tinkerpop.gremlin.process.oltp.map.OrderStep;
import com.tinkerpop.gremlin.process.oltp.map.PathStep;
import com.tinkerpop.gremlin.process.oltp.map.PropertyStep;
import com.tinkerpop.gremlin.process.oltp.map.PropertyValuesStep;
import com.tinkerpop.gremlin.process.oltp.map.SelectStep;
import com.tinkerpop.gremlin.process.oltp.map.ShuffleStep;
import com.tinkerpop.gremlin.process.oltp.map.UnionStep;
import com.tinkerpop.gremlin.process.oltp.map.ValueStep;
import com.tinkerpop.gremlin.process.oltp.map.VertexQueryStep;
import com.tinkerpop.gremlin.process.oltp.sideeffect.AggregateStep;
import com.tinkerpop.gremlin.process.oltp.sideeffect.GroupByStep;
import com.tinkerpop.gremlin.process.oltp.sideeffect.GroupCountStep;
import com.tinkerpop.gremlin.process.oltp.sideeffect.LinkStep;
import com.tinkerpop.gremlin.process.oltp.sideeffect.SideEffectStep;
import com.tinkerpop.gremlin.process.oltp.util.FunctionRing;
import com.tinkerpop.gremlin.process.oltp.util.MapHelper;
import com.tinkerpop.gremlin.process.oltp.util.optimizers.HolderOptimizer;
import com.tinkerpop.gremlin.process.oltp.util.structures.Tree;
import com.tinkerpop.gremlin.process.util.GremlinHelper;
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

    public <S, E> Traversal<S, E> addStep(final Step<?, E> step);

    public List<Step> getSteps();

    ///////////////////// TRANSFORM STEPS /////////////////////

    public default <E2> Traversal<S, E2> map(final Function<Holder<E>, E2> function) {
        return this.addStep(new MapStep<>(this, function));
    }

    public default <E2> Traversal<S, E2> flatMap(final Function<Holder<E>, Iterator<E2>> function) {
        return this.addStep(new FlatMapStep<>(this, function));
    }

    public default Traversal<S, E> identity() {
        return this.addStep(new IdentityStep<>(this));
    }

    public default <E2> Traversal<S, E2> annotation(final String annotationKey) {
        return this.addStep(new AnnotatedValueAnnotationValueStep<>(this, annotationKey));
    }

    public default Traversal<S, Map<String, Object>> annotations(final String... annotationKeys) {
        return this.addStep(new AnnotationsStep(this, annotationKeys));
    }

    public default Traversal<S, Vertex> out(final int branchFactor, final String... labels) {
        return this.addStep(new VertexQueryStep<>(this, new VertexQueryBuilder().direction(Direction.OUT).limit(branchFactor).labels(labels), Vertex.class));
    }

    public default Traversal<S, Vertex> out(final String... labels) {
        return this.out(Integer.MAX_VALUE, labels);
    }

    public default Traversal<S, Vertex> in(final int branchFactor, final String... labels) {
        return this.addStep(new VertexQueryStep<>(this, new VertexQueryBuilder().direction(Direction.IN).limit(branchFactor).labels(labels), Vertex.class));
    }

    public default Traversal<S, Vertex> in(final String... labels) {
        return this.in(Integer.MAX_VALUE, labels);
    }

    public default Traversal<S, Vertex> both(final int branchFactor, final String... labels) {
        return this.addStep(new VertexQueryStep<>(this, new VertexQueryBuilder().direction(Direction.BOTH).limit(branchFactor).labels(labels), Vertex.class));
    }

    public default Traversal<S, Vertex> both(final String... labels) {
        return this.both(Integer.MAX_VALUE, labels);
    }

    public default Traversal<S, Edge> outE(final int branchFactor, final String... labels) {
        return this.addStep(new VertexQueryStep<>(this, new VertexQueryBuilder().direction(Direction.OUT).limit(branchFactor).labels(labels), Edge.class));
    }

    public default Traversal<S, Edge> outE(final String... labels) {
        return this.outE(Integer.MAX_VALUE, labels);
    }

    public default Traversal<S, Edge> inE(final int branchFactor, final String... labels) {
        return this.addStep(new VertexQueryStep<>(this, new VertexQueryBuilder().direction(Direction.IN).limit(branchFactor).labels(labels), Edge.class));
    }

    public default Traversal<S, Edge> inE(final String... labels) {
        return this.inE(Integer.MAX_VALUE, labels);
    }

    public default Traversal<S, Edge> bothE(final int branchFactor, final String... labels) {
        return this.addStep(new VertexQueryStep<>(this, new VertexQueryBuilder().direction(Direction.BOTH).limit(branchFactor).labels(labels), Edge.class));
    }

    public default Traversal<S, Edge> bothE(final String... labels) {
        return this.bothE(Integer.MAX_VALUE, labels);
    }

    public default Traversal<S, Vertex> inV() {
        return this.addStep(new EdgeVertexStep(this, Direction.IN));
    }

    public default Traversal<S, Vertex> outV() {
        return this.addStep(new EdgeVertexStep(this, Direction.OUT));
    }

    public default Traversal<S, Vertex> bothV() {
        return this.addStep(new EdgeVertexStep(this, Direction.BOTH));
    }

    public default Traversal<S, E> order() {
        return this.addStep(new OrderStep<E>(this, (a, b) -> ((Comparable<E>) a.get()).compareTo(b.get())));
    }

    public default Traversal<S, E> order(final Comparator<Holder<E>> comparator) {
        return this.addStep(new OrderStep<>(this, comparator));
    }

    public default <E2> Traversal<S, Property<E2>> property(final String propertyKey) {
        return this.addStep(new PropertyStep<>(this, propertyKey));
    }

    public default Traversal<S, E> shuffle() {
        return this.addStep(new ShuffleStep<>(this));
    }

    public default <E2> Traversal<S, E2> value() {
        return this.addStep(new ValueStep<>(this));
    }

    public default <E2> Traversal<S, E2> value(final String propertyKey) {
        return this.addStep(new ElementPropertyValueStep<>(this, propertyKey));
    }

    public default <E2> Traversal<S, E2> value(final String propertyKey, final E2 defaultValue) {
        return this.addStep(new ElementPropertyValueStep<>(this, propertyKey, defaultValue));
    }

    public default <E2> Traversal<S, E2> value(final String propertyKey, final Supplier<E2> defaultSupplier) {
        return this.addStep(new ElementPropertyValueStep<>(this, propertyKey, defaultSupplier));
    }

    public default <E2> Traversal<S, AnnotatedValue<E2>> annotatedValues(final String propertyKey) {
        return this.addStep(new AnnotatedListQueryStep<>(this, propertyKey, new AnnotatedListQueryBuilder()));
    }

    public default Traversal<S, Map<String, Object>> values(final String... propertyKeys) {
        return this.addStep(new PropertyValuesStep(this, propertyKeys));
    }

    public default Traversal<S, Path> path(final Function... pathFunctions) {
        return this.addStep(new PathStep<>(this, pathFunctions));
    }

    public default <E2> Traversal<S, E2> back(final String as) {
        return this.addStep(new BackStep<>(this, as));
    }

    public default <E2> Traversal<S, E2> match(final String inAs, final String outAs, final Traversal... traversals) {
        return this.addStep(new MatchStep<>(this, inAs, outAs, traversals));
    }

    public default Traversal<S, Path> select(final List<String> asLabels, Function... stepFunctions) {
        return this.addStep(new SelectStep(this, asLabels, stepFunctions));
    }

    public default Traversal<S, Path> select(final Function... stepFunctions) {
        return this.addStep(new SelectStep(this, Arrays.asList(), stepFunctions));
    }

    public default <E2> Traversal<S, E2> union(final Traversal<?, E2>... traversals) {
        return this.addStep(new UnionStep(this, traversals));
    }

    public default <E2> Traversal<S, E2> intersect(final Traversal<?, E2>... traversals) {
        return this.addStep(new IntersectStep(this, traversals));
    }

    /*public default <E2> Traversal<S, E2> unroll() {
        return this.addStep(new FlatMapStep<List, Object>(this, l -> l.get().iterator()));
    }

    public default <E2> Traversal<S, E2> rollup() {
        return this.addStep(new Map<Object, List>(this, o -> o);
    }*/

    ///////////////////// FILTER STEPS /////////////////////

    public default Traversal<S, E> filter(final Predicate<Holder<E>> predicate) {
        return this.addStep(new FilterStep<>(this, predicate));
    }

    public default Traversal<S, E> dedup() {
        return this.addStep(new DedupStep<>(this));
    }

    public default Traversal<S, E> dedup(final Function<E, ?> uniqueFunction) {
        return this.addStep(new DedupStep<>(this, uniqueFunction));
    }

    public default Traversal<S, E> except(final String variable) {
        return this.addStep(new ExceptStep<E>(this, variable));
    }

    public default Traversal<S, E> except(final E exceptionObject) {
        return this.addStep(new ExceptStep<>(this, exceptionObject));
    }

    public default Traversal<S, E> except(final Collection<E> exceptionCollection) {
        return this.addStep(new ExceptStep<>(this, exceptionCollection));
    }

    public default <E2> Traversal<S, E2> has(final String key) {
        return this.addStep(new HasStep<>(this, new HasContainer(key, Contains.IN)));
    }

    public default <E2> Traversal<S, E2> has(final String key, final Object value) {
        return has(key, Compare.EQUAL, value);
    }

    public default <E2> Traversal<S, E2> has(final String key, final T t, final Object value) {
        return this.has(key, T.convert(t), value);
    }

    public default <E2> Traversal<S, E2> hasNot(final String key) {
        return this.addStep(new HasStep<>(this, new HasContainer(key, Contains.NOT_IN)));
    }

    public default <E2> Traversal<S, E2> has(final String key, final BiPredicate predicate, final Object value) {
        return this.addStep(new HasStep<>(this, new HasContainer(key, predicate, value)));
    }

    ///////////

    /*public default Traversal<S, Element> has(final String propertyKey, final String annotationKey) {
        return this.addStep(new HasAnnotationStep(this, propertyKey, new HasContainer(annotationKey, Contains.IN)));
    }*/

    public default Traversal<S, Element> has(final String propertyKey, final String annotationKey, final BiPredicate biPredicate, final Object annotationValue) {
        return this.addStep(new HasAnnotationStep(this, propertyKey, new HasContainer(annotationKey, biPredicate, annotationValue)));
    }

    public default Traversal<S, Element> has(final String propertyKey, final String annotationKey, final T t, final Object annotationValue) {
        return this.has(propertyKey, annotationKey, T.convert(t), annotationValue);
    }

    public default Traversal<S, Element> has(final String propertyKey, final String annotationKey, final Object annotationValue) {
        return this.has(propertyKey, annotationKey, Compare.EQUAL, annotationValue);
    }

    /*public default Traversal<S, Element> hasNot(final String propertyKey, final String annotationKey) {
        return this.addStep(new HasAnnotationStep(this, propertyKey, new HasContainer(annotationKey, Contains.NOT_IN)));
    }*/
    //////////

    public default <E2> Traversal<S, E2> interval(final String key, final Comparable startValue, final Comparable endValue) {
        return this.addStep(new IntervalStep(this,
                new HasContainer(key, Compare.GREATER_THAN_EQUAL, startValue),
                new HasContainer(key, Compare.LESS_THAN, endValue)));
    }

    public default Traversal<S, E> range(final int low, final int high) {
        return this.addStep(new RangeStep<>(this, low, high));
    }

    public default Traversal<S, E> retain(final String variable) {
        return this.addStep(new RetainStep<>(this, variable));
    }

    public default Traversal<S, E> retain(final E retainObject) {
        return this.addStep(new RetainStep<>(this, retainObject));
    }

    public default Traversal<S, E> retain(final Collection<E> retainCollection) {
        return this.addStep(new RetainStep<>(this, retainCollection));
    }

    public default Traversal<S, E> simplePath() {
        return this.addStep(new SimplePathStep<>(this));
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public default Traversal<S, E> sideEffect(final Consumer<Holder<E>> consumer) {
        return this.addStep(new SideEffectStep<>(this, consumer));
    }

    public default Traversal<S, E> aggregate(final String variable, final Function<E, ?>... preAggregateFunctions) {
        return this.addStep(new AggregateStep<>(this, variable, preAggregateFunctions));
    }

    public default Traversal<S, E> groupBy(final String variable, final Function<E, ?> keyFunction, final Function<E, ?> valueFunction, final Function<Collection, ?> reduceFunction) {
        return this.addStep(new GroupByStep(this, variable, keyFunction, valueFunction, reduceFunction));
    }

    public default Traversal<S, E> groupBy(final String variable, final Function<E, ?> keyFunction, final Function<E, ?> valueFunction) {
        return this.addStep(new GroupByStep(this, variable, keyFunction, valueFunction));
    }

    public default Traversal<S, E> groupBy(final String variable, final Function<E, ?> keyFunction) {
        return this.addStep(new GroupByStep(this, variable, keyFunction));
    }

    public default Traversal<S, E> groupCount(final String variable, final Function<E, ?>... preGroupFunctions) {
        return this.addStep(new GroupCountStep<>(this, variable, preGroupFunctions));
    }

    public default Traversal<S, Vertex> linkIn(final String label, final String as) {
        return this.addStep(new LinkStep(this, Direction.IN, label, as));
    }

    public default Traversal<S, Vertex> linkOut(final String label, final String as) {
        return this.addStep(new LinkStep(this, Direction.OUT, label, as));
    }

    public default Traversal<S, Vertex> linkBoth(final String label, final String as) {
        return this.addStep(new LinkStep(this, Direction.BOTH, label, as));
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public default Traversal<S, E> jump(final String as) {
        return this.jump(as, h -> true, h -> false);
    }

    public default Traversal<S, E> jump(final String as, final Predicate<Holder<E>> ifPredicate) {
        return this.jump(as, ifPredicate, h -> false);
    }

    public default Traversal<S, E> jump(final String as, final Predicate<Holder<E>> ifPredicate, final Predicate<Holder<E>> emitPredicate) {
        return this.addStep(new JumpStep<>(this, as, ifPredicate, emitPredicate));
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public default Traversal<S, E> as(final String as) {
        if (GremlinHelper.asExists(as, this))
            throw new IllegalStateException("The named step already exists");
        final List<Step> steps = this.getSteps();
        steps.get(steps.size() - 1).setAs(as);
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
                GroupByStep.doGroup(this.next(), groupMap, (Function) keyFunction, (Function) valueFunction);
            }
        } catch (final NoSuchElementException e) {
        }
        if (null != reduceFunction) {
            GroupByStep.doReduce(groupMap, reduceMap, (Function) reduceFunction);
            return (Map<K, V>) reduceMap;
        } else {
            return (Map<K, V>) groupMap;
        }
    }

    public default <T> Tree<T> tree(final Function... branchFunctions) {
        final Tree<Object> tree = new Tree<>();
        Tree<Object> depth = tree;
        HolderOptimizer.doPathTracking(this);
        final Step endStep = GremlinHelper.getEnd(this);
        final FunctionRing functionRing = new FunctionRing(branchFunctions);
        try {
            while (true) {
                final Path path = ((Holder) endStep.next()).getPath();
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
