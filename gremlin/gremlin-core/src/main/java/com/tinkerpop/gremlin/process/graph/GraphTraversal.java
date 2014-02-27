package com.tinkerpop.gremlin.process.graph;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.ranking.PageRankStep;
import com.tinkerpop.gremlin.process.graph.filter.CyclicPathStep;
import com.tinkerpop.gremlin.process.graph.filter.DedupStep;
import com.tinkerpop.gremlin.process.graph.filter.ExceptStep;
import com.tinkerpop.gremlin.process.graph.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.filter.HasAnnotationStep;
import com.tinkerpop.gremlin.process.graph.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.filter.IntervalStep;
import com.tinkerpop.gremlin.process.graph.filter.RangeStep;
import com.tinkerpop.gremlin.process.graph.filter.RetainStep;
import com.tinkerpop.gremlin.process.graph.filter.SimplePathStep;
import com.tinkerpop.gremlin.process.graph.map.AnnotatedValueStep;
import com.tinkerpop.gremlin.process.graph.map.AnnotationValueStep;
import com.tinkerpop.gremlin.process.graph.map.AnnotationValuesStep;
import com.tinkerpop.gremlin.process.graph.map.BackStep;
import com.tinkerpop.gremlin.process.graph.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.graph.map.FlatMapStep;
import com.tinkerpop.gremlin.process.graph.map.IdentityStep;
import com.tinkerpop.gremlin.process.graph.map.IntersectStep;
import com.tinkerpop.gremlin.process.graph.map.JumpStep;
import com.tinkerpop.gremlin.process.graph.map.MapStep;
import com.tinkerpop.gremlin.process.graph.map.MatchStep;
import com.tinkerpop.gremlin.process.graph.map.OrderStep;
import com.tinkerpop.gremlin.process.graph.map.PathStep;
import com.tinkerpop.gremlin.process.graph.map.PropertyStep;
import com.tinkerpop.gremlin.process.graph.map.PropertyValueStep;
import com.tinkerpop.gremlin.process.graph.map.PropertyValuesStep;
import com.tinkerpop.gremlin.process.graph.map.SelectStep;
import com.tinkerpop.gremlin.process.graph.map.ShuffleStep;
import com.tinkerpop.gremlin.process.graph.map.UnionStep;
import com.tinkerpop.gremlin.process.graph.map.ValueStep;
import com.tinkerpop.gremlin.process.graph.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.sideEffect.AggregateStep;
import com.tinkerpop.gremlin.process.graph.sideEffect.GroupByStep;
import com.tinkerpop.gremlin.process.graph.sideEffect.GroupCountStep;
import com.tinkerpop.gremlin.process.graph.sideEffect.LinkStep;
import com.tinkerpop.gremlin.process.graph.sideEffect.SideEffectStep;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.process.graph.util.Tree;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.graph.util.optimizers.HolderOptimizer;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Contains;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import org.javatuples.Pair;

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
public interface GraphTraversal<S, E> extends Traversal<S, E> {

    ///////////////////// TRANSFORM STEPS /////////////////////

    public default <E2> GraphTraversal<S, E2> map(final Function<Holder<E>, E2> function) {
        return (GraphTraversal) this.addStep(new MapStep<>(this, function));
    }

    public default <E2> GraphTraversal<S, E2> flatMap(final Function<Holder<E>, Iterator<E2>> function) {
        return (GraphTraversal) this.addStep(new FlatMapStep<>(this, function));
    }

    public default GraphTraversal<S, E> identity() {
        return (GraphTraversal) this.addStep(new IdentityStep<>(this));
    }

    public default <E2> GraphTraversal<S, E2> annotation(final String annotationKey) {
        return (GraphTraversal) this.addStep(new AnnotationValueStep<>(this, annotationKey));
    }

    public default GraphTraversal<S, Map<String, Object>> annotations(final String... annotationKeys) {
        return (GraphTraversal) this.addStep(new AnnotationValuesStep(this, annotationKeys));
    }

    public default <E2> GraphTraversal<S, AnnotatedValue<E2>> annotatedValues(final String propertyKey) {
        return (GraphTraversal) this.addStep(new AnnotatedValueStep<>(this, propertyKey));
    }

    public default GraphTraversal<S, Vertex> out(final int branchFactor, final String... labels) {
        return (GraphTraversal) this.addStep(new VertexStep(this, Vertex.class, Direction.OUT, branchFactor, labels));
    }

    public default GraphTraversal<S, Vertex> out(final String... labels) {
        return this.out(Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<S, Vertex> in(final int branchFactor, final String... labels) {
        return (GraphTraversal) this.addStep(new VertexStep(this, Vertex.class, Direction.IN, branchFactor, labels));
    }

    public default GraphTraversal<S, Vertex> in(final String... labels) {
        return this.in(Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<S, Vertex> both(final int branchFactor, final String... labels) {
        return (GraphTraversal) this.addStep(new VertexStep(this, Vertex.class, Direction.BOTH, branchFactor, labels));
    }

    public default GraphTraversal<S, Vertex> both(final String... labels) {
        return this.both(Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<S, Edge> outE(final int branchFactor, final String... labels) {
        return (GraphTraversal) this.addStep(new VertexStep(this, Edge.class, Direction.OUT, branchFactor, labels));
    }

    public default GraphTraversal<S, Edge> outE(final String... labels) {
        return this.outE(Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<S, Edge> inE(final int branchFactor, final String... labels) {
        return (GraphTraversal) this.addStep(new VertexStep(this, Edge.class, Direction.IN, branchFactor, labels));
    }

    public default GraphTraversal<S, Edge> inE(final String... labels) {
        return this.inE(Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<S, Edge> bothE(final int branchFactor, final String... labels) {
        return (GraphTraversal) this.addStep(new VertexStep(this, Edge.class, Direction.BOTH, branchFactor, labels));
    }

    public default GraphTraversal<S, Edge> bothE(final String... labels) {
        return this.bothE(Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<S, Vertex> inV() {
        return (GraphTraversal) this.addStep(new EdgeVertexStep(this, Direction.IN));
    }

    public default GraphTraversal<S, Vertex> outV() {
        return (GraphTraversal) this.addStep(new EdgeVertexStep(this, Direction.OUT));
    }

    public default GraphTraversal<S, Vertex> bothV() {
        return (GraphTraversal) this.addStep(new EdgeVertexStep(this, Direction.BOTH));
    }

    public default GraphTraversal<S, E> order() {
        return (GraphTraversal) this.addStep(new OrderStep<E>(this, (a, b) -> ((Comparable<E>) a.get()).compareTo(b.get())));
    }

    public default GraphTraversal<S, E> order(final Comparator<Holder<E>> comparator) {
        return (GraphTraversal) this.addStep(new OrderStep<>(this, comparator));
    }

    public default <E2> GraphTraversal<S, Property<E2>> property(final String propertyKey) {
        return (GraphTraversal) this.addStep(new PropertyStep<>(this, propertyKey));
    }

    public default GraphTraversal<S, E> shuffle() {
        return (GraphTraversal) this.addStep(new ShuffleStep<>(this));
    }

    public default <E2> GraphTraversal<S, E2> value() {
        return (GraphTraversal) this.addStep(new ValueStep<>(this));
    }

    public default <E2> GraphTraversal<S, E2> value(final String propertyKey) {
        return (GraphTraversal) this.addStep(new PropertyValueStep<>(this, propertyKey));
    }

    public default <E2> GraphTraversal<S, E2> value(final String propertyKey, final E2 defaultValue) {
        return (GraphTraversal) this.addStep(new PropertyValueStep<>(this, propertyKey, defaultValue));
    }

    public default <E2> GraphTraversal<S, E2> value(final String propertyKey, final Supplier<E2> defaultSupplier) {
        return (GraphTraversal) this.addStep(new PropertyValueStep<>(this, propertyKey, defaultSupplier));
    }

    public default GraphTraversal<S, Map<String, Object>> values(final String... propertyKeys) {
        return (GraphTraversal) this.addStep(new PropertyValuesStep(this, propertyKeys));
    }

    public default GraphTraversal<S, Path> path(final Function... pathFunctions) {
        return (GraphTraversal) this.addStep(new PathStep<>(this, pathFunctions));
    }

    public default <E2> GraphTraversal<S, E2> back(final String as) {
        return (GraphTraversal) this.addStep(new BackStep<>(this, as));
    }

    public default <E2> GraphTraversal<S, E2> match(final String inAs, final String outAs, final Traversal... traversals) {
        return (GraphTraversal) this.addStep(new MatchStep<>(this, inAs, outAs, traversals));
    }

    public default GraphTraversal<S, Path> select(final List<String> asLabels, Function... stepFunctions) {
        return (GraphTraversal) this.addStep(new SelectStep(this, asLabels, stepFunctions));
    }

    public default GraphTraversal<S, Path> select(final Function... stepFunctions) {
        return (GraphTraversal) this.addStep(new SelectStep(this, Arrays.asList(), stepFunctions));
    }

    public default <E2> GraphTraversal<S, E2> union(final Traversal<?, E2>... traversals) {
        return (GraphTraversal) this.addStep(new UnionStep(this, traversals));
    }

    public default <E2> GraphTraversal<S, E2> intersect(final Traversal<?, E2>... traversals) {
        return (GraphTraversal) this.addStep(new IntersectStep(this, traversals));
    }

    /*public default <E2> Traversal<S, E2> unroll() {
        return this.addStep(new FlatMapStep<List, Object>(this, l -> l.get().iterator()));
    }

    public default <E2> Traversal<S, E2> rollup() {
        return this.addStep(new Map<Object, List>(this, o -> o);
    }*/

    ///////////////////// FILTER STEPS /////////////////////

    public default GraphTraversal<S, E> filter(final Predicate<Holder<E>> predicate) {
        return (GraphTraversal) this.addStep(new FilterStep<>(this, predicate));
    }

    public default GraphTraversal<S, E> dedup() {
        return (GraphTraversal) this.addStep(new DedupStep<>(this));
    }

    public default GraphTraversal<S, E> dedup(final Function<E, ?> uniqueFunction) {
        return (GraphTraversal) this.addStep(new DedupStep<>(this, uniqueFunction));
    }

    public default GraphTraversal<S, E> except(final String variable) {
        return (GraphTraversal) this.addStep(new ExceptStep<E>(this, variable));
    }

    public default GraphTraversal<S, E> except(final E exceptionObject) {
        return (GraphTraversal) this.addStep(new ExceptStep<>(this, exceptionObject));
    }

    public default GraphTraversal<S, E> except(final Collection<E> exceptionCollection) {
        return (GraphTraversal) this.addStep(new ExceptStep<>(this, exceptionCollection));
    }

    public default <E2> GraphTraversal<S, E2> has(final String key) {
        return (GraphTraversal) this.addStep(new HasStep<>(this, new HasContainer(key, Contains.IN)));
    }

    public default <E2> GraphTraversal<S, E2> has(final String key, final Object value) {
        return this.has(key, Compare.EQUAL, value);
    }

    public default <E2> GraphTraversal<S, E2> has(final String key, final T t, final Object value) {
        return this.has(key, T.convert(t), value);
    }

    public default <E2> GraphTraversal<S, E2> hasNot(final String key) {
        return (GraphTraversal) this.addStep(new HasStep<>(this, new HasContainer(key, Contains.NOT_IN)));
    }

    public default <E2> GraphTraversal<S, E2> has(final String key, final BiPredicate predicate, final Object value) {
        return (GraphTraversal) this.addStep(new HasStep<>(this, new HasContainer(key, predicate, value)));
    }

    ///////////

    /*public default Traversal<S, Element> has(final String propertyKey, final String annotationKey) {
        return this.addStep(new HasAnnotationStep(this, propertyKey, new HasContainer(annotationKey, Contains.IN)));
    }*/

    public default GraphTraversal<S, Element> has(final String propertyKey, final String annotationKey, final BiPredicate biPredicate, final Object annotationValue) {
        return (GraphTraversal) this.addStep(new HasAnnotationStep(this, propertyKey, new HasContainer(annotationKey, biPredicate, annotationValue)));
    }

    public default GraphTraversal<S, Element> has(final String propertyKey, final String annotationKey, final T t, final Object annotationValue) {
        return this.has(propertyKey, annotationKey, T.convert(t), annotationValue);
    }

    public default GraphTraversal<S, Element> has(final String propertyKey, final String annotationKey, final Object annotationValue) {
        return this.has(propertyKey, annotationKey, Compare.EQUAL, annotationValue);
    }

    /*public default Traversal<S, Element> hasNot(final String propertyKey, final String annotationKey) {
        return this.addStep(new HasAnnotationStep(this, propertyKey, new HasContainer(annotationKey, Contains.NOT_IN)));
    }*/
    //////////

    public default <E2> GraphTraversal<S, E2> interval(final String key, final Comparable startValue, final Comparable endValue) {
        return (GraphTraversal) this.addStep(new IntervalStep(this,
                new HasContainer(key, Compare.GREATER_THAN_EQUAL, startValue),
                new HasContainer(key, Compare.LESS_THAN, endValue)));
    }

    public default GraphTraversal<S, E> range(final int low, final int high) {
        return (GraphTraversal) this.addStep(new RangeStep<>(this, low, high));
    }

    public default GraphTraversal<S, E> retain(final String variable) {
        return (GraphTraversal) this.addStep(new RetainStep<>(this, variable));
    }

    public default GraphTraversal<S, E> retain(final E retainObject) {
        return (GraphTraversal) this.addStep(new RetainStep<>(this, retainObject));
    }

    public default GraphTraversal<S, E> retain(final Collection<E> retainCollection) {
        return (GraphTraversal) this.addStep(new RetainStep<>(this, retainCollection));
    }

    public default GraphTraversal<S, E> simplePath() {
        return (GraphTraversal) this.addStep(new SimplePathStep<>(this));
    }

    public default GraphTraversal<S, E> cyclicPath() {
        return (GraphTraversal) this.addStep(new CyclicPathStep<E>(this));
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public default GraphTraversal<S, E> sideEffect(final Consumer<Holder<E>> consumer) {
        return (GraphTraversal) this.addStep(new SideEffectStep<>(this, consumer));
    }

    public default GraphTraversal<S, E> aggregate(final String variable, final Function<E, ?>... preAggregateFunctions) {
        return (GraphTraversal) this.addStep(new AggregateStep<>(this, variable, preAggregateFunctions));
    }

    public default GraphTraversal<S, E> groupBy(final String variable, final Function<E, ?> keyFunction, final Function<E, ?> valueFunction, final Function<Collection, ?> reduceFunction) {
        return (GraphTraversal) this.addStep(new GroupByStep(this, variable, keyFunction, valueFunction, reduceFunction));
    }

    public default GraphTraversal<S, E> groupBy(final String variable, final Function<E, ?> keyFunction, final Function<E, ?> valueFunction) {
        return (GraphTraversal) this.addStep(new GroupByStep(this, variable, keyFunction, valueFunction));
    }

    public default GraphTraversal<S, E> groupBy(final String variable, final Function<E, ?> keyFunction) {
        return (GraphTraversal) this.addStep(new GroupByStep(this, variable, keyFunction));
    }

    public default GraphTraversal<S, E> groupCount(final String variable, final Function<E, ?>... preGroupFunctions) {
        return (GraphTraversal) this.addStep(new GroupCountStep<>(this, variable, preGroupFunctions));
    }

    public default GraphTraversal<S, Vertex> linkIn(final String label, final String as) {
        return (GraphTraversal) this.addStep(new LinkStep(this, Direction.IN, label, as));
    }

    public default GraphTraversal<S, Vertex> linkOut(final String label, final String as) {
        return (GraphTraversal) this.addStep(new LinkStep(this, Direction.OUT, label, as));
    }

    public default GraphTraversal<S, Vertex> linkBoth(final String label, final String as) {
        return (GraphTraversal) this.addStep(new LinkStep(this, Direction.BOTH, label, as));
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public default GraphTraversal<S, E> jump(final String as) {
        return this.jump(as, h -> true, h -> false);
    }

    public default GraphTraversal<S, E> jump(final String as, final Predicate<Holder<E>> ifPredicate) {
        return this.jump(as, ifPredicate, h -> false);
    }

    public default GraphTraversal<S, E> jump(final String as, final Predicate<Holder<E>> ifPredicate, final Predicate<Holder<E>> emitPredicate) {
        return (GraphTraversal) this.addStep(new JumpStep<>(this, as, ifPredicate, emitPredicate));
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public default GraphTraversal<S, E> as(final String as) {
        if (TraversalHelper.asExists(as, this))
            throw new IllegalStateException("The named step already exists");
        final List<Step> steps = this.getSteps();
        steps.get(steps.size() - 1).setAs(as);
        return this;

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
        final Step endStep = TraversalHelper.getEnd(this);
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

    public default GraphTraversal<S, E> with(final Object... variableValues) {
        for (int i = 0; i < variableValues.length; i = i + 2) {
            this.memory().set((String) variableValues[i], variableValues[i + 1]);
        }
        return this;
    }

    /////////////////////////////////////

    public default GraphTraversal<S, Pair<Vertex, Double>> pageRank() {
        return (GraphTraversal) this.addStep(new PageRankStep(this));
    }
}
