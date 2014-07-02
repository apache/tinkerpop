package com.tinkerpop.gremlin.process.graph;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.ranking.PageRankStep;
import com.tinkerpop.gremlin.process.graph.step.filter.CyclicPathStep;
import com.tinkerpop.gremlin.process.graph.step.filter.DedupStep;
import com.tinkerpop.gremlin.process.graph.step.filter.ExceptStep;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.step.filter.IdentityStep;
import com.tinkerpop.gremlin.process.graph.step.filter.IntervalStep;
import com.tinkerpop.gremlin.process.graph.step.filter.PathIdentityStep;
import com.tinkerpop.gremlin.process.graph.step.filter.RandomStep;
import com.tinkerpop.gremlin.process.graph.step.filter.RangeStep;
import com.tinkerpop.gremlin.process.graph.step.filter.RetainStep;
import com.tinkerpop.gremlin.process.graph.step.filter.SimplePathStep;
import com.tinkerpop.gremlin.process.graph.step.map.BackStep;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeOtherVertexStep;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.graph.step.map.ElementPropertyStep;
import com.tinkerpop.gremlin.process.graph.step.map.ElementValueStep;
import com.tinkerpop.gremlin.process.graph.step.map.ElementValuesStep;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;
import com.tinkerpop.gremlin.process.graph.step.map.IdStep;
import com.tinkerpop.gremlin.process.graph.step.map.IntersectStep;
import com.tinkerpop.gremlin.process.graph.step.map.JumpStep;
import com.tinkerpop.gremlin.process.graph.step.map.LabelStep;
import com.tinkerpop.gremlin.process.graph.step.map.MapStep;
import com.tinkerpop.gremlin.process.graph.step.map.MatchStep;
import com.tinkerpop.gremlin.process.graph.step.map.OrderStep;
import com.tinkerpop.gremlin.process.graph.step.map.PathStep;
import com.tinkerpop.gremlin.process.graph.step.map.PropertyValueStep;
import com.tinkerpop.gremlin.process.graph.step.map.SelectStep;
import com.tinkerpop.gremlin.process.graph.step.map.ShuffleStep;
import com.tinkerpop.gremlin.process.graph.step.map.StartStep;
import com.tinkerpop.gremlin.process.graph.step.map.UnionStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AggregateStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupByStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupCountStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.LinkStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SubgraphStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.TimeLimitStep;
import com.tinkerpop.gremlin.process.graph.step.util.Tree;
import com.tinkerpop.gremlin.process.graph.strategy.PathConsumerStrategy;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Contains;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.util.function.SConsumer;
import com.tinkerpop.gremlin.util.function.SFunction;
import com.tinkerpop.gremlin.util.function.SPredicate;
import com.tinkerpop.gremlin.util.function.SSupplier;
import org.javatuples.Pair;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GraphTraversal<S, E> extends Traversal<S, E> {

    public static GraphTraversal of() {
        final GraphTraversal traversal = new DefaultGraphTraversal<>();
        traversal.addStep(new IdentityStep(traversal));
        return traversal;
    }

    public default GraphTraversal<S, E> submit(final TraversalEngine engine) {
        final GraphTraversal<S, E> traversal = new DefaultGraphTraversal<>();
        traversal.addStep(new StartStep<>(traversal, engine.execute(this)));
        return traversal;
    }

    public default GraphTraversal<S, E> trackPaths() {
        return (GraphTraversal) this.addStep(new PathIdentityStep<>(this));
    }

    ///////////////////// TRANSFORM STEPS /////////////////////

    public default <E2> GraphTraversal<S, E2> map(final SFunction<Traverser<E>, E2> function) {
        final MapStep<E, E2> mapStep = new MapStep<>(this);
        mapStep.setFunction(function);
        return (GraphTraversal) this.addStep(mapStep);
    }

    public default <E2> GraphTraversal<S, E2> flatMap(final SFunction<Traverser<E>, Iterator<E2>> function) {
        final FlatMapStep<E, E2> flatMapStep = new FlatMapStep<>(this);
        flatMapStep.setFunction(function);
        return (GraphTraversal) this.addStep(flatMapStep);
    }

    public default GraphTraversal<S, Object> id() {
        return (GraphTraversal) this.addStep(new IdStep<Element>(this));
    }

    public default GraphTraversal<S, String> label() {
        return (GraphTraversal) this.addStep(new LabelStep<Element>(this));
    }

    public default GraphTraversal<S, E> identity() {
        return (GraphTraversal) this.addStep(new IdentityStep<>(this));
    }

    public default GraphTraversal<S, Vertex> to(final Direction direction, final int branchFactor, final String... labels) {
        return (GraphTraversal) this.addStep(new VertexStep(this, Vertex.class, direction, branchFactor, labels));
    }

    public default GraphTraversal<S, Vertex> to(final Direction direction, final String... labels) {
        return this.to(direction, Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<S, Vertex> out(final int branchFactor, final String... labels) {
        return this.to(Direction.OUT, branchFactor, labels);
    }

    public default GraphTraversal<S, Vertex> out(final String... labels) {
        return this.to(Direction.OUT, Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<S, Vertex> in(final int branchFactor, final String... labels) {
        return this.to(Direction.IN, branchFactor, labels);
    }

    public default GraphTraversal<S, Vertex> in(final String... labels) {
        return this.to(Direction.IN, Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<S, Vertex> both(final int branchFactor, final String... labels) {
        return this.to(Direction.BOTH, branchFactor, labels);
    }

    public default GraphTraversal<S, Vertex> both(final String... labels) {
        return this.to(Direction.BOTH, Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<S, Edge> toE(final Direction direction, final int branchFactor, final String... labels) {
        return (GraphTraversal) this.addStep(new VertexStep(this, Edge.class, direction, branchFactor, labels));
    }

    public default GraphTraversal<S, Edge> toE(final Direction direction, final String... labels) {
        return this.toE(direction, Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<S, Edge> outE(final int branchFactor, final String... labels) {
        return this.toE(Direction.OUT, branchFactor, labels);
    }

    public default GraphTraversal<S, Edge> outE(final String... labels) {
        return this.toE(Direction.OUT, Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<S, Edge> inE(final int branchFactor, final String... labels) {
        return this.toE(Direction.IN, branchFactor, labels);
    }

    public default GraphTraversal<S, Edge> inE(final String... labels) {
        return this.toE(Direction.IN, Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<S, Edge> bothE(final int branchFactor, final String... labels) {
        return this.toE(Direction.BOTH, branchFactor, labels);
    }

    public default GraphTraversal<S, Edge> bothE(final String... labels) {
        return this.toE(Direction.BOTH, Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<S, Vertex> toV(final Direction direction) {
        return (GraphTraversal) this.addStep(new EdgeVertexStep(this, direction));
    }

    public default GraphTraversal<S, Vertex> inV() {
        return this.toV(Direction.IN);
    }

    public default GraphTraversal<S, Vertex> outV() {
        return this.toV(Direction.OUT);
    }

    public default GraphTraversal<S, Vertex> bothV() {
        return this.toV(Direction.BOTH);
    }

    public default GraphTraversal<S, Vertex> otherV() {
        return (GraphTraversal) this.addStep(new EdgeOtherVertexStep(this));
    }

    public default GraphTraversal<S, E> order() {
        return (GraphTraversal) this.addStep(new OrderStep<E>(this, (a, b) -> ((Comparable<E>) a.get()).compareTo(b.get())));
    }

    public default GraphTraversal<S, E> order(final Comparator<Traverser<E>> comparator) {
        return (GraphTraversal) this.addStep(new OrderStep<>(this, comparator));
    }

    public default <E2> GraphTraversal<S, Property<E2>> property(final String propertyKey) {
        return (GraphTraversal) this.addStep(new ElementPropertyStep<>(this, propertyKey));
    }

    public default GraphTraversal<S, E> shuffle() {
        return (GraphTraversal) this.addStep(new ShuffleStep<>(this));
    }

    public default <E2> GraphTraversal<S, E2> value() {
        return (GraphTraversal) this.addStep(new PropertyValueStep<>(this));
    }

    public default <E2> GraphTraversal<S, E2> value(final String propertyKey) {
        return (GraphTraversal) this.addStep(new ElementValueStep<>(this, propertyKey));
    }

    public default <E2> GraphTraversal<S, E2> value(final String propertyKey, final E2 defaultValue) {
        return (GraphTraversal) this.addStep(new ElementValueStep<>(this, propertyKey, defaultValue));
    }

    public default <E2> GraphTraversal<S, E2> value(final String propertyKey, final Supplier<E2> defaultSupplier) {
        return (GraphTraversal) this.addStep(new ElementValueStep<>(this, propertyKey, defaultSupplier));
    }

    public default GraphTraversal<S, Map<String, Object>> values(final String... propertyKeys) {
        return (GraphTraversal) this.addStep(new ElementValuesStep(this, propertyKeys));
    }

    public default GraphTraversal<S, Path> path(final SFunction... pathFunctions) {
        return (GraphTraversal) this.addStep(new PathStep<>(this, pathFunctions));
    }

    public default <E2> GraphTraversal<S, E2> back(final String as) {
        return (GraphTraversal) this.addStep(new BackStep<>(this, as));
    }

    public default <E2> GraphTraversal<S, E2> match(final String inAs, final String outAs, final Traversal... traversals) {
        return (GraphTraversal) this.addStep(new MatchStep<>(this, inAs, outAs, traversals));
    }

    public default GraphTraversal<S, Path> select(final List<String> asLabels, SFunction... stepFunctions) {
        return (GraphTraversal) this.addStep(new SelectStep(this, asLabels, stepFunctions));
    }

    public default GraphTraversal<S, Path> select(final SFunction... stepFunctions) {
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

    public default GraphTraversal<S, E> filter(final SPredicate<Traverser<E>> predicate) {
        final FilterStep<E> filterStep = new FilterStep<>(this);
        filterStep.setPredicate(predicate);
        return (GraphTraversal) this.addStep(filterStep);
    }

    public default GraphTraversal<S, E> dedup() {
        return (GraphTraversal) this.addStep(new DedupStep<>(this));
    }

    public default GraphTraversal<S, E> dedup(final SFunction<E, ?> uniqueFunction) {
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
        return (GraphTraversal) this.addStep(new HasStep(this, new HasContainer(key, Contains.IN)));
    }

    public default <E2> GraphTraversal<S, E2> has(final String key, final Object value) {
        return this.has(key, Compare.EQUAL, value);
    }

    public default <E2> GraphTraversal<S, E2> has(final String key, final T t, final Object value) {
        return this.has(key, T.convert(t), value);
    }

    public default <E2> GraphTraversal<S, E2> has(final String key, final BiPredicate predicate, final Object value) {
        return (GraphTraversal) this.addStep(new HasStep(this, new HasContainer(key, predicate, value)));
    }

    public default <E2> GraphTraversal<S, E2> hasNot(final String key) {
        return (GraphTraversal) this.addStep(new HasStep(this, new HasContainer(key, Contains.NOT_IN)));
    }

    public default <E2> GraphTraversal<S, E2> interval(final String key, final Comparable startValue, final Comparable endValue) {
        return (GraphTraversal) this.addStep(new IntervalStep(this,
                new HasContainer(key, Compare.GREATER_THAN_EQUAL, startValue),
                new HasContainer(key, Compare.LESS_THAN, endValue)));
    }

    public default GraphTraversal<S, E> random(final double probability) {
        return (GraphTraversal) this.addStep(new RandomStep<>(this, probability));
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

    public default GraphTraversal<S, E> sideEffect(final SConsumer<Traverser<E>> consumer) {
        return (GraphTraversal) this.addStep(new SideEffectStep<>(this, consumer));
    }

    public default <E2> GraphTraversal<S, E2> cap(final String variable) {
        return (GraphTraversal) this.addStep(new SideEffectCapStep<>(this, variable));
    }

    public default <E2> GraphTraversal<S, E2> cap() {
        return this.cap(SideEffectCapable.CAP_KEY);
    }

    public default GraphTraversal<S, E> subgraph(final Graph g, final SPredicate<Edge> includeEdge) {
        return (GraphTraversal) this.addStep(new SubgraphStep<>(this, g, null, null, includeEdge));
    }

    public default GraphTraversal<S, E> subgraph(final Graph g, final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final SPredicate<Edge> includeEdge) {
        return (GraphTraversal) this.addStep(new SubgraphStep<>(this, g, edgeIdHolder, vertexMap, includeEdge));
    }

    public default GraphTraversal<S, E> aggregate(final String variable, final SFunction<E, ?>... preAggregateFunctions) {
        return (GraphTraversal) this.addStep(new AggregateStep<>(this, variable, preAggregateFunctions));
    }

    public default GraphTraversal<S, E> groupBy(final String variable, final SFunction<E, ?> keyFunction, final SFunction<E, ?> valueFunction, final SFunction<Collection, ?> reduceFunction) {
        return (GraphTraversal) this.addStep(new GroupByStep(this, variable, keyFunction, valueFunction, reduceFunction));
    }

    public default GraphTraversal<S, E> groupBy(final String variable, final SFunction<E, ?> keyFunction, final SFunction<E, ?> valueFunction) {
        return (GraphTraversal) this.groupBy(variable, keyFunction, valueFunction, null);
    }

    public default GraphTraversal<S, E> groupBy(final String variable, final SFunction<E, ?> keyFunction) {
        return (GraphTraversal) this.groupBy(variable, keyFunction, null, null);
    }

    public default GraphTraversal<S, E> groupBy(final SFunction<E, ?> keyFunction) {
        return (GraphTraversal) this.groupBy(keyFunction, null, null);
    }

    public default GraphTraversal<S, E> groupBy(final SFunction<E, ?> keyFunction, final SFunction<E, ?> valueFunction) {
        return (GraphTraversal) this.groupBy(keyFunction, valueFunction, null);
    }

    public default GraphTraversal<S, E> groupBy(final SFunction<E, ?> keyFunction, final SFunction<E, ?> valueFunction, final SFunction<Collection, ?> reduceFunction) {
        return (GraphTraversal) this.addStep(new GroupByStep(this, SideEffectCapable.CAP_KEY, keyFunction, (SFunction) valueFunction, (SFunction) reduceFunction));
    }

    public default GraphTraversal<S, E> groupCount(final String variable, final SFunction<E, ?>... preGroupFunctions) {
        return (GraphTraversal) this.addStep(new GroupCountStep<>(this, variable, preGroupFunctions));
    }

    public default GraphTraversal<S, E> groupCount(final SFunction<E, ?>... preGroupFunctions) {
        return (GraphTraversal) this.addStep(new GroupCountStep<>(this, SideEffectCapable.CAP_KEY, preGroupFunctions));
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

    public default GraphTraversal<S, E> timeLimit(final long timeLimit) {
        return (GraphTraversal) this.addStep(new TimeLimitStep<E>(this, timeLimit));
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public default GraphTraversal<S, E> jump(final String as) {
        return this.jump(as, h -> true, h -> false);
    }

    public default GraphTraversal<S, E> jump(final String as, final SPredicate<Traverser<E>> ifPredicate) {
        return this.jump(as, ifPredicate, h -> false);
    }

    public default GraphTraversal<S, E> jump(final String as, final SPredicate<Traverser<E>> ifPredicate, final SPredicate<Traverser<E>> emitPredicate) {
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

    // TODO: this should be side effect capable.
    public default <T> Tree<T> tree(final SFunction... branchFunctions) {
        final Tree<Object> tree = new Tree<>();
        Tree<Object> depth = tree;
        PathConsumerStrategy.doPathTracking(this);
        final Step endStep = TraversalHelper.getEnd(this);
        final FunctionRing functionRing = new FunctionRing(branchFunctions);
        try {
            while (true) {
                final Path path = ((Traverser) endStep.next()).getPath();
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
                else {
                    // TODO: USE REFLECTION TO FIND REMOVE?
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

    public default GraphTraversal<S, Pair<Vertex, Double>> pageRank(final SSupplier<Traversal<Vertex, Edge>> incidentTraversal) {
        return (GraphTraversal) this.addStep(new PageRankStep(this, incidentTraversal));
    }
}
