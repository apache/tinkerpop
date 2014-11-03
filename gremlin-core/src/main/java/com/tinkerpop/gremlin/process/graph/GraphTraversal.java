package com.tinkerpop.gremlin.process.graph;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.branch.ChooseStep;
import com.tinkerpop.gremlin.process.graph.step.branch.JumpStep;
import com.tinkerpop.gremlin.process.graph.step.branch.UnionStep;
import com.tinkerpop.gremlin.process.graph.step.branch.UntilStep;
import com.tinkerpop.gremlin.process.graph.step.filter.CyclicPathStep;
import com.tinkerpop.gremlin.process.graph.step.filter.DedupStep;
import com.tinkerpop.gremlin.process.graph.step.filter.ExceptStep;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.step.filter.IntervalStep;
import com.tinkerpop.gremlin.process.graph.step.filter.LocalRangeStep;
import com.tinkerpop.gremlin.process.graph.step.filter.RandomStep;
import com.tinkerpop.gremlin.process.graph.step.filter.RangeStep;
import com.tinkerpop.gremlin.process.graph.step.filter.RetainStep;
import com.tinkerpop.gremlin.process.graph.step.filter.SimplePathStep;
import com.tinkerpop.gremlin.process.graph.step.filter.TimeLimitStep;
import com.tinkerpop.gremlin.process.graph.step.filter.WhereStep;
import com.tinkerpop.gremlin.process.graph.step.map.BackStep;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeOtherVertexStep;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;
import com.tinkerpop.gremlin.process.graph.step.map.FoldStep;
import com.tinkerpop.gremlin.process.graph.step.map.IdStep;
import com.tinkerpop.gremlin.process.graph.step.map.KeyStep;
import com.tinkerpop.gremlin.process.graph.step.map.LabelStep;
import com.tinkerpop.gremlin.process.graph.step.map.MapStep;
import com.tinkerpop.gremlin.process.graph.step.map.OrderByStep;
import com.tinkerpop.gremlin.process.graph.step.map.OrderStep;
import com.tinkerpop.gremlin.process.graph.step.map.PathStep;
import com.tinkerpop.gremlin.process.graph.step.map.PropertiesStep;
import com.tinkerpop.gremlin.process.graph.step.map.PropertyMapStep;
import com.tinkerpop.gremlin.process.graph.step.map.PropertyValueStep;
import com.tinkerpop.gremlin.process.graph.step.map.SelectOneStep;
import com.tinkerpop.gremlin.process.graph.step.map.SelectStep;
import com.tinkerpop.gremlin.process.graph.step.map.ShuffleStep;
import com.tinkerpop.gremlin.process.graph.step.map.UnfoldStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.step.map.match.MatchStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AddEdgeStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AggregateStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupByStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupCountStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.InjectStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StoreStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SubgraphStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.TreeStep;
import com.tinkerpop.gremlin.process.graph.step.util.PathIdentityStep;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.marker.CapTraversal;
import com.tinkerpop.gremlin.process.marker.CountTraversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Contains;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Order;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.PropertyType;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.HasContainer;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GraphTraversal<S, E> extends Traversal<S, E>, CountTraversal<S, E>, CapTraversal<S, E> {

    @Override
    public default GraphTraversal<S, E> submit(final GraphComputer computer) {
        return (GraphTraversal) CountTraversal.super.<S, E>submit(computer);
    }

    public static <S> GraphTraversal<S, S> of(final Graph graph) {
        final GraphTraversal<S, S> traversal = new DefaultGraphTraversal<>();
        traversal.sideEffects().setGraph(graph);
        return traversal;
    }

    public static <S> GraphTraversal<S, S> of() {
        return new DefaultGraphTraversal<>();
    }

    @Override
    public default <E2> GraphTraversal<S, E2> addStep(final Step<?, E2> step) {
        return (GraphTraversal) CountTraversal.super.addStep((Step) step);
    }

    ///////////////////// MAP STEPS /////////////////////

    public default <E2> GraphTraversal<S, E2> map(final Function<Traverser<E>, E2> function) {
        final MapStep<E, E2> mapStep = new MapStep<>(this);
        mapStep.setFunction(function);
        return this.addStep(mapStep);
    }

    public default <E2> GraphTraversal<S, E2> flatMap(final Function<Traverser<E>, Iterator<E2>> function) {
        final FlatMapStep<E, E2> flatMapStep = new FlatMapStep<>(this);
        flatMapStep.setFunction(function);
        return this.addStep(flatMapStep);
    }

    public default GraphTraversal<S, Object> id() {
        return this.addStep(new IdStep<>(this));
    }

    public default GraphTraversal<S, String> label() {
        return this.addStep(new LabelStep<>(this));
    }

    public default GraphTraversal<S, E> identity() {
        return this.addStep(new IdentityStep<>(this));
    }

    public default GraphTraversal<S, Vertex> to(final Direction direction, final String... edgeLabels) {
        return this.addStep(new VertexStep<>(this, Vertex.class, direction, edgeLabels));
    }

    public default GraphTraversal<S, Vertex> out(final String... edgeLabels) {
        return this.to(Direction.OUT, edgeLabels);
    }

    public default GraphTraversal<S, Vertex> in(final String... edgeLabels) {
        return this.to(Direction.IN, edgeLabels);
    }

    public default GraphTraversal<S, Vertex> both(final String... edgeLabels) {
        return this.to(Direction.BOTH, edgeLabels);
    }

    public default GraphTraversal<S, Edge> toE(final Direction direction, final String... edgeLabels) {
        return this.addStep(new VertexStep<>(this, Edge.class, direction, edgeLabels));
    }

    public default GraphTraversal<S, Edge> outE(final String... edgeLabels) {
        return this.toE(Direction.OUT, edgeLabels);
    }

    public default GraphTraversal<S, Edge> inE(final String... edgeLabels) {
        return this.toE(Direction.IN, edgeLabels);
    }

    public default GraphTraversal<S, Edge> bothE(final String... edgeLabels) {
        return this.toE(Direction.BOTH, edgeLabels);
    }

    public default GraphTraversal<S, Vertex> toV(final Direction direction) {
        return this.addStep(new EdgeVertexStep(this, direction));
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
        return this.addStep(new EdgeOtherVertexStep(this));
    }

    public default GraphTraversal<S, E> order() {
        return this.order((a, b) -> a.compareTo(b));
    }

    public default GraphTraversal<S, E> order(final Comparator<Traverser<E>>... comparators) {
        return this.addStep(new OrderStep<>(this, comparators));
    }

    public default GraphTraversal<S, E> orderBy(final String key) {
        return this.orderBy(key, Order.incr);
    }

    public default GraphTraversal<S, E> orderBy(final T accessor) {
        return this.orderBy(accessor, Order.incr);
    }

    public default <C> GraphTraversal<S, E> orderBy(final String key, final Comparator<C>... comparators) {
        return this.addStep(new OrderByStep(this, key, comparators));
    }

    public default <C> GraphTraversal<S, E> orderBy(final T accessor, final Comparator<C>... comparators) {
        return this.addStep(new OrderByStep(this, accessor, comparators));
    }

    public default GraphTraversal<S, E> shuffle() {
        return this.addStep(new ShuffleStep<>(this));
    }

    public default <E2> GraphTraversal<S, ? extends Property<E2>> properties(final String... propertyKeys) {
        return this.addStep(new PropertiesStep<>(this, PropertyType.PROPERTY, propertyKeys));
    }

    public default <E2> GraphTraversal<S, E2> values(final String... propertyKeys) {
        return this.addStep(new PropertiesStep<>(this, PropertyType.VALUE, propertyKeys));
    }

    public default <E2> GraphTraversal<S, ? extends Property<E2>> hiddens(final String... propertyKeys) {
        return this.addStep(new PropertiesStep<>(this, PropertyType.HIDDEN_PROPERTY, propertyKeys));
    }

    public default <E2> GraphTraversal<S, E2> hiddenValues(final String... propertyKeys) {
        return this.addStep(new PropertiesStep<>(this, PropertyType.HIDDEN_VALUE, propertyKeys));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> propertyMap(final String... propertyKeys) {
        return this.addStep(new PropertyMapStep<>(this, PropertyType.PROPERTY, propertyKeys));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> valueMap(final String... propertyKeys) {
        return this.addStep(new PropertyMapStep<>(this, PropertyType.VALUE, propertyKeys));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> hiddenMap(final String... propertyKeys) {
        return this.addStep(new PropertyMapStep<>(this, PropertyType.HIDDEN_PROPERTY, propertyKeys));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> hiddenValueMap(final String... propertyKeys) {
        return this.addStep(new PropertyMapStep<>(this, PropertyType.HIDDEN_VALUE, propertyKeys));
    }

    public default GraphTraversal<S, String> key() {
        return this.addStep(new KeyStep(this));
    }

    public default <E2> GraphTraversal<S, E2> value() {
        return this.addStep(new PropertyValueStep<>(this));
    }

    public default GraphTraversal<S, Path> path(final Function... pathFunctions) {
        return this.addStep(new PathStep<>(this, pathFunctions));
    }

    public default <E2> GraphTraversal<S, E2> back(final String stepLabel) {
        return this.addStep(new BackStep<>(this, stepLabel));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> match(final String startLabel, final Traversal... traversals) {
        return (GraphTraversal) this.addStep(new MatchStep<S, Map<String, E2>>(this, startLabel, traversals));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> select(final List<String> labels, Function... stepFunctions) {
        return this.addStep(new SelectStep<>(this, labels, stepFunctions));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> select(final Function... stepFunctions) {
        return this.select(Collections.emptyList(), stepFunctions);
    }

    public default <E2> GraphTraversal<S, E2> select(final String label, Function stepFunction) {
        return this.addStep(new SelectOneStep(this, label, stepFunction));
    }

    public default <E2> GraphTraversal<S, E2> select(final String label) {
        return this.select(label, Function.identity());
    }

    public default <E2> GraphTraversal<S, E2> unfold() {
        return this.addStep(new UnfoldStep<>(this));
    }

    public default GraphTraversal<S, List<E>> fold() {
        return this.addStep(new FoldStep<>(this));
    }

    public default <E2> GraphTraversal<S, E2> fold(final E2 seed, final BiFunction<E2, Traverser<E>, E2> foldFunction) {
        return this.addStep(new FoldStep<>(this, () -> (E2) seed, foldFunction)); // TODO: User should provide supplier
    }

    ///////////////////// FILTER STEPS /////////////////////

    public default GraphTraversal<S, E> filter(final Predicate<Traverser<E>> predicate) {
        final FilterStep<E> filterStep = new FilterStep<>(this);
        filterStep.setPredicate(predicate);
        return this.addStep(filterStep);
    }

    public default GraphTraversal<S, E> inject(final E... injections) {
        return this.addStep(new InjectStep<>(this, injections));
    }

    public default GraphTraversal<S, E> dedup() {
        return this.addStep(new DedupStep<>(this));
    }

    public default GraphTraversal<S, E> dedup(final Function<Traverser<E>, ?> uniqueFunction) {
        return this.addStep(new DedupStep<>(this, uniqueFunction));
    }

    public default GraphTraversal<S, E> except(final String variable) {
        return this.addStep(new ExceptStep<E>(this, variable));
    }

    public default GraphTraversal<S, E> except(final E exceptionObject) {
        return this.addStep(new ExceptStep<>(this, exceptionObject));
    }

    public default GraphTraversal<S, E> except(final Collection<E> exceptionCollection) {
        return this.addStep(new ExceptStep<>(this, exceptionCollection));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> where(final String firstKey, final String secondKey, final BiPredicate predicate) {
        return this.addStep(new WhereStep(this, firstKey, secondKey, predicate));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> where(final String firstKey, final BiPredicate predicate, final String secondKey) {
        return this.where(firstKey, secondKey, predicate);
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> where(final Traversal constraint) {
        return this.addStep(new WhereStep<>(this, constraint));
    }

    public default <E2 extends Element> GraphTraversal<S, E2> has(final String key) {
        return this.addStep(new HasStep<>(this, new HasContainer(key, Contains.within)));
    }

    public default <E2 extends Element> GraphTraversal<S, E2> has(final String key, final Object value) {
        return this.has(key, Compare.eq, value);
    }

    public default <E2 extends Element> GraphTraversal<S, E2> has(final T accessor, final Object value) {
        return this.has(accessor.getAccessor(), value);
    }

    public default <E2 extends Element> GraphTraversal<S, E2> has(final String key, final BiPredicate predicate, final Object value) {
        return this.addStep(new HasStep<>(this, new HasContainer(key, predicate, value)));
    }

    public default <E2 extends Element> GraphTraversal<S, E2> has(final T accessor, final BiPredicate predicate, final Object value) {
        return this.addStep(new HasStep<>(this, new HasContainer(accessor.getAccessor(), predicate, value)));
    }

    public default <E2 extends Element> GraphTraversal<S, E2> has(final String label, final String key, final Object value) {
        return this.has(label, key, Compare.eq, value);
    }

    public default <E2 extends Element> GraphTraversal<S, E2> has(final String label, final String key, final BiPredicate predicate, final Object value) {
        return this.has(T.label, label).addStep(new HasStep<>(this, new HasContainer(key, predicate, value)));
    }

    public default <E2 extends Element> GraphTraversal<S, E2> hasNot(final String key) {
        return this.addStep(new HasStep<>(this, new HasContainer(key, Contains.without)));
    }

    public default <E2 extends Element> GraphTraversal<S, E2> interval(final String key, final Comparable startValue, final Comparable endValue) {
        return this.addStep(new IntervalStep<>(this,
                new HasContainer(key, Compare.gte, startValue),
                new HasContainer(key, Compare.lt, endValue)));
    }

    public default GraphTraversal<S, E> random(final double probability) {
        return this.addStep(new RandomStep<>(this, probability));
    }

    public default GraphTraversal<S, E> range(final long low, final long high) {
        return this.addStep(new RangeStep<>(this, low, high));
    }

    public default GraphTraversal<S, E> limit(final long limit) {
        return this.range(0, limit);
    }

    public default <E2 extends Element> GraphTraversal<S, E2> localRange(final int low, final int high) {
        return this.addStep(new LocalRangeStep<>(this, low, high));
    }

    public default <E2 extends Element> GraphTraversal<S, E2> localLimit(final int limit) {
        return this.localRange(0, limit);
    }

    public default GraphTraversal<S, E> retain(final String variable) {
        return this.addStep(new RetainStep<>(this, variable));
    }

    public default GraphTraversal<S, E> retain(final E retainObject) {
        return this.addStep(new RetainStep<>(this, retainObject));
    }

    public default GraphTraversal<S, E> retain(final Collection<E> retainCollection) {
        return this.addStep(new RetainStep<>(this, retainCollection));
    }

    public default GraphTraversal<S, E> simplePath() {
        return this.addStep(new SimplePathStep<>(this));
    }

    public default GraphTraversal<S, E> cyclicPath() {
        return this.addStep(new CyclicPathStep<E>(this));
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public default GraphTraversal<S, E> sideEffect(final Consumer<Traverser<E>> consumer) {
        final SideEffectStep<E> sideEffectStep = new SideEffectStep<>(this);
        sideEffectStep.setConsumer(consumer);
        return this.addStep(sideEffectStep);
    }

    public default <E2> GraphTraversal<S, E2> cap(final String sideEffecyKey) {
        return this.addStep(new SideEffectCapStep<>(this, sideEffecyKey));
    }

    public default <E2> GraphTraversal<S, E2> cap() {
        return this.cap(((SideEffectCapable) TraversalHelper.getEnd(this)).getSideEffectKey());
    }

    public default GraphTraversal<S, Long> count() {
        return this.addStep(new CountStep<>(this));
    }

    public default GraphTraversal<S, E> subgraph(final String sideEffectKey, final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final Predicate<Edge> includeEdge) {
        return this.addStep(new SubgraphStep<>(this, sideEffectKey, edgeIdHolder, vertexMap, includeEdge));
    }

    public default GraphTraversal<S, E> subgraph(final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final Predicate<Edge> includeEdge) {
        return this.subgraph(null, edgeIdHolder, vertexMap, includeEdge);
    }

    public default GraphTraversal<S, E> subgraph(final String sideEffectKey, final Predicate<Edge> includeEdge) {
        return this.subgraph(sideEffectKey, null, null, includeEdge);
    }

    public default GraphTraversal<S, E> subgraph(final Predicate<Edge> includeEdge) {
        return this.subgraph(null, null, null, includeEdge);
    }

    public default GraphTraversal<S, E> aggregate(final String sideEffectKey, final Function<Traverser<E>, ?> preAggregateFunction) {
        return this.addStep(new AggregateStep<>(this, sideEffectKey, preAggregateFunction));
    }

    public default GraphTraversal<S, E> aggregate(final Function<Traverser<E>, ?> preAggregateFunction) {
        return this.aggregate(null, preAggregateFunction);
    }

    public default GraphTraversal<S, E> aggregate() {
        return this.aggregate(null, null);
    }

    public default GraphTraversal<S, E> aggregate(final String sideEffectKey) {
        return this.aggregate(sideEffectKey, null);
    }

    public default GraphTraversal<S, E> groupBy(final String sideEffectKey, final Function<Traverser<E>, ?> keyFunction, final Function<Traverser<E>, ?> valueFunction, final Function<Collection, ?> reduceFunction) {
        return this.addStep(new GroupByStep(this, sideEffectKey, keyFunction, valueFunction, reduceFunction));
    }

    public default GraphTraversal<S, E> groupBy(final Function<Traverser<E>, ?> keyFunction, final Function<Traverser<E>, ?> valueFunction, final Function<Collection, ?> reduceFunction) {
        return this.groupBy(null, keyFunction, valueFunction, reduceFunction);
    }

    public default GraphTraversal<S, E> groupBy(final Function<Traverser<E>, ?> keyFunction, final Function<Traverser<E>, ?> valueFunction) {
        return this.groupBy(null, keyFunction, valueFunction, null);
    }

    public default GraphTraversal<S, E> groupBy(final Function<Traverser<E>, ?> keyFunction) {
        return this.groupBy(null, keyFunction, null, null);
    }

    public default GraphTraversal<S, E> groupBy(final String sideEffectKey, final Function<Traverser<E>, ?> keyFunction) {
        return this.groupBy(sideEffectKey, keyFunction, null, null);
    }

    public default GraphTraversal<S, E> groupBy(final String sideEffectKey, final Function<Traverser<E>, ?> keyFunction, final Function<Traverser<E>, ?> valueFunction) {
        return this.groupBy(sideEffectKey, keyFunction, valueFunction, null);
    }

    public default GraphTraversal<S, E> groupCount(final String sideEffectKey, final Function<Traverser<E>, ?> preGroupFunction) {
        return this.addStep(new GroupCountStep<>(this, sideEffectKey, preGroupFunction));
    }

    public default GraphTraversal<S, E> groupCount(final Function<Traverser<E>, ?> preGroupFunction) {
        return this.groupCount(null, preGroupFunction);
    }

    public default GraphTraversal<S, E> groupCount(final String sideEffectKey) {
        return this.groupCount(sideEffectKey, null);
    }

    public default GraphTraversal<S, E> groupCount() {
        return this.groupCount(null, null);
    }

    public default GraphTraversal<S, Vertex> addE(final Direction direction, final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.addStep(new AddEdgeStep(this, direction, edgeLabel, stepLabel, propertyKeyValues));
    }

    public default GraphTraversal<S, Vertex> addInE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.addE(Direction.IN, edgeLabel, stepLabel, propertyKeyValues);
    }

    public default GraphTraversal<S, Vertex> addOutE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.addE(Direction.OUT, edgeLabel, stepLabel, propertyKeyValues);
    }

    public default GraphTraversal<S, Vertex> addBothE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.addE(Direction.BOTH, edgeLabel, stepLabel, propertyKeyValues);
    }

    public default GraphTraversal<S, E> timeLimit(final long timeLimit) {
        return this.addStep(new TimeLimitStep<E>(this, timeLimit));
    }

    public default GraphTraversal<S, E> tree(final String sideEffectKey, final Function... branchFunctions) {
        return this.addStep(new TreeStep<>(this, sideEffectKey, branchFunctions));
    }

    public default GraphTraversal<S, E> tree(final Function... branchFunctions) {
        return this.tree(null, branchFunctions);
    }

    public default GraphTraversal<S, E> store(final String sideEffectKey, final Function<Traverser<E>, ?> preStoreFunction) {
        return this.addStep(new StoreStep<>(this, sideEffectKey, preStoreFunction));
    }

    public default GraphTraversal<S, E> store(final String sideEffectKey) {
        return this.store(sideEffectKey, null);
    }

    public default GraphTraversal<S, E> store(final Function<Traverser<E>, ?> preStoreFunction) {
        return this.store(null, preStoreFunction);
    }

    public default GraphTraversal<S, E> store() {
        return this.store(null, null);
    }

    ///////////////////// BRANCH STEPS /////////////////////

    /*public default GraphTraversal<S, E> branch(final Function<Traverser<E>, String>... labelFunctions) {
        final BranchStep<E> branchStep = new BranchStep<>(this);
        branchStep.setFunctions(labelFunctions);
        return this.addStep(branchStep);
    }*/

    public default GraphTraversal<S, E> jump(final String jumpLabel, final Predicate<Traverser<E>> jumpPredicate, final Predicate<Traverser<E>> emitPredicate) {
        return this.addStep(JumpStep.<E>build(this).jumpLabel(jumpLabel).jumpPredicate(jumpPredicate).emitPredicate(emitPredicate).create());
    }

    public default GraphTraversal<S, E> jump(final String jumpLabel, final Predicate<Traverser<E>> jumpPredicate) {
        return this.addStep(JumpStep.<E>build(this).jumpLabel(jumpLabel).jumpPredicate(jumpPredicate).emitChoice(false).create());
    }

    public default GraphTraversal<S, E> jump(final String jumpLabel, final int loops, final Predicate<Traverser<E>> emitPredicate) {
        return this.addStep(JumpStep.<E>build(this).jumpLabel(jumpLabel).jumpLoops(loops, Compare.lt).emitPredicate(emitPredicate).create());
    }

    public default GraphTraversal<S, E> jump(final String jumpLabel, final int loops) {
        return this.addStep(JumpStep.<E>build(this).jumpLabel(jumpLabel).jumpLoops(loops, Compare.lt).emitChoice(false).create());
    }

    public default GraphTraversal<S, E> jump(final String jumpLabel) {
        return this.addStep(JumpStep.<E>build(this).jumpLabel(jumpLabel).jumpChoice(true).emitChoice(false).create());
    }

    public default GraphTraversal<S, E> until(final String breakLabel, final Predicate<Traverser<E>> breakPredicate, final Predicate<Traverser<E>> emitPredicate) {
        return this.addStep(new UntilStep<>(this, breakLabel, breakPredicate, emitPredicate));
    }

    public default GraphTraversal<S, E> until(final String breakLabel, final Predicate<Traverser<E>> breakPredicate) {
        return this.addStep(new UntilStep<>(this, breakLabel, breakPredicate, null));
    }

    public default GraphTraversal<S, E> until(final String breakLabel, final int loops, final Predicate<Traverser<E>> emitPredicate) {
        return this.addStep(new UntilStep<>(this, breakLabel, loops, emitPredicate));
    }

    public default GraphTraversal<S, E> until(final String breakLabel, final int loops) {
        return this.addStep(new UntilStep<>(this, breakLabel, loops, null));
    }

    public default <E2> GraphTraversal<S, E2> choose(final Predicate<Traverser<E>> choosePredicate, final Traversal<E, E2> trueChoice, final Traversal<E, E2> falseChoice) {
        return this.addStep(new ChooseStep<E, E2, Boolean>(this, choosePredicate, trueChoice, falseChoice));
    }

    public default <M, E2> GraphTraversal<S, E2> choose(final Function<Traverser<E>, M> mapFunction, final Map<M, Traversal<E, E2>> choices) {
        return this.addStep(new ChooseStep<>(this, mapFunction, choices));
    }

    public default <E2> GraphTraversal<S, E2> union(final Traversal<E, E2>... traversals) {
        return this.addStep(new UnionStep<>(this, traversals));
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public default GraphTraversal<S, E> with(final String key, final Supplier supplier) {
        this.sideEffects().registerSupplier(key, supplier);
        return this;
    }

    public default GraphTraversal<S, E> trackPaths() {
        return this.addStep(new PathIdentityStep<>(this));
    }

    public default GraphTraversal<S, E> as(final String label) {
        TraversalHelper.verifyStepLabelIsNotAlreadyAStepLabel(label, this);
        TraversalHelper.verifyStepLabelIsNotASideEffectKey(label, this);
        TraversalHelper.getEnd(this).setLabel(label);
        return this;
    }

    public default GraphTraversal<S, E> profile() {
        return this.addStep(new ProfileStep<>(this));
    }

    @Override
    public default void remove() {
        try {
            this.getStrategies().apply(TraversalEngine.STANDARD);
            final Step<?, E> endStep = TraversalHelper.getEnd(this);
            while (true) {
                final Object object = endStep.next().get();
                if (object instanceof Element)
                    ((Element) object).remove();
                else if (object instanceof Property)
                    ((Property) object).remove();
                else {
                    throw new IllegalStateException("The following object does not have a remove() method: " + object);
                }
            }
        } catch (final NoSuchElementException ignored) {

        }
    }

    /////////////////////////////////////

    /*
    // TODO: Will add as we flush out for GA
    public default GraphTraversal<S, Pair<Vertex, Double>> pageRank() {
        return (GraphTraversal) this.addStep(new PageRankStep(this));
    }

    public default GraphTraversal<S, Pair<Vertex, Double>> pageRank(final Supplier<Traversal> incidentTraversal) {
        return (GraphTraversal) this.addStep(new PageRankStep(this, (Supplier) incidentTraversal));
    }
    */
}
