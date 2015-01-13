package com.tinkerpop.gremlin.process.graph;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.marker.ComparatorHolder;
import com.tinkerpop.gremlin.process.graph.marker.FunctionHolder;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.branch.BranchStep;
import com.tinkerpop.gremlin.process.graph.step.branch.ChooseStep;
import com.tinkerpop.gremlin.process.graph.step.branch.RepeatStep;
import com.tinkerpop.gremlin.process.graph.step.branch.UnionStep;
import com.tinkerpop.gremlin.process.graph.step.filter.CoinStep;
import com.tinkerpop.gremlin.process.graph.step.filter.CyclicPathStep;
import com.tinkerpop.gremlin.process.graph.step.filter.DedupStep;
import com.tinkerpop.gremlin.process.graph.step.filter.ExceptStep;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.step.filter.RangeStep;
import com.tinkerpop.gremlin.process.graph.step.filter.RetainStep;
import com.tinkerpop.gremlin.process.graph.step.filter.SampleStep;
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
import com.tinkerpop.gremlin.process.graph.step.map.LocalStep;
import com.tinkerpop.gremlin.process.graph.step.map.MapStep;
import com.tinkerpop.gremlin.process.graph.step.map.OrderStep;
import com.tinkerpop.gremlin.process.graph.step.map.PathStep;
import com.tinkerpop.gremlin.process.graph.step.map.PropertiesStep;
import com.tinkerpop.gremlin.process.graph.step.map.PropertyMapStep;
import com.tinkerpop.gremlin.process.graph.step.map.PropertyValueStep;
import com.tinkerpop.gremlin.process.graph.step.map.SackStep;
import com.tinkerpop.gremlin.process.graph.step.map.SelectOneStep;
import com.tinkerpop.gremlin.process.graph.step.map.SelectStep;
import com.tinkerpop.gremlin.process.graph.step.map.ShuffleStep;
import com.tinkerpop.gremlin.process.graph.step.map.UnfoldStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.step.map.match.MatchStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AddEdgeStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AggregateStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupCountStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.InjectStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SackElementValueStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SackObjectStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StoreStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SubgraphStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SumStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.TreeStep;
import com.tinkerpop.gremlin.process.graph.step.util.PathIdentityStep;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.util.ElementFunctionComparator;
import com.tinkerpop.gremlin.process.util.ElementValueComparator;
import com.tinkerpop.gremlin.process.util.ElementValueFunction;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Contains;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.PropertyType;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GraphTraversal<S, E> extends Traversal<S, E> {

    @Override
    public default GraphTraversal<S, E> submit(final GraphComputer computer) {
        return (GraphTraversal) Traversal.super.<S, E>submit(computer);
    }

    @Override
    public GraphTraversal.Admin<S, E> asAdmin();

    public interface Admin<S, E> extends Traversal.Admin<S, E> {

        @Override
        public default <E2> GraphTraversal<S, E2> addStep(final Step<?, E2> step) {
            return (GraphTraversal) Traversal.Admin.super.addStep((Step) step);
        }
    }

    ///////////////////// MAP STEPS /////////////////////

    public default <E2> GraphTraversal<S, E2> map(final Function<Traverser<E>, E2> function) {
        final MapStep<E, E2> mapStep = new MapStep<>(this);
        mapStep.setFunction(function);
        return this.asAdmin().addStep(mapStep);
    }

    public default <E2> GraphTraversal<S, E2> flatMap(final Function<Traverser<E>, Iterator<E2>> function) {
        final FlatMapStep<E, E2> flatMapStep = new FlatMapStep<>(this);
        flatMapStep.setFunction(function);
        return this.asAdmin().addStep(flatMapStep);
    }

    public default GraphTraversal<S, Object> id() {
        return this.asAdmin().addStep(new IdStep<>(this));
    }

    public default GraphTraversal<S, String> label() {
        return this.asAdmin().addStep(new LabelStep<>(this));
    }

    public default GraphTraversal<S, E> identity() {
        return this.asAdmin().addStep(new IdentityStep<>(this));
    }

    public default GraphTraversal<S, Vertex> to(final Direction direction, final String... edgeLabels) {
        return this.asAdmin().addStep(new VertexStep<>(this, Vertex.class, direction, edgeLabels));
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
        return this.asAdmin().addStep(new VertexStep<>(this, Edge.class, direction, edgeLabels));
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
        return this.asAdmin().addStep(new EdgeVertexStep(this, direction));
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
        return this.asAdmin().addStep(new EdgeOtherVertexStep(this));
    }

    public default GraphTraversal<S, E> order() {
        return this.asAdmin().addStep(new OrderStep<>(this));
    }

    public default GraphTraversal<S, E> shuffle() {
        return this.asAdmin().addStep(new ShuffleStep<>(this));
    }

    public default <E2> GraphTraversal<S, ? extends Property<E2>> properties(final String... propertyKeys) {
        return this.asAdmin().addStep(new PropertiesStep<>(this, PropertyType.PROPERTY, propertyKeys));
    }

    public default <E2> GraphTraversal<S, E2> values(final String... propertyKeys) {
        return this.asAdmin().addStep(new PropertiesStep<>(this, PropertyType.VALUE, propertyKeys));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> propertyMap(final String... propertyKeys) {
        return this.asAdmin().addStep(new PropertyMapStep<>(this, false, PropertyType.PROPERTY, propertyKeys));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> valueMap(final String... propertyKeys) {
        return this.asAdmin().addStep(new PropertyMapStep<>(this, false, PropertyType.VALUE, propertyKeys));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> valueMap(final boolean includeTokens, final String... propertyKeys) {
        return this.asAdmin().addStep(new PropertyMapStep<>(this, includeTokens, PropertyType.VALUE, propertyKeys));
    }

    public default GraphTraversal<S, String> key() {
        return this.asAdmin().addStep(new KeyStep(this));
    }

    public default <E2> GraphTraversal<S, E2> value() {
        return this.asAdmin().addStep(new PropertyValueStep<>(this));
    }

    public default GraphTraversal<S, Path> path() {
        return this.asAdmin().addStep(new PathStep<>(this));
    }

    public default <E2> GraphTraversal<S, E2> back(final String stepLabel) {
        return this.asAdmin().addStep(new BackStep<>(this, stepLabel));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> match(final String startLabel, final Traversal... traversals) {
        return (GraphTraversal) this.asAdmin().addStep(new MatchStep<S, Map<String, E2>>(this, startLabel, traversals));
    }

    public default <E2> GraphTraversal<S, E2> sack() {
        return this.asAdmin().addStep(new SackStep<>(this));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> select(final String... stepLabels) {
        return this.asAdmin().addStep(new SelectStep<>(this, stepLabels));
    }

    public default <E2> GraphTraversal<S, E2> select(final String stepLabel) {
        return this.asAdmin().addStep(new SelectOneStep(this, stepLabel));
    }

    public default <E2> GraphTraversal<S, E2> unfold() {
        return this.asAdmin().addStep(new UnfoldStep<>(this));
    }

    public default GraphTraversal<S, List<E>> fold() {
        return this.asAdmin().addStep(new FoldStep<>(this));
    }

    public default <E2> GraphTraversal<S, E2> fold(final E2 seed, final BiFunction<E2, E, E2> foldFunction) {
        return this.asAdmin().addStep(new FoldStep<>(this, () -> seed, foldFunction)); // TODO: User should provide supplier
    }

    public default <E2> GraphTraversal<S, E2> local(final Traversal<?, E2> localTraversal) {
        return this.asAdmin().addStep(new LocalStep<>(this, localTraversal));
    }

    ///////////////////// FILTER STEPS /////////////////////

    public default GraphTraversal<S, E> filter(final Predicate<Traverser<E>> predicate) {
        final FilterStep<E> filterStep = new FilterStep<>(this);
        filterStep.setPredicate(predicate);
        return this.asAdmin().addStep(filterStep);
    }

    public default GraphTraversal<S, E> inject(final E... injections) {
        return this.asAdmin().addStep(new InjectStep<>(this, injections));
    }

    public default GraphTraversal<S, E> dedup() {
        return this.asAdmin().addStep(new DedupStep<>(this));
    }

    public default GraphTraversal<S, E> except(final String sideEffectKey) {
        return this.asAdmin().addStep(new ExceptStep<E>(this, sideEffectKey));
    }

    public default GraphTraversal<S, E> except(final E exceptionObject) {
        return this.asAdmin().addStep(new ExceptStep<>(this, exceptionObject));
    }

    public default GraphTraversal<S, E> except(final Collection<E> exceptionCollection) {
        return this.asAdmin().addStep(new ExceptStep<>(this, exceptionCollection));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> where(final String firstKey, final String secondKey, final BiPredicate predicate) {
        return this.asAdmin().addStep(new WhereStep(this, firstKey, secondKey, predicate));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> where(final String firstKey, final BiPredicate predicate, final String secondKey) {
        return this.where(firstKey, secondKey, predicate);
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> where(final Traversal constraint) {
        return this.asAdmin().addStep(new WhereStep<>(this, constraint));
    }

    public default <E2 extends Element> GraphTraversal<S, E2> has(final String key) {
        return this.asAdmin().addStep(new HasStep<>(this, new HasContainer(key, Contains.within)));
    }

    public default <E2 extends Element> GraphTraversal<S, E2> has(final String key, final Object value) {
        return this.has(key, Compare.eq, value);
    }

    public default <E2 extends Element> GraphTraversal<S, E2> has(final T accessor, final Object value) {
        return this.has(accessor.getAccessor(), value);
    }

    public default <E2 extends Element> GraphTraversal<S, E2> has(final String key, final BiPredicate predicate, final Object value) {
        return this.asAdmin().addStep(new HasStep<>(this, new HasContainer(key, predicate, value)));
    }

    public default <E2 extends Element> GraphTraversal<S, E2> has(final T accessor, final BiPredicate predicate, final Object value) {
        return this.asAdmin().addStep(new HasStep<>(this, new HasContainer(accessor.getAccessor(), predicate, value)));
    }

    public default <E2 extends Element> GraphTraversal<S, E2> has(final String label, final String key, final Object value) {
        return this.has(label, key, Compare.eq, value);
    }

    public default <E2 extends Element> GraphTraversal<S, E2> has(final String label, final String key, final BiPredicate predicate, final Object value) {
        return this.has(T.label, label).asAdmin().addStep(new HasStep<>(this, new HasContainer(key, predicate, value)));
    }

    public default <E2 extends Element> GraphTraversal<S, E2> hasNot(final String key) {
        return this.asAdmin().addStep(new HasStep<>(this, new HasContainer(key, Contains.without)));
    }

    public default <E2 extends Element> GraphTraversal<S, E2> between(final String key, final Comparable startValue, final Comparable endValue) {
        return this.has(key, Compare.gte, startValue).has(key, Compare.lt, endValue);
    }

    public default GraphTraversal<S, E> coin(final double probability) {
        return this.asAdmin().addStep(new CoinStep<>(this, probability));
    }

    public default GraphTraversal<S, E> range(final long low, final long high) {
        return this.asAdmin().addStep(new RangeStep<>(this, low, high));
    }

    public default GraphTraversal<S, E> limit(final long limit) {
        return this.range(0, limit);
    }

    public default GraphTraversal<S, E> retain(final String sideEffectKey) {
        return this.asAdmin().addStep(new RetainStep<>(this, sideEffectKey));
    }

    public default GraphTraversal<S, E> retain(final E retainObject) {
        return this.asAdmin().addStep(new RetainStep<>(this, retainObject));
    }

    public default GraphTraversal<S, E> retain(final Collection<E> retainCollection) {
        return this.asAdmin().addStep(new RetainStep<>(this, retainCollection));
    }

    public default GraphTraversal<S, E> simplePath() {
        return this.asAdmin().addStep(new SimplePathStep<>(this));
    }

    public default GraphTraversal<S, E> cyclicPath() {
        return this.asAdmin().addStep(new CyclicPathStep<>(this));
    }

    public default GraphTraversal<S, E> sample(final int amountToSample) {
        return this.asAdmin().addStep(new SampleStep<>(this, amountToSample));
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public default GraphTraversal<S, E> sideEffect(final Consumer<Traverser<E>> consumer) {
        final SideEffectStep<E> sideEffectStep = new SideEffectStep<>(this);
        sideEffectStep.setConsumer(consumer);
        return this.asAdmin().addStep(sideEffectStep);
    }

    public default <E2> GraphTraversal<S, E2> cap(final String sideEffectKey) {
        return this.asAdmin().addStep(new SideEffectCapStep<>(this, sideEffectKey));
    }

    public default <E2> GraphTraversal<S, E2> cap() {
        return this.cap(((SideEffectCapable) TraversalHelper.getEnd(this)).getSideEffectKey());
    }

    public default GraphTraversal<S, Long> count() {
        return this.asAdmin().addStep(new CountStep<>(this));
    }

    public default GraphTraversal<S, Double> sum() {
        return this.asAdmin().addStep(new SumStep(this));
    }

    public default GraphTraversal<S, E> subgraph(final String sideEffectKey, final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final Predicate<Edge> includeEdge) {
        return this.asAdmin().addStep(new SubgraphStep<>(this, sideEffectKey, edgeIdHolder, vertexMap, includeEdge));
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

    public default GraphTraversal<S, E> aggregate(final String sideEffectKey) {
        return this.asAdmin().addStep(new AggregateStep<>(this, sideEffectKey));
    }

    public default GraphTraversal<S, E> aggregate() {
        return this.aggregate(null);
    }

    public default GraphTraversal<S, E> group(final String sideEffectKey) {
        return this.asAdmin().addStep(new GroupStep<>(this, sideEffectKey));
    }

    public default GraphTraversal<S, E> group() {
        return this.group(null);
    }

    public default GraphTraversal<S, E> groupCount(final String sideEffectKey) {
        return this.asAdmin().addStep(new GroupCountStep<>(this, sideEffectKey));
    }

    public default GraphTraversal<S, E> groupCount() {
        return this.groupCount(null);
    }

    public default GraphTraversal<S, Vertex> addE(final Direction direction, final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.asAdmin().addStep(new AddEdgeStep(this, direction, edgeLabel, stepLabel, propertyKeyValues));
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
        return this.asAdmin().addStep(new TimeLimitStep<E>(this, timeLimit));
    }

    public default GraphTraversal<S, E> tree(final String sideEffectKey) {
        return this.asAdmin().addStep(new TreeStep<>(this, sideEffectKey));
    }

    public default GraphTraversal<S, E> tree() {
        return this.tree(null);
    }

    public default <V> GraphTraversal<S, E> sack(final BiFunction<V, E, V> sackFunction) {
        return this.asAdmin().addStep(new SackObjectStep<>(this, sackFunction));
    }

    public default <E2 extends Element, V> GraphTraversal<S, E2> sack(final BinaryOperator<V> sackOperator, final String elementPropertyKey) {
        return this.asAdmin().addStep(new SackElementValueStep<>(this, sackOperator, elementPropertyKey));
    }

    public default GraphTraversal<S, E> store(final String sideEffectKey) {
        return this.asAdmin().addStep(new StoreStep<>(this, sideEffectKey));
    }

    public default GraphTraversal<S, E> store() {
        return this.store(null);
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public default GraphTraversal<S, E> branch(final Function<Traverser<E>, Collection<String>> function) {
        final BranchStep<E> branchStep = new BranchStep<>(this);
        branchStep.setFunction(function);
        return this.asAdmin().addStep(branchStep);
    }

    public default <E2> GraphTraversal<S, E2> choose(final Predicate<E> choosePredicate, final Traversal<?, E2> trueChoice, final Traversal<?, E2> falseChoice) {
        return this.asAdmin().addStep(new ChooseStep<E, E2, Boolean>(this, choosePredicate, (Traversal<E, E2>) trueChoice, (Traversal<E, E2>) falseChoice));
    }

    public default <M, E2> GraphTraversal<S, E2> choose(final Function<E, M> mapFunction, final Map<M, Traversal<?, E2>> choices) {
        return this.asAdmin().addStep(new ChooseStep(this, mapFunction, (Map) choices));
    }

    public default <E2> GraphTraversal<S, E2> union(final Traversal<?, E2>... traversals) {
        return this.asAdmin().addStep(new UnionStep<>(this, (Traversal<E, E2>[]) traversals));
    }

    public default GraphTraversal<S, E> repeat(final Traversal<?, E> repeatTraversal) {
        return RepeatStep.addRepeatToTraversal(this, (Traversal<E, E>) repeatTraversal);
    }

    public default GraphTraversal<S, E> emit(final Predicate<Traverser<E>> emitPredicate) {
        return RepeatStep.addEmitToTraversal(this, emitPredicate);
    }

    public default GraphTraversal<S, E> emit(final Traversal<?, ?> emitTraversal) {
        return this.emit(new RepeatStep.TraversalPredicate<>((Traversal<E, ?>) emitTraversal));
    }

    public default GraphTraversal<S, E> emit() {
        return this.emit(t -> true);
    }

    public default GraphTraversal<S, E> until(final Predicate<Traverser<E>> untilPredicate) {
        return RepeatStep.addUntilToTraversal(this, untilPredicate);
    }

    public default GraphTraversal<S, E> until(final Traversal<?, ?> untilTraversal) {
        return this.until(new RepeatStep.TraversalPredicate<>((Traversal<E, ?>) untilTraversal));
    }

    public default GraphTraversal<S, E> times(final int maxLoops) {
        return this.until(new RepeatStep.LoopPredicate<>(maxLoops));
    }


    ///////////////////// UTILITY STEPS /////////////////////

    public default GraphTraversal<S, E> withSideEffect(final String key, final Supplier supplier) {
        this.asAdmin().getSideEffects().registerSupplier(key, supplier);
        return this;
    }

    public default <A> GraphTraversal<S, E> withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator) {
        this.asAdmin().getSideEffects().setSack(initialValue, Optional.of(splitOperator));
        return this;
    }

    public default <A> GraphTraversal<S, E> withSack(final Supplier<A> initialValue) {
        this.asAdmin().getSideEffects().setSack(initialValue, Optional.empty());
        return this;
    }

    public default GraphTraversal<S, E> withPath() {
        return this.asAdmin().addStep(new PathIdentityStep<>(this));
    }

    public default GraphTraversal<S, E> as(final String label) {
        TraversalHelper.verifyStepLabelIsNotHidden(label);
        TraversalHelper.verifyStepLabelIsNotAlreadyAStepLabel(label, this);
        TraversalHelper.verifyStepLabelIsNotASideEffectKey(label, this);
        if (this.asAdmin().getSteps().size() == 0) this.asAdmin().addStep(new StartStep<>(this));
        TraversalHelper.getEnd(this).setLabel(label);
        return this;
    }

    public default GraphTraversal<S, E> by() {
        ((FunctionHolder) TraversalHelper.getEnd(this)).addFunction(Function.identity());
        return this;
    }

    public default <V> GraphTraversal<S, E> by(final Function<V, Object> functionProjection) {
        ((FunctionHolder<V, Object>) TraversalHelper.getEnd(this)).addFunction(functionProjection);
        return this;
    }

    public default GraphTraversal<S, E> by(final T tokenProjection) {
        ((FunctionHolder<Element, Object>) TraversalHelper.getEnd(this)).addFunction(tokenProjection);
        return this;
    }

    public default GraphTraversal<S, E> by(final String elementPropertyProjection) {
        ((FunctionHolder<Element, ?>) TraversalHelper.getEnd(this)).addFunction(new ElementValueFunction<>(elementPropertyProjection));
        return this;
    }

    ////

    public default GraphTraversal<S, E> by(final Comparator<E> comparator) {
        ((ComparatorHolder<E>) TraversalHelper.getEnd(this)).addComparator(comparator);
        return this;
    }

    public default <V> GraphTraversal<S, E> by(final Function<Element, V> elementFunctionProjection, final Comparator<V> elementFunctionValueComparator) {
        ((ComparatorHolder<Element>) TraversalHelper.getEnd(this)).addComparator(new ElementFunctionComparator<>(elementFunctionProjection, elementFunctionValueComparator));
        return this;
    }

    public default <V> GraphTraversal<S, E> by(final T tokenProjection, final Comparator<V> tokenValueComparator) {
        ((ComparatorHolder<Element>) TraversalHelper.getEnd(this)).addComparator(new ElementFunctionComparator<>(tokenProjection, (Comparator) tokenValueComparator));
        return this;
    }

    public default <V> GraphTraversal<S, E> by(final String elementPropertyProjection, final Comparator<V> propertyValueComparator) {
        ((ComparatorHolder<Element>) TraversalHelper.getEnd(this)).addComparator(new ElementValueComparator<>(elementPropertyProjection, propertyValueComparator));
        return this;
    }

    public default GraphTraversal<S, E> profile() {
        return this.asAdmin().addStep(new ProfileStep<>(this));
    }

    @Override
    public default void remove() {
        try {
            this.asAdmin().applyStrategies(TraversalEngine.STANDARD);
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
        return (GraphTraversal) this.asAdmin().addStep(new PageRankStep(this));
    }

    public default GraphTraversal<S, Pair<Vertex, Double>> pageRank(final Supplier<Traversal> incidentTraversal) {
        return (GraphTraversal) this.asAdmin().addStep(new PageRankStep(this, (Supplier) incidentTraversal));
    }
    */
}
