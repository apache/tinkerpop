package com.tinkerpop.gremlin.process.graph.traversal;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Scope;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.ComputerResult;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import com.tinkerpop.gremlin.process.graph.traversal.lambda.LoopTraversal;
import com.tinkerpop.gremlin.process.graph.traversal.step.ComparatorHolder;
import com.tinkerpop.gremlin.process.graph.traversal.step.TraversalOptionParent;
import com.tinkerpop.gremlin.process.graph.traversal.step.branch.BranchStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.branch.ChooseStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.branch.LocalStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.branch.RepeatStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.branch.UnionStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.AndStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.CoinStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.CyclicPathStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.DedupStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.ExceptStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.HasTraversalStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.IsStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.OrStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.RangeStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.RetainStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.SampleStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.SimplePathStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.TimeLimitStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.WhereStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.BackStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.CountStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.EdgeOtherVertexStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.FlatMapStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.FoldStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.IdStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.KeyStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.LabelStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.MapStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.MaxStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.MeanStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.MinStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.OrderGlobalStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.OrderLocalStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.PathStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.PropertiesStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.PropertyMapStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.PropertyValueStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.SackStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.SelectOneStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.SelectStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.SumStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.UnfoldStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.match.MatchStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.AddEdgeStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.AggregateStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GroupCountStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GroupStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.InjectStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.ProfileStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.SackElementValueStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.SackObjectStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.SideEffectStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.StoreStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.SubgraphStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.TreeStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.util.CollectingBarrierStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.util.PathIdentityStep;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import com.tinkerpop.gremlin.process.traversal.lambda.FilterTraversal;
import com.tinkerpop.gremlin.process.traversal.lambda.FilterTraverserTraversal;
import com.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import com.tinkerpop.gremlin.process.traversal.lambda.MapTraversal;
import com.tinkerpop.gremlin.process.traversal.lambda.MapTraverserTraversal;
import com.tinkerpop.gremlin.process.traversal.lambda.TrueTraversal;
import com.tinkerpop.gremlin.process.traversal.step.ElementFunctionComparator;
import com.tinkerpop.gremlin.process.traversal.step.ElementValueComparator;
import com.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Contains;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Order;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.PropertyType;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
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
        try {
            final TraversalVertexProgram vertexProgram = TraversalVertexProgram.build().traversal(this::asAdmin).create();
            final ComputerResult result = computer.program(vertexProgram).submit().get();
            final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(result.graph().getClass());
            return traversal.addStep(new ComputerResultStep<>(traversal, result, vertexProgram, true));
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public default GraphTraversal.Admin<S, E> asAdmin() {
        return (GraphTraversal.Admin<S, E>) this;
    }

    public interface Admin<S, E> extends Traversal.Admin<S, E>, GraphTraversal<S, E> {

        @Override
        public default <E2> GraphTraversal.Admin<S, E2> addStep(final Step<?, E2> step) {
            return (GraphTraversal.Admin) Traversal.Admin.super.addStep((Step) step);
        }

        @Override
        public default GraphTraversal<S, E> iterate() {
            return GraphTraversal.super.iterate();
        }
    }

    ///////////////////// MAP STEPS /////////////////////

    public default <E2> GraphTraversal<S, E2> map(final Function<Traverser<E>, E2> function) {
        final MapStep<E, E2> mapStep = new MapStep<>(this.asAdmin());
        mapStep.setFunction(function);
        return this.asAdmin().addStep(mapStep);
    }

    public default <E2> GraphTraversal<S, E2> flatMap(final Function<Traverser<E>, Iterator<E2>> function) {
        final FlatMapStep<E, E2> flatMapStep = new FlatMapStep<>(this.asAdmin());
        flatMapStep.setFunction(function);
        return this.asAdmin().addStep(flatMapStep);
    }

    public default GraphTraversal<S, Object> id() {
        return this.asAdmin().addStep(new IdStep<>(this.asAdmin()));
    }

    public default GraphTraversal<S, String> label() {
        return this.asAdmin().addStep(new LabelStep<>(this.asAdmin()));
    }

    public default GraphTraversal<S, E> identity() {
        return this.asAdmin().addStep(new IdentityStep<>(this.asAdmin()));
    }

    public default GraphTraversal<S, Vertex> to(final Direction direction, final String... edgeLabels) {
        return this.asAdmin().addStep(new VertexStep<>(this.asAdmin(), Vertex.class, direction, edgeLabels));
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
        return this.asAdmin().addStep(new VertexStep<>(this.asAdmin(), Edge.class, direction, edgeLabels));
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
        return this.asAdmin().addStep(new EdgeVertexStep(this.asAdmin(), direction));
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
        return this.asAdmin().addStep(new EdgeOtherVertexStep(this.asAdmin()));
    }

    public default GraphTraversal<S, E> order() {
        return this.order(Scope.global);
    }

    public default GraphTraversal<S, E> order(final Scope scope) {
        return scope.equals(Scope.local) ? this.asAdmin().addStep(new OrderLocalStep<>(this.asAdmin())) : this.asAdmin().addStep(new OrderGlobalStep<>(this.asAdmin()));
    }

    public default <E2> GraphTraversal<S, ? extends Property<E2>> properties(final String... propertyKeys) {
        return this.asAdmin().addStep(new PropertiesStep<>(this.asAdmin(), PropertyType.PROPERTY, propertyKeys));
    }

    public default <E2> GraphTraversal<S, E2> values(final String... propertyKeys) {
        return this.asAdmin().addStep(new PropertiesStep<>(this.asAdmin(), PropertyType.VALUE, propertyKeys));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> propertyMap(final String... propertyKeys) {
        return this.asAdmin().addStep(new PropertyMapStep<>(this.asAdmin(), false, PropertyType.PROPERTY, propertyKeys));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> valueMap(final String... propertyKeys) {
        return this.asAdmin().addStep(new PropertyMapStep<>(this.asAdmin(), false, PropertyType.VALUE, propertyKeys));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> valueMap(final boolean includeTokens, final String... propertyKeys) {
        return this.asAdmin().addStep(new PropertyMapStep<>(this.asAdmin(), includeTokens, PropertyType.VALUE, propertyKeys));
    }

    public default GraphTraversal<S, String> key() {
        return this.asAdmin().addStep(new KeyStep(this.asAdmin()));
    }

    public default <E2> GraphTraversal<S, E2> value() {
        return this.asAdmin().addStep(new PropertyValueStep<>(this.asAdmin()));
    }

    public default GraphTraversal<S, Path> path() {
        return this.asAdmin().addStep(new PathStep<>(this.asAdmin()));
    }

    public default <E2> GraphTraversal<S, E2> back(final String stepLabel) {
        return this.asAdmin().addStep(new BackStep<>(this.asAdmin(), stepLabel));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> match(final String startLabel, final Traversal... traversals) {
        return (GraphTraversal) this.asAdmin().addStep(new MatchStep<S, Map<String, E2>>(this.asAdmin(), startLabel, traversals));
    }

    public default <E2> GraphTraversal<S, E2> sack() {
        return this.asAdmin().addStep(new SackStep<>(this.asAdmin()));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> select(final String... stepLabels) {
        return this.asAdmin().addStep(new SelectStep<>(this.asAdmin(), stepLabels));
    }

    public default <E2> GraphTraversal<S, E2> select(final String stepLabel) {
        return this.asAdmin().addStep(new SelectOneStep(this.asAdmin(), stepLabel));
    }

    public default <E2> GraphTraversal<S, E2> unfold() {
        return this.asAdmin().addStep(new UnfoldStep<>(this.asAdmin()));
    }

    public default GraphTraversal<S, List<E>> fold() {
        return this.asAdmin().addStep(new FoldStep<>(this.asAdmin()));
    }

    public default <E2> GraphTraversal<S, E2> fold(final E2 seed, final BiFunction<E2, E, E2> foldFunction) {
        return this.asAdmin().addStep(new FoldStep<>(this.asAdmin(), () -> seed, foldFunction)); // TODO: User should provide supplier?
    }

    public default GraphTraversal<S, Long> count() {
        return this.asAdmin().addStep(new CountStep<>(this.asAdmin()));
    }

    public default GraphTraversal<S, Double> sum() {
        return this.asAdmin().addStep(new SumStep(this.asAdmin()));
    }

    public default <E2 extends Number> GraphTraversal<S, E2> max() {
        return this.asAdmin().addStep(new MaxStep<>(this.asAdmin()));
    }

    public default <E2 extends Number> GraphTraversal<S, E2> min() {
        return this.asAdmin().addStep(new MinStep<>(this.asAdmin()));
    }

    public default GraphTraversal<S, Double> mean() {
        return this.asAdmin().addStep(new MeanStep<>(this.asAdmin()));
    }

    ///////////////////// FILTER STEPS /////////////////////

    public default GraphTraversal<S, E> filter(final Predicate<Traverser<E>> predicate) {
        final FilterStep<E> filterStep = new FilterStep<>(this.asAdmin());
        filterStep.setPredicate(predicate);
        return this.asAdmin().addStep(filterStep);
    }

    public default GraphTraversal<S, E> or(final Traversal<?, ?>... orTraversals) {
        return this.asAdmin().addStep(new OrStep(this.asAdmin(), Arrays.copyOf(orTraversals, orTraversals.length, Traversal.Admin[].class)));
    }

    public default GraphTraversal<S, E> and(final Traversal<?, ?>... andTraversals) {
        return this.asAdmin().addStep(new AndStep(this.asAdmin(), Arrays.copyOf(andTraversals, andTraversals.length, Traversal.Admin[].class)));
    }

    public default GraphTraversal<S, E> inject(final E... injections) {
        return this.asAdmin().addStep(new InjectStep<>(this.asAdmin(), injections));
    }

    public default GraphTraversal<S, E> dedup() {
        return this.asAdmin().addStep(new DedupStep<>(this.asAdmin()));
    }

    public default GraphTraversal<S, E> except(final String sideEffectKeyOrPathLabel) {
        return this.asAdmin().addStep(new ExceptStep<E>(this.asAdmin(), sideEffectKeyOrPathLabel));
    }

    public default GraphTraversal<S, E> except(final E exceptObject) {
        return this.asAdmin().addStep(new ExceptStep<>(this.asAdmin(), exceptObject));
    }

    public default GraphTraversal<S, E> except(final Collection<E> exceptCollection) {
        return this.asAdmin().addStep(new ExceptStep<>(this.asAdmin(), exceptCollection));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> where(final String firstKey, final String secondKey, final BiPredicate predicate) {
        return this.asAdmin().addStep(new WhereStep(this.asAdmin(), firstKey, secondKey, predicate));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> where(final String firstKey, final BiPredicate predicate, final String secondKey) {
        return this.where(firstKey, secondKey, predicate);
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> where(final Traversal constraint) {
        return this.asAdmin().addStep(new WhereStep<>(this.asAdmin(), constraint.asAdmin()));
    }

    public default GraphTraversal<S, E> has(final Traversal<?, ?> hasNextTraversal) {
        return this.asAdmin().addStep(new HasTraversalStep<>(this.asAdmin(), (Traversal.Admin<E, ?>) hasNextTraversal));
    }

    public default GraphTraversal<S, E> has(final String key) {
        return this.asAdmin().addStep(new HasStep(this.asAdmin(), new HasContainer(key, Contains.within)));
    }

    public default GraphTraversal<S, E> has(final String key, final Object value) {
        return this.has(key, Compare.eq, value);
    }

    public default GraphTraversal<S, E> has(final T accessor, final Object value) {
        return this.has(accessor.getAccessor(), value);
    }

    public default GraphTraversal<S, E> has(final String key, final BiPredicate predicate, final Object value) {
        return this.asAdmin().addStep(new HasStep(this.asAdmin(), new HasContainer(key, predicate, value)));
    }

    public default GraphTraversal<S, E> has(final T accessor, final BiPredicate predicate, final Object value) {
        return this.has(accessor.getAccessor(), predicate, value);
    }

    public default GraphTraversal<S, E> has(final String label, final String key, final Object value) {
        return this.has(label, key, Compare.eq, value);
    }

    public default GraphTraversal<S, E> has(final String label, final String key, final BiPredicate predicate, final Object value) {
        return this.has(T.label, label).has(key, predicate, value);
    }

    public default GraphTraversal<S, E> hasNot(final String key) {
        return this.asAdmin().addStep(new HasStep(this.asAdmin(), new HasContainer(key, Contains.without)));
    }

    public default GraphTraversal<S, E> is(final Object value) {
        return this.is(Compare.eq, value);
    }

    public default GraphTraversal<S, E> is(final BiPredicate predicate, final Object value) {
        return this.asAdmin().addStep(new IsStep(this.asAdmin(), predicate, value));
    }

    public default GraphTraversal<S, E> coin(final double probability) {
        return this.asAdmin().addStep(new CoinStep<>(this.asAdmin(), probability));
    }

    public default GraphTraversal<S, E> range(final long low, final long high) {
        return this.asAdmin().addStep(new RangeStep<>(this.asAdmin(), low, high));
    }

    public default GraphTraversal<S, E> limit(final long limit) {
        return this.range(0, limit);
    }

    public default GraphTraversal<S, E> retain(final String sideEffectKeyOrPathLabel) {
        return this.asAdmin().addStep(new RetainStep<>(this.asAdmin(), sideEffectKeyOrPathLabel));
    }

    public default GraphTraversal<S, E> retain(final E retainObject) {
        return this.asAdmin().addStep(new RetainStep<>(this.asAdmin(), retainObject));
    }

    public default GraphTraversal<S, E> retain(final Collection<E> retainCollection) {
        return this.asAdmin().addStep(new RetainStep<>(this.asAdmin(), retainCollection));
    }

    public default GraphTraversal<S, E> simplePath() {
        return this.asAdmin().addStep(new SimplePathStep<>(this.asAdmin()));
    }

    public default GraphTraversal<S, E> cyclicPath() {
        return this.asAdmin().addStep(new CyclicPathStep<>(this.asAdmin()));
    }

    public default GraphTraversal<S, E> sample(final int amountToSample) {
        return this.asAdmin().addStep(new SampleStep<>(this.asAdmin(), amountToSample));
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public default GraphTraversal<S, E> sideEffect(final Consumer<Traverser<E>> consumer) {
        final SideEffectStep<E> sideEffectStep = new SideEffectStep<>(this.asAdmin());
        sideEffectStep.setConsumer(consumer);
        return this.asAdmin().addStep(sideEffectStep);
    }

    public default <E2> GraphTraversal<S, E2> cap(final String... sideEffectKeys) {
        return this.asAdmin().addStep(new SideEffectCapStep<>(this.asAdmin(), sideEffectKeys));
    }

    public default GraphTraversal<S, Edge> subgraph(final String sideEffectKey) {
        return this.asAdmin().addStep(new SubgraphStep(this.asAdmin(), sideEffectKey));
    }

    public default GraphTraversal<S, Edge> subgraph() {
        return this.subgraph(null);
    }

    public default GraphTraversal<S, E> aggregate(final String sideEffectKey) {
        return this.asAdmin().addStep(new AggregateStep<>(this.asAdmin(), sideEffectKey));
    }

    public default GraphTraversal<S, E> aggregate() {
        return this.aggregate(null);
    }

    public default GraphTraversal<S, E> group(final String sideEffectKey) {
        return this.asAdmin().addStep(new GroupStep<>(this.asAdmin(), sideEffectKey));
    }

    public default GraphTraversal<S, E> group() {
        return this.group(null);
    }

    public default GraphTraversal<S, E> groupCount(final String sideEffectKey) {
        return this.asAdmin().addStep(new GroupCountStep<>(this.asAdmin(), sideEffectKey));
    }

    public default GraphTraversal<S, E> groupCount() {
        return this.groupCount(null);
    }

    public default GraphTraversal<S, Vertex> addE(final Direction direction, final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.asAdmin().addStep(new AddEdgeStep(this.asAdmin(), direction, edgeLabel, stepLabel, propertyKeyValues));
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
        return this.asAdmin().addStep(new TimeLimitStep<E>(this.asAdmin(), timeLimit));
    }

    public default GraphTraversal<S, E> tree(final String sideEffectKey) {
        return this.asAdmin().addStep(new TreeStep<>(this.asAdmin(), sideEffectKey));
    }

    public default GraphTraversal<S, E> tree() {
        return this.tree(null);
    }

    public default <V> GraphTraversal<S, E> sack(final BiFunction<V, E, V> sackFunction) {
        return this.asAdmin().addStep(new SackObjectStep<>(this.asAdmin(), sackFunction));
    }

    public default <V> GraphTraversal<S, E> sack(final BinaryOperator<V> sackOperator, final String elementPropertyKey) {
        return this.asAdmin().addStep(new SackElementValueStep(this.asAdmin(), sackOperator, elementPropertyKey));
    }

    public default GraphTraversal<S, E> store(final String sideEffectKey) {
        return this.asAdmin().addStep(new StoreStep<>(this.asAdmin(), sideEffectKey));
    }

    public default GraphTraversal<S, E> store() {
        return this.store(null);
    }

    public default GraphTraversal<S, E> profile() {
        return this.asAdmin().addStep(new ProfileStep<>(this.asAdmin()));
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public default <M, E2> GraphTraversal<S, E2> branch(final Traversal<?, M> branchTraversal) {
        final BranchStep<E, E2, M> branchStep = new BranchStep<>(this.asAdmin());
        branchStep.setBranchTraversal((Traversal.Admin<E, M>) branchTraversal);
        return this.asAdmin().addStep(branchStep);
    }

    public default <M, E2> GraphTraversal<S, E2> branch(final Function<Traverser<E>, M> function) {
        return this.branch(new MapTraverserTraversal<>(function));
    }

    public default <M, E2> GraphTraversal<S, E2> choose(final Traversal<?, M> choiceTraversal) {
        return this.asAdmin().addStep(new ChooseStep<>(this.asAdmin(), (Traversal.Admin<E, M>) choiceTraversal));
    }

    public default <E2> GraphTraversal<S, E2> choose(final Traversal<?, ?> traversalPredicate, final Traversal<?, E2> trueChoice, final Traversal<?, E2> falseChoice) {
        return this.asAdmin().addStep(new ChooseStep<E, E2, Boolean>(this.asAdmin(), (Traversal.Admin<E, ?>) traversalPredicate, (Traversal.Admin<E, E2>) trueChoice, (Traversal.Admin<E, E2>) falseChoice));
    }

    public default <M, E2> GraphTraversal<S, E2> choose(final Function<E, M> choiceFunction) {
        return this.choose(new MapTraversal<>(choiceFunction));
    }

    public default <E2> GraphTraversal<S, E2> choose(final Predicate<E> choosePredicate, final Traversal<?, E2> trueChoice, final Traversal<?, E2> falseChoice) {
        return this.choose(new FilterTraversal<>(choosePredicate), trueChoice, falseChoice);
    }

    public default <E2> GraphTraversal<S, E2> union(final Traversal<?, E2>... unionTraversals) {
        return this.asAdmin().addStep(new UnionStep(this.asAdmin(), Arrays.copyOf(unionTraversals, unionTraversals.length, Traversal.Admin[].class)));
    }

    public default GraphTraversal<S, E> repeat(final Traversal<?, E> repeatTraversal) {
        return RepeatStep.addRepeatToTraversal(this, (Traversal.Admin<E, E>) repeatTraversal);
    }

    public default GraphTraversal<S, E> emit(final Traversal<?, ?> emitTraversal) {
        return RepeatStep.addEmitToTraversal(this, (Traversal.Admin<E, ?>) emitTraversal);
    }

    public default GraphTraversal<S, E> emit(final Predicate<Traverser<E>> emitPredicate) {
        return this.emit(new FilterTraverserTraversal<>(emitPredicate));
    }

    public default GraphTraversal<S, E> emit() {
        return this.emit(TrueTraversal.instance());
    }

    public default GraphTraversal<S, E> until(final Traversal<?, ?> untilTraversal) {
        return RepeatStep.addUntilToTraversal(this, (Traversal.Admin<E, ?>) untilTraversal);
    }

    public default GraphTraversal<S, E> until(final Predicate<Traverser<E>> untilPredicate) {
        return this.until(new FilterTraverserTraversal<>(untilPredicate));
    }

    public default GraphTraversal<S, E> times(final int maxLoops) {
        return this.until(new LoopTraversal(maxLoops));
    }

    public default <E2> GraphTraversal<S, E2> local(final Traversal<?, E2> localTraversal) {
        return this.asAdmin().addStep(new LocalStep<>(this.asAdmin(), localTraversal.asAdmin()));
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
        return this.asAdmin().addStep(new PathIdentityStep<>(this.asAdmin()));
    }

    public default GraphTraversal<S, E> as(final String stepLabel) {
        if (this.asAdmin().getSteps().size() == 0) this.asAdmin().addStep(new StartStep<>(this.asAdmin()));
        this.asAdmin().getEndStep().setLabel(stepLabel);
        return this;
    }

    public default GraphTraversal<S, E> barrier() {
        return this.asAdmin().addStep(new CollectingBarrierStep<>(this.asAdmin()));
    }

    ////

    public default GraphTraversal<S, E> by() {
        ((TraversalParent) this.asAdmin().getEndStep()).addLocalChild(new IdentityTraversal<>());
        return this;
    }

    public default <V> GraphTraversal<S, E> by(final Function<V, Object> functionProjection) {
        ((TraversalParent) this.asAdmin().getEndStep()).addLocalChild(new MapTraversal<>(functionProjection));
        return this;
    }

    public default GraphTraversal<S, E> by(final T tokenProjection) {
        ((TraversalParent) this.asAdmin().getEndStep()).addLocalChild(new MapTraversal<>(tokenProjection));
        return this;
    }

    public default GraphTraversal<S, E> by(final String elementPropertyKey) {
        ((TraversalParent) this.asAdmin().getEndStep()).addLocalChild(new ElementValueTraversal<>(elementPropertyKey));
        return this;
    }

    public default GraphTraversal<S, E> by(final Traversal<?, ?> byTraversal) {
        ((TraversalParent) this.asAdmin().getEndStep()).addLocalChild(byTraversal.asAdmin());
        return this;
    }

    ////

    public default GraphTraversal<S, E> by(final Order order) {
        ((ComparatorHolder) this.asAdmin().getEndStep()).addComparator(order);
        return this;
    }

    public default GraphTraversal<S, E> by(final Comparator<E> comparator) {
        ((ComparatorHolder<E>) this.asAdmin().getEndStep()).addComparator(comparator);
        return this;
    }

    public default <V> GraphTraversal<S, E> by(final Function<Element, V> elementFunctionProjection, final Comparator<V> elementFunctionValueComparator) {
        ((ComparatorHolder<Element>) this.asAdmin().getEndStep()).addComparator(new ElementFunctionComparator<>(elementFunctionProjection, elementFunctionValueComparator));
        return this;
    }

    public default <V> GraphTraversal<S, E> by(final String elementPropertyProjection, final Comparator<V> propertyValueComparator) {
        ((ComparatorHolder<Element>) this.asAdmin().getEndStep()).addComparator(new ElementValueComparator<>(elementPropertyProjection, propertyValueComparator));
        return this;
    }

    ////

    public default <M, E2> GraphTraversal<S, E> option(final M pickToken, final Traversal<E, E2> traversalOption) {
        ((TraversalOptionParent<M, E, E2>) this.asAdmin().getEndStep()).addGlobalChildOption(pickToken, traversalOption.asAdmin());
        return this;
    }

    public default <E2> GraphTraversal<S, E> option(final Traversal<E, E2> traversalOption) {
        ((TraversalOptionParent<TraversalOptionParent.Pick, E, E2>) this.asAdmin().getEndStep()).addGlobalChildOption(TraversalOptionParent.Pick.any, traversalOption.asAdmin());
        return this;
    }

    ////

    @Override
    public default void remove() {
        try {
            this.asAdmin().applyStrategies(TraversalEngine.STANDARD);
            final Step<?, E> endStep = this.asAdmin().getEndStep();
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

    @Override
    public default GraphTraversal<S, E> iterate() {
        Traversal.super.iterate();
        return this;
    }
}
