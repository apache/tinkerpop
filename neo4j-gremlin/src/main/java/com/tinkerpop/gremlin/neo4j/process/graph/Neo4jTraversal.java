package com.tinkerpop.gremlin.neo4j.process.graph;

import com.tinkerpop.gremlin.neo4j.process.graph.step.map.Neo4jCypherStep;
import com.tinkerpop.gremlin.neo4j.process.graph.util.DefaultNeo4jTraversal;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.filter.CyclicPathStep;
import com.tinkerpop.gremlin.process.graph.step.filter.DedupStep;
import com.tinkerpop.gremlin.process.graph.step.filter.ExceptStep;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.step.filter.IdentityStep;
import com.tinkerpop.gremlin.process.graph.step.filter.InjectStep;
import com.tinkerpop.gremlin.process.graph.step.filter.IntervalStep;
import com.tinkerpop.gremlin.process.graph.step.filter.PathIdentityStep;
import com.tinkerpop.gremlin.process.graph.step.filter.RandomStep;
import com.tinkerpop.gremlin.process.graph.step.filter.RangeStep;
import com.tinkerpop.gremlin.process.graph.step.filter.RetainStep;
import com.tinkerpop.gremlin.process.graph.step.filter.SimplePathStep;
import com.tinkerpop.gremlin.process.graph.step.map.BackStep;
import com.tinkerpop.gremlin.process.graph.step.map.ChooseStep;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeOtherVertexStep;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;
import com.tinkerpop.gremlin.process.graph.step.map.FoldStep;
import com.tinkerpop.gremlin.process.graph.step.map.IdStep;
import com.tinkerpop.gremlin.process.graph.step.map.JumpStep;
import com.tinkerpop.gremlin.process.graph.step.map.LabelStep;
import com.tinkerpop.gremlin.process.graph.step.map.MapStep;
import com.tinkerpop.gremlin.process.graph.step.map.OrderStep;
import com.tinkerpop.gremlin.process.graph.step.map.PathStep;
import com.tinkerpop.gremlin.process.graph.step.map.PropertyStep;
import com.tinkerpop.gremlin.process.graph.step.map.PropertyValueStep;
import com.tinkerpop.gremlin.process.graph.step.map.SelectOneStep;
import com.tinkerpop.gremlin.process.graph.step.map.SelectStep;
import com.tinkerpop.gremlin.process.graph.step.map.ShuffleStep;
import com.tinkerpop.gremlin.process.graph.step.map.UnfoldStep;
import com.tinkerpop.gremlin.process.graph.step.map.ValueStep;
import com.tinkerpop.gremlin.process.graph.step.map.ValuesStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.step.map.match.MatchStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AddEdgeStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AggregateStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupByStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupCountStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StoreStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SubgraphStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.TimeLimitStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.TreeStep;
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
import com.tinkerpop.gremlin.util.function.SBiFunction;
import com.tinkerpop.gremlin.util.function.SBiPredicate;
import com.tinkerpop.gremlin.util.function.SConsumer;
import com.tinkerpop.gremlin.util.function.SFunction;
import com.tinkerpop.gremlin.util.function.SPredicate;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Neo4jTraversal<S, E> extends GraphTraversal<S, E> {

    public static <S> Neo4jTraversal<S, S> of(final Graph graph) {
        if (!(graph instanceof Neo4jGraph))
            throw new IllegalArgumentException(String.format("graph must be of type %s", Neo4jGraph.class));
        return new DefaultNeo4jTraversal<S, S>((Neo4jGraph) graph);
    }

    public static <S> Neo4jTraversal<S, S> of() {
        return new DefaultNeo4jTraversal<>();
    }

    public default Neo4jTraversal<S, E> cypher(final String query) {
        return (Neo4jTraversal) this.addStep(new Neo4jCypherStep<>(query, this));
    }

    public default Neo4jTraversal<S, E> cypher(final String query, final Map<String, Object> params) {
        return (Neo4jTraversal) this.addStep(new Neo4jCypherStep<>(query, params, this));
    }

    /////////////////////////////////////////////////////////////
    // everything from here down is from GraphTraversal

    public default Neo4jTraversal<S, E> submit(final GraphComputer computer) {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    public default Neo4jTraversal<S, E> trackPaths() {
        return (Neo4jTraversal) this.addStep(new PathIdentityStep<>(this));
    }

    public default Neo4jTraversal<S, Long> count() {
        return (Neo4jTraversal) this.addStep(new CountStep<>(this));
    }

    ///////////////////// TRANSFORM STEPS /////////////////////

    public default <E2> Neo4jTraversal<S, E2> map(final SFunction<Traverser<E>, E2> function) {
        final MapStep<E, E2> mapStep = new MapStep<>(this);
        mapStep.setFunction(function);
        return (Neo4jTraversal) this.addStep(mapStep);
    }

    public default <E2> Neo4jTraversal<S, E2> flatMap(final SFunction<Traverser<E>, Iterator<E2>> function) {
        final FlatMapStep<E, E2> flatMapStep = new FlatMapStep<>(this);
        flatMapStep.setFunction(function);
        return (Neo4jTraversal) this.addStep(flatMapStep);
    }

    public default Neo4jTraversal<S, Object> id() {
        return (Neo4jTraversal) this.addStep(new IdStep<>(this));
    }

    public default Neo4jTraversal<S, String> label() {
        return (Neo4jTraversal) this.addStep(new LabelStep<>(this));
    }

    public default Neo4jTraversal<S, E> identity() {
        return (Neo4jTraversal) this.addStep(new IdentityStep<>(this));
    }

    public default Neo4jTraversal<S, Vertex> to(final Direction direction, final int branchFactor, final String... labels) {
        return (Neo4jTraversal) this.addStep(new VertexStep(this, Vertex.class, direction, branchFactor, labels));
    }

    public default Neo4jTraversal<S, Vertex> to(final Direction direction, final String... labels) {
        return this.to(direction, Integer.MAX_VALUE, labels);
    }

    public default Neo4jTraversal<S, Vertex> out(final int branchFactor, final String... labels) {
        return this.to(Direction.OUT, branchFactor, labels);
    }

    public default Neo4jTraversal<S, Vertex> out(final String... labels) {
        return this.to(Direction.OUT, Integer.MAX_VALUE, labels);
    }

    public default Neo4jTraversal<S, Vertex> in(final int branchFactor, final String... labels) {
        return this.to(Direction.IN, branchFactor, labels);
    }

    public default Neo4jTraversal<S, Vertex> in(final String... labels) {
        return this.to(Direction.IN, Integer.MAX_VALUE, labels);
    }

    public default Neo4jTraversal<S, Vertex> both(final int branchFactor, final String... labels) {
        return this.to(Direction.BOTH, branchFactor, labels);
    }

    public default Neo4jTraversal<S, Vertex> both(final String... labels) {
        return this.to(Direction.BOTH, Integer.MAX_VALUE, labels);
    }

    public default Neo4jTraversal<S, Edge> toE(final Direction direction, final int branchFactor, final String... labels) {
        return (Neo4jTraversal) this.addStep(new VertexStep(this, Edge.class, direction, branchFactor, labels));
    }

    public default Neo4jTraversal<S, Edge> toE(final Direction direction, final String... labels) {
        return this.toE(direction, Integer.MAX_VALUE, labels);
    }

    public default Neo4jTraversal<S, Edge> outE(final int branchFactor, final String... labels) {
        return this.toE(Direction.OUT, branchFactor, labels);
    }

    public default Neo4jTraversal<S, Edge> outE(final String... labels) {
        return this.toE(Direction.OUT, Integer.MAX_VALUE, labels);
    }

    public default Neo4jTraversal<S, Edge> inE(final int branchFactor, final String... labels) {
        return this.toE(Direction.IN, branchFactor, labels);
    }

    public default Neo4jTraversal<S, Edge> inE(final String... labels) {
        return this.toE(Direction.IN, Integer.MAX_VALUE, labels);
    }

    public default Neo4jTraversal<S, Edge> bothE(final int branchFactor, final String... labels) {
        return this.toE(Direction.BOTH, branchFactor, labels);
    }

    public default Neo4jTraversal<S, Edge> bothE(final String... labels) {
        return this.toE(Direction.BOTH, Integer.MAX_VALUE, labels);
    }

    public default Neo4jTraversal<S, Vertex> toV(final Direction direction) {
        return (Neo4jTraversal) this.addStep(new EdgeVertexStep(this, direction));
    }

    public default Neo4jTraversal<S, Vertex> inV() {
        return this.toV(Direction.IN);
    }

    public default Neo4jTraversal<S, Vertex> outV() {
        return this.toV(Direction.OUT);
    }

    public default Neo4jTraversal<S, Vertex> bothV() {
        return this.toV(Direction.BOTH);
    }

    public default Neo4jTraversal<S, Vertex> otherV() {
        return (Neo4jTraversal) this.addStep(new EdgeOtherVertexStep(this));
    }

    public default Neo4jTraversal<S, E> order() {
        return (Neo4jTraversal) this.addStep(new OrderStep<E>(this, (a, b) -> ((Comparable<E>) a.get()).compareTo(b.get())));
    }

    public default Neo4jTraversal<S, E> order(final Comparator<Traverser<E>> comparator) {
        return (Neo4jTraversal) this.addStep(new OrderStep<>(this, comparator));
    }

    public default <E2> Neo4jTraversal<S, Property<E2>> property(final String propertyKey) {
        return (Neo4jTraversal) this.addStep(new PropertyStep<>(this, propertyKey));
    }

    public default Neo4jTraversal<S, E> shuffle() {
        return (Neo4jTraversal) this.addStep(new ShuffleStep<>(this));
    }

    public default <E2> Neo4jTraversal<S, E2> value() {
        return (Neo4jTraversal) this.addStep(new PropertyValueStep<>(this));
    }

    public default <E2> Neo4jTraversal<S, E2> value(final String propertyKey) {
        return (Neo4jTraversal) this.addStep(new ValueStep<>(this, propertyKey));
    }

    public default <E2> Neo4jTraversal<S, E2> value(final String propertyKey, final E2 defaultValue) {
        return (Neo4jTraversal) this.addStep(new ValueStep<>(this, propertyKey, defaultValue));
    }

    public default <E2> Neo4jTraversal<S, E2> value(final String propertyKey, final Supplier<E2> defaultSupplier) {
        return (Neo4jTraversal) this.addStep(new ValueStep<>(this, propertyKey, defaultSupplier));
    }

    public default Neo4jTraversal<S, Map<String, Object>> values(final String... propertyKeys) {
        return (Neo4jTraversal) this.addStep(new ValuesStep(this, propertyKeys));
    }

    public default Neo4jTraversal<S, Path> path(final SFunction... pathFunctions) {
        return (Neo4jTraversal) this.addStep(new PathStep<>(this, pathFunctions));
    }

    public default <E2> Neo4jTraversal<S, E2> back(final String as) {
        return (Neo4jTraversal) this.addStep(new BackStep<>(this, as));
    }

    public default <E2> Neo4jTraversal<S, Map<String, E2>> match(final String inAs, final Traversal... traversals) {
        return (Neo4jTraversal) this.addStep(new MatchStep<S, Map<String, E2>>(this, inAs, traversals));
    }

    public default <E2> Neo4jTraversal<S, Map<String, E2>> select(final List<String> asLabels, SFunction... stepFunctions) {
        this.addStep(new SelectStep<>(this, asLabels, stepFunctions));
        if (asLabels.stream().filter(as -> TraversalHelper.hasAs(as, this)).findFirst().isPresent())
            this.addStep(new PathIdentityStep<>(this));
        return (Neo4jTraversal) this;
    }

    public default <E2> Neo4jTraversal<S, Map<String, E2>> select(final SFunction... stepFunctions) {
        this.addStep(new SelectStep(this, Arrays.asList(), stepFunctions));
        return (Neo4jTraversal) this.addStep(new PathIdentityStep<>(this));
    }

    public default <E2> Neo4jTraversal<S, E2> select(final String as, SFunction stepFunction) {
        this.addStep(new SelectOneStep<>(this, as, stepFunction));
        if (TraversalHelper.hasAs(as, this))
            this.addStep(new PathIdentityStep<>(this));
        return (Neo4jTraversal) this;
    }

    public default <E2> Neo4jTraversal<S, E2> select(final String as) {
        return this.select(as, null);
    }

    public default <E2> Neo4jTraversal<S, E2> unfold() {
        return (Neo4jTraversal) this.addStep(new UnfoldStep<>(this));
    }

    public default Neo4jTraversal<S, List<E>> fold() {
        return (Neo4jTraversal) this.addStep(new FoldStep<>(this));
    }

    public default <E2> Neo4jTraversal<S, E2> fold(final E2 seed, final SBiFunction<E2, E, E2> foldFunction) {
        return (Neo4jTraversal) this.addStep(new FoldStep<>(this, seed, foldFunction));
    }

    ///////////////////// EXPERIMENTAL STEPS /////////////////////

    public default <E2> Neo4jTraversal<S, E2> choose(final SPredicate<Traverser<S>> choosePredicate, final Traversal trueChoice, final Traversal falseChoice) {
        return (Neo4jTraversal) this.addStep(new ChooseStep(this, choosePredicate, trueChoice, falseChoice));
    }

    public default <E2, M> Neo4jTraversal<S, E2> choose(final SFunction<Traverser<S>, M> mapFunction, final Map<M, Traversal<S, E2>> choices) {
        return (Neo4jTraversal) this.addStep(new ChooseStep<>(this, mapFunction, choices));
    }

    ///////////////////// FILTER STEPS /////////////////////

    public default Neo4jTraversal<S, E> filter(final SPredicate<Traverser<E>> predicate) {
        final FilterStep<E> filterStep = new FilterStep<>(this);
        filterStep.setPredicate(predicate);
        return (Neo4jTraversal) this.addStep(filterStep);
    }

    public default Neo4jTraversal<S, E> inject(final E... injections) {
        return (Neo4jTraversal) this.addStep(new InjectStep(this, injections));
    }

    public default Neo4jTraversal<S, E> dedup() {
        return (Neo4jTraversal) this.addStep(new DedupStep<>(this));
    }

    public default Neo4jTraversal<S, E> dedup(final SFunction<E, ?> uniqueFunction) {
        return (Neo4jTraversal) this.addStep(new DedupStep<>(this, uniqueFunction));
    }

    public default Neo4jTraversal<S, E> except(final String variable) {
        return (Neo4jTraversal) this.addStep(new ExceptStep<E>(this, variable));
    }

    public default Neo4jTraversal<S, E> except(final E exceptionObject) {
        return (Neo4jTraversal) this.addStep(new ExceptStep<>(this, exceptionObject));
    }

    public default Neo4jTraversal<S, E> except(final Collection<E> exceptionCollection) {
        return (Neo4jTraversal) this.addStep(new ExceptStep<>(this, exceptionCollection));
    }

    public default <E2> Neo4jTraversal<S, E2> has(final String key) {
        return (Neo4jTraversal) this.addStep(new HasStep(this, new HasContainer(key, Contains.IN)));
    }

    public default <E2> Neo4jTraversal<S, E2> has(final String key, final Object value) {
        return this.has(key, Compare.EQUAL, value);
    }

    public default <E2> Neo4jTraversal<S, E2> has(final String key, final T t, final Object value) {
        return this.has(key, T.convert(t), value);
    }

    public default <E2> Neo4jTraversal<S, E2> has(final String key, final SBiPredicate predicate, final Object value) {
        return (Neo4jTraversal) this.addStep(new HasStep(this, new HasContainer(key, predicate, value)));
    }

    public default <E2> Neo4jTraversal<S, E2> hasNot(final String key) {
        return (Neo4jTraversal) this.addStep(new HasStep(this, new HasContainer(key, Contains.NOT_IN)));
    }

    public default <E2> Neo4jTraversal<S, E2> interval(final String key, final Comparable startValue, final Comparable endValue) {
        return (Neo4jTraversal) this.addStep(new IntervalStep(this,
                new HasContainer(key, Compare.GREATER_THAN_EQUAL, startValue),
                new HasContainer(key, Compare.LESS_THAN, endValue)));
    }

    public default Neo4jTraversal<S, E> random(final double probability) {
        return (Neo4jTraversal) this.addStep(new RandomStep<>(this, probability));
    }

    public default Neo4jTraversal<S, E> range(final int low, final int high) {
        return (Neo4jTraversal) this.addStep(new RangeStep<>(this, low, high));
    }

    public default Neo4jTraversal<S, E> retain(final String variable) {
        return (Neo4jTraversal) this.addStep(new RetainStep<>(this, variable));
    }

    public default Neo4jTraversal<S, E> retain(final E retainObject) {
        return (Neo4jTraversal) this.addStep(new RetainStep<>(this, retainObject));
    }

    public default Neo4jTraversal<S, E> retain(final Collection<E> retainCollection) {
        return (Neo4jTraversal) this.addStep(new RetainStep<>(this, retainCollection));
    }

    public default Neo4jTraversal<S, E> simplePath() {
        return (Neo4jTraversal) this.addStep(new SimplePathStep<>(this));
    }

    public default Neo4jTraversal<S, E> cyclicPath() {
        return (Neo4jTraversal) this.addStep(new CyclicPathStep<E>(this));
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public default Neo4jTraversal<S, E> sideEffect(final SConsumer<Traverser<E>> consumer) {
        return (Neo4jTraversal) this.addStep(new SideEffectStep<>(this, consumer));
    }

    public default <E2> Neo4jTraversal<S, E2> cap(final String memoryKey) {
        return (Neo4jTraversal) this.addStep(new SideEffectCapStep<>(this, memoryKey));
    }

    public default <E2> Neo4jTraversal<S, E2> cap() {
        return this.cap(((SideEffectCapable) TraversalHelper.getEnd(this)).getMemoryKey());
    }

    public default Neo4jTraversal<S, E> subgraph(final Graph g, final SPredicate<Edge> includeEdge) {
        return (Neo4jTraversal) this.addStep(new SubgraphStep<>(this, g, null, null, includeEdge));
    }

    public default Neo4jTraversal<S, E> subgraph(final Graph g, final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final SPredicate<Edge> includeEdge) {
        return (Neo4jTraversal) this.addStep(new SubgraphStep<>(this, g, edgeIdHolder, vertexMap, includeEdge));
    }

    public default Neo4jTraversal<S, E> aggregate(final String memoryKey, final SFunction<E, ?> preAggregateFunction) {
        return (Neo4jTraversal) this.addStep(new AggregateStep<>(this, memoryKey, preAggregateFunction));
    }

    public default Neo4jTraversal<S, E> aggregate(final SFunction<E, ?> preAggregateFunction) {
        return this.aggregate(null, preAggregateFunction);
    }

    public default Neo4jTraversal<S, E> aggregate() {
        return this.aggregate(null, null);
    }

    public default Neo4jTraversal<S, E> groupBy(final String memoryKey, final SFunction<E, ?> keyFunction, final SFunction<E, ?> valueFunction, final SFunction<Collection, ?> reduceFunction) {
        return (Neo4jTraversal) this.addStep(new GroupByStep(this, memoryKey, keyFunction, (SFunction) valueFunction, (SFunction) reduceFunction));
    }

    public default Neo4jTraversal<S, E> groupBy(final SFunction<E, ?> keyFunction, final SFunction<E, ?> valueFunction, final SFunction<Collection, ?> reduceFunction) {
        return this.groupBy(null, keyFunction, valueFunction, reduceFunction);
    }

    public default Neo4jTraversal<S, E> groupBy(final String memoryKey, final SFunction<E, ?> keyFunction) {
        return this.groupBy(memoryKey, keyFunction, null, null);
    }

    public default Neo4jTraversal<S, E> groupBy(final SFunction<E, ?> keyFunction) {
        return this.groupBy(null, keyFunction, null, null);
    }

    public default Neo4jTraversal<S, E> groupBy(final String memoryKey, final SFunction<E, ?> keyFunction, final SFunction<E, ?> valueFunction) {
        return this.groupBy(memoryKey, keyFunction, valueFunction, null);
    }

    public default Neo4jTraversal<S, E> groupBy(final SFunction<E, ?> keyFunction, final SFunction<E, ?> valueFunction) {
        return this.groupBy(null, keyFunction, valueFunction, null);
    }

    public default Neo4jTraversal<S, E> groupCount(final String memoryKey, final SFunction<E, ?> preGroupFunction) {
        return (Neo4jTraversal) this.addStep(new GroupCountStep<>(this, memoryKey, preGroupFunction));
    }

    public default Neo4jTraversal<S, E> groupCount(final String memoryKey) {
        return this.groupCount(memoryKey, null);
    }


    public default Neo4jTraversal<S, E> groupCount(final SFunction<E, ?> preGroupFunction) {
        return this.groupCount(null, preGroupFunction);
    }

    public default Neo4jTraversal<S, E> groupCount() {
        return this.groupCount(null, null);
    }

    public default Neo4jTraversal<S, Vertex> addInE(final String label, final String as) {
        return (Neo4jTraversal) this.addStep(new AddEdgeStep(this, Direction.IN, label, as));
    }

    public default Neo4jTraversal<S, Vertex> addOutE(final String label, final String as) {
        return (Neo4jTraversal) this.addStep(new AddEdgeStep(this, Direction.OUT, label, as));
    }

    public default Neo4jTraversal<S, Vertex> addBothE(final String label, final String as) {
        return (Neo4jTraversal) this.addStep(new AddEdgeStep(this, Direction.BOTH, label, as));
    }

    public default Neo4jTraversal<S, Vertex> addE(final Direction direction, final String label, final String as) {
        return (Neo4jTraversal) this.addStep(new AddEdgeStep(this, direction, label, as));
    }

    public default Neo4jTraversal<S, E> timeLimit(final long timeLimit) {
        return (Neo4jTraversal) this.addStep(new TimeLimitStep<E>(this, timeLimit));
    }

    public default Neo4jTraversal<S, E> tree(final String memoryKey, final SFunction... branchFunctions) {
        return (Neo4jTraversal) this.addStep(new TreeStep<>(this, memoryKey, branchFunctions));
    }

    public default Neo4jTraversal<S, E> tree(final SFunction... branchFunctions) {
        return this.tree(null, branchFunctions);
    }

    public default Neo4jTraversal<S, E> store(final String memoryKey, final SFunction<E, ?> preStoreFunction) {
        return (Neo4jTraversal) this.addStep(new StoreStep<>(this, memoryKey, preStoreFunction));
    }

    public default Neo4jTraversal<S, E> store(final String memoryKey) {
        return this.store(memoryKey, null);
    }

    public default Neo4jTraversal<S, E> store(final SFunction<E, ?> preStoreFunction) {
        return this.store(null, preStoreFunction);
    }

    public default Neo4jTraversal<S, E> store() {
        return this.store(null, null);
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public default Neo4jTraversal<S, E> jump(final String as, final SPredicate<Traverser<E>> ifPredicate, final SPredicate<Traverser<E>> emitPredicate) {
        return (Neo4jTraversal) this.addStep(new JumpStep<>(this, as, ifPredicate, emitPredicate));
    }

    public default Neo4jTraversal<S, E> jump(final String as, final SPredicate<Traverser<E>> ifPredicate) {
        return (Neo4jTraversal) this.addStep(new JumpStep<>(this, as, ifPredicate));
    }

    public default Neo4jTraversal<S, E> jump(final String as, final int loops, final SPredicate<Traverser<E>> emitPredicate) {
        return (Neo4jTraversal) this.addStep(new JumpStep<>(this, as, loops, emitPredicate));
    }

    public default Neo4jTraversal<S, E> jump(final String as, final int loops) {
        return (Neo4jTraversal) this.addStep(new JumpStep<>(this, as, loops));
    }

    public default Neo4jTraversal<S, E> jump(final String as) {
        return (Neo4jTraversal) this.addStep(new JumpStep<>(this, as));
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public default Neo4jTraversal<S, E> as(final String as) {
        if (TraversalHelper.hasAs(as, this))
            throw new IllegalStateException("The named step already exists");
        final List<Step> steps = this.getSteps();
        steps.get(steps.size() - 1).setAs(as);
        return this;
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
        } catch (final NoSuchElementException ignored) {

        }
    }

    public default Neo4jTraversal<S, E> with(final Object... variableValues) {
        for (int i = 0; i < variableValues.length; i = i + 2) {
            this.memory().set((String) variableValues[i], variableValues[i + 1]);
        }
        return this;
    }

}
