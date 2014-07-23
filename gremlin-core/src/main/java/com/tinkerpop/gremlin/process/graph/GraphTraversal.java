package com.tinkerpop.gremlin.process.graph;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.ranking.PageRankStep;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.step.filter.ComputerResultStartStep;
import com.tinkerpop.gremlin.process.computer.traversal.step.util.TraversalResultMapReduce;
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
import com.tinkerpop.gremlin.process.graph.step.map.ChooseStep;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeOtherVertexStep;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.graph.step.map.ElementPropertyStep;
import com.tinkerpop.gremlin.process.graph.step.map.ElementValueStep;
import com.tinkerpop.gremlin.process.graph.step.map.ElementValuesStep;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;
import com.tinkerpop.gremlin.process.graph.step.map.FoldStep;
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
import com.tinkerpop.gremlin.process.graph.step.map.UnfoldStep;
import com.tinkerpop.gremlin.process.graph.step.map.UnionStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AddEdgeStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AggregateStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupByStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupCountStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapable;
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
import com.tinkerpop.gremlin.util.function.SSupplier;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

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
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GraphTraversal<S, E> extends Traversal<S, E> {

    public default GraphTraversal<S, E> submit(final GraphComputer computer) {
        try {
            final Configuration configuration = TraversalVertexProgram.create().traversal(() -> this).getConfiguration();
            final TraversalVertexProgram program = VertexProgram.createVertexProgram(configuration);
            final Traversal programTraversal = (Traversal) program.getTraversalSupplier().get();
            programTraversal.strategies().applyFinalStrategies();
            final Pair<Graph, GraphComputer.Globals> result = computer.program(configuration).submit().get();
            final GraphTraversal traversal = new DefaultGraphTraversal<>();
            if (program.getResultVariable().equals(TraversalResultMapReduce.TRAVERSERS))
                traversal.addStep(new ComputerResultStartStep<>(traversal, computer.getGraph(), result.getValue1().get(program.getResultVariable()), programTraversal));
            else
                traversal.addStep(new StartStep<>(traversal, result.getValue1().get(program.getResultVariable())));
            return traversal;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public default GraphTraversal<S, E> trackPaths() {
        return (GraphTraversal) this.addStep(new PathIdentityStep<>(this));
    }

    public default GraphTraversal<S, Long> count() {
        return (GraphTraversal) this.addStep(new CountStep<>(this));
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
        //return (GraphTraversal) this.addStep(new MatchStepNew<S, E2>(this, inAs, traversals));
        return (GraphTraversal) this.addStep(new MatchStep<S, E2>(this, inAs, outAs, traversals));
    }

    public default GraphTraversal<S, Path> select(final List<String> asLabels, SFunction... stepFunctions) {
        return (GraphTraversal) this.addStep(new SelectStep(this, asLabels, stepFunctions));
    }

    public default GraphTraversal<S, Path> select(final SFunction... stepFunctions) {
        return (GraphTraversal) this.addStep(new SelectStep(this, Arrays.asList(), stepFunctions));
    }

    public default <E2> GraphTraversal<S, E2> union(final Traversal... traversals) {
        return (GraphTraversal) this.addStep(new UnionStep(this, traversals));
    }

    public default <E2> GraphTraversal<S, E2> intersect(final Traversal... traversals) {
        return (GraphTraversal) this.addStep(new IntersectStep(this, traversals));
    }

    public default <E2> GraphTraversal<S, E2> unfold() {
        return (GraphTraversal) this.addStep(new UnfoldStep<>(this));
    }

    public default GraphTraversal<S, List<E>> fold() {
        return (GraphTraversal) this.addStep(new FoldStep<>(this));
    }

    public default <E2> GraphTraversal<S, E2> fold(final E2 seed, final SBiFunction<E2, E, E2> foldFunction) {
        return (GraphTraversal) this.addStep(new FoldStep<>(this, seed, foldFunction));
    }

    ///////////////////// EXPERIMENTAL STEPS /////////////////////

    public default <E2> GraphTraversal<S, E2> choose(final SPredicate<Traverser<S>> choosePredicate, final Traversal trueChoice, final Traversal falseChoice) {
        return (GraphTraversal) this.addStep(new ChooseStep(this, choosePredicate, trueChoice, falseChoice));
    }

    public default <E2, M> GraphTraversal<S, E2> choose(final SFunction<Traverser<S>, M> mapFunction, final Map<M, Traversal<S, E2>> choices) {
        return (GraphTraversal) this.addStep(new ChooseStep<>(this, mapFunction, choices));
    }

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

    public default <E2> GraphTraversal<S, E2> has(final String key, final SBiPredicate predicate, final Object value) {
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

    public default GraphTraversal<S, E> aggregate(final SFunction<E, ?> preAggregateFunction) {
        return (GraphTraversal) this.addStep(new AggregateStep<>(this, SideEffectCapable.CAP_KEY, preAggregateFunction));
    }

    public default GraphTraversal<S, E> aggregate(final String variable, final SFunction<E, ?> preAggregateFunction) {
        return (GraphTraversal) this.addStep(new AggregateStep<>(this, variable, preAggregateFunction));
    }

    public default GraphTraversal<S, E> aggregate() {
        return (GraphTraversal) this.addStep(new AggregateStep<>(this, SideEffectCapable.CAP_KEY));
    }

    public default GraphTraversal<S, E> aggregate(final String variable) {
        return (GraphTraversal) this.addStep(new AggregateStep<>(this, variable));
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

    public default GraphTraversal<S, E> groupCount(final String variable, final SFunction<E, ?> preGroupFunction) {
        return (GraphTraversal) this.addStep(new GroupCountStep<>(this, variable, preGroupFunction));
    }

    public default GraphTraversal<S, E> groupCount(final SFunction<E, ?> preGroupFunction) {
        return (GraphTraversal) this.addStep(new GroupCountStep<>(this, SideEffectCapable.CAP_KEY, preGroupFunction));
    }

    public default GraphTraversal<S, E> groupCount(final String variable) {
        return (GraphTraversal) this.addStep(new GroupCountStep<>(this, variable));
    }

    public default GraphTraversal<S, E> groupCount() {
        return (GraphTraversal) this.addStep(new GroupCountStep<>(this, SideEffectCapable.CAP_KEY));
    }

    public default GraphTraversal<S, Vertex> addInE(final String label, final String as) {
        return (GraphTraversal) this.addStep(new AddEdgeStep(this, Direction.IN, label, as));
    }

    public default GraphTraversal<S, Vertex> addOutE(final String label, final String as) {
        return (GraphTraversal) this.addStep(new AddEdgeStep(this, Direction.OUT, label, as));
    }

    public default GraphTraversal<S, Vertex> addBothE(final String label, final String as) {
        return (GraphTraversal) this.addStep(new AddEdgeStep(this, Direction.BOTH, label, as));
    }

    public default GraphTraversal<S, Vertex> addE(final Direction direction, final String label, final String as) {
        return (GraphTraversal) this.addStep(new AddEdgeStep(this, direction, label, as));
    }

    public default GraphTraversal<S, E> timeLimit(final long timeLimit) {
        return (GraphTraversal) this.addStep(new TimeLimitStep<E>(this, timeLimit));
    }

    public default GraphTraversal<S, E> tree(final String variable, final SFunction... branchFunctions) {
        return (GraphTraversal) this.addStep(new TreeStep<>(this, variable, branchFunctions));
    }

    public default GraphTraversal<S, E> tree(final SFunction... branchFunctions) {
        return (GraphTraversal) this.addStep(new TreeStep<>(this, SideEffectCapable.CAP_KEY, branchFunctions));
    }

    public default GraphTraversal<S, E> store(final String variable, final SFunction<E, ?> preStoreFunction) {
        return (GraphTraversal) this.addStep(new StoreStep<>(this, variable, preStoreFunction));
    }

    public default GraphTraversal<S, E> store(final String variable) {
        return (GraphTraversal) this.addStep(new StoreStep<>(this, variable));
    }

    public default GraphTraversal<S, E> store(final SFunction<E, ?> preStoreFunction) {
        return (GraphTraversal) this.addStep(new StoreStep<>(this, SideEffectCapable.CAP_KEY, preStoreFunction));
    }

    public default GraphTraversal<S, E> store() {
        return (GraphTraversal) this.addStep(new StoreStep<>(this, SideEffectCapable.CAP_KEY));
    }
    ///////////////////// BRANCH STEPS /////////////////////

    public default GraphTraversal<S, E> jump(final String as, final SPredicate<Traverser<E>> ifPredicate, final SPredicate<Traverser<E>> emitPredicate) {
        return (GraphTraversal) this.addStep(new JumpStep<>(this, as, ifPredicate, emitPredicate));
    }

    public default GraphTraversal<S, E> jump(final String as, final SPredicate<Traverser<E>> ifPredicate) {
        return (GraphTraversal) this.addStep(new JumpStep<>(this, as, ifPredicate));
    }

    public default GraphTraversal<S, E> jump(final String as, final int loops, final SPredicate<Traverser<E>> emitPredicate) {
        return (GraphTraversal) this.addStep(new JumpStep<>(this, as, loops, emitPredicate));
    }

    public default GraphTraversal<S, E> jump(final String as, final int loops) {
        return (GraphTraversal) this.addStep(new JumpStep<>(this, as, loops));
    }

    public default GraphTraversal<S, E> jump(final String as) {
        return (GraphTraversal) this.addStep(new JumpStep<>(this, as));
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public default GraphTraversal<S, E> as(final String as) {
        if (TraversalHelper.asExists(as, this))
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

    public default GraphTraversal<S, E> with(final Object... variableValues) {
        for (int i = 0; i < variableValues.length; i = i + 2) {
            this.memory().set((String) variableValues[i], variableValues[i + 1]);
        }
        return this;
    }

    public default <E2> GraphTraversal<S, E2> memory(final String key) {
        final MapStep<S, E2> mapStep = new MapStep<>(this);
        mapStep.setFunction(t -> this.memory().get(key));
        this.addStep(mapStep);
        return (GraphTraversal) this;
    }


    /////////////////////////////////////

    public default GraphTraversal<S, Pair<Vertex, Double>> pageRank() {
        return (GraphTraversal) this.addStep(new PageRankStep(this));
    }

    public default GraphTraversal<S, Pair<Vertex, Double>> pageRank(final SSupplier<Traversal> incidentTraversal) {
        return (GraphTraversal) this.addStep(new PageRankStep(this, (SSupplier) incidentTraversal));
    }
}
