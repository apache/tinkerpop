package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jTraversal;
import com.tinkerpop.gremlin.neo4j.process.graph.util.DefaultNeo4jTraversal;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;
import com.tinkerpop.gremlin.util.function.SBiFunction;
import com.tinkerpop.gremlin.util.function.SBiPredicate;
import com.tinkerpop.gremlin.util.function.SConsumer;
import com.tinkerpop.gremlin.util.function.SFunction;
import com.tinkerpop.gremlin.util.function.SPredicate;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Neo4jEdge extends Neo4jElement implements Edge, WrappedEdge<Relationship> {

    public Neo4jEdge(final Relationship relationship, final Neo4jGraph graph) {
        super(graph);
        this.baseElement = relationship;
    }

    @Override
    public void remove() {
        this.graph.tx().readWrite();
        try {
            ((Relationship) baseElement).delete();
        } catch (NotFoundException ignored) {
            // this one happens if the edge is committed
        } catch (IllegalStateException ignored) {
            // this one happens if the edge is still chilling in the tx
        }
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        this.graph.tx().readWrite();
        return (Iterator) Neo4jHelper.getVertices(this, direction);
    }

    public Neo4jTraversal<Edge, Edge> start() {
        final Neo4jTraversal<Edge, Edge> traversal = new DefaultNeo4jTraversal<>(this.graph);
        return (Neo4jTraversal) traversal.addStep(new StartStep<>(traversal, this));
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }

    public Relationship getBaseEdge() {
        return (Relationship) this.baseElement;
    }

    //////////////////////////////////////////////////////////////////////

    public Neo4jTraversal<Edge, Edge> trackPaths() {
        return this.start().trackPaths();
    }

    public Neo4jTraversal<Edge, Long> count() {
        return this.start().count();
    }

    public Neo4jTraversal<Edge, Edge> submit(final GraphComputer graphComputer) {
        return this.start().submit(graphComputer);
    }

    ///////////////////// TRANSFORM STEPS /////////////////////

    public <E2> Neo4jTraversal<Edge, E2> map(final SFunction<Traverser<Edge>, E2> function) {
        return this.start().map(function);
    }

    public <E2> Neo4jTraversal<Edge, E2> flatMap(final SFunction<Traverser<Edge>, Iterator<E2>> function) {
        return this.start().flatMap(function);
    }

    public Neo4jTraversal<Edge, Edge> identity() {
        return this.start().identity();
    }

    public Neo4jTraversal<Edge, Vertex> to(final Direction direction, final int branchFactor, final String... labels) {
        return this.start().to(direction, branchFactor, labels);
    }

    public Neo4jTraversal<Edge, Vertex> to(final Direction direction, final String... edgeLabels) {
        return this.start().to(direction, edgeLabels);
    }

    public Neo4jTraversal<Edge, Vertex> out(final int branchFactor, final String... edgeLabels) {
        return this.start().out(branchFactor, edgeLabels);
    }

    public Neo4jTraversal<Edge, Vertex> out(final String... edgeLabels) {
        return this.start().out(edgeLabels);
    }

    public Neo4jTraversal<Edge, Vertex> in(final int branchFactor, final String... edgeLabels) {
        return this.start().in(branchFactor, edgeLabels);
    }

    public Neo4jTraversal<Edge, Vertex> in(final String... edgeLabels) {
        return this.start().in(edgeLabels);
    }

    public Neo4jTraversal<Edge, Vertex> both(final int branchFactor, final String... edgeLabels) {
        return this.start().both(branchFactor, edgeLabels);
    }

    public Neo4jTraversal<Edge, Vertex> both(final String... edgeLabels) {
        return this.start().both(edgeLabels);
    }

    public Neo4jTraversal<Edge, Edge> toE(final Direction direction, final int branchFactor, final String... edgeLabels) {
        return this.start().toE(direction, branchFactor, edgeLabels);
    }

    public Neo4jTraversal<Edge, Edge> toE(final Direction direction, final String... edgeLabels) {
        return this.start().toE(direction, edgeLabels);
    }

    public Neo4jTraversal<Edge, Edge> outE(final int branchFactor, final String... edgeLabels) {
        return this.start().outE(branchFactor, edgeLabels);
    }

    public Neo4jTraversal<Edge, Edge> outE(final String... edgeLabels) {
        return this.start().outE(edgeLabels);
    }

    public Neo4jTraversal<Edge, Edge> inE(final int branchFactor, final String... edgeLabels) {
        return this.start().inE(branchFactor, edgeLabels);
    }

    public Neo4jTraversal<Edge, Edge> inE(final String... edgeLabels) {
        return this.start().inE(edgeLabels);
    }

    public Neo4jTraversal<Edge, Edge> bothE(final int branchFactor, final String... edgeLabels) {
        return this.start().bothE(branchFactor, edgeLabels);
    }

    public Neo4jTraversal<Edge, Edge> bothE(final String... edgeLabels) {
        return this.start().bothE(edgeLabels);
    }

    public Neo4jTraversal<Edge, Vertex> toV(final Direction direction) {
        return this.start().toV(direction);
    }

    public Neo4jTraversal<Edge, Vertex> inV() {
        return this.start().inV();
    }

    public Neo4jTraversal<Edge, Vertex> outV() {
        return this.start().outV();
    }

    public Neo4jTraversal<Edge, Vertex> bothV() {
        return this.start().bothV();
    }

    public Neo4jTraversal<Edge, Vertex> otherV() {
        return this.start().otherV();
    }

    public Neo4jTraversal<Edge, Edge> order() {
        return this.start().order();
    }

    public Neo4jTraversal<Edge, Edge> order(final Comparator<Traverser<Edge>> comparator) {
        return this.start().order(comparator);
    }

    public Neo4jTraversal<Edge, Edge> shuffle() {
        return this.start().shuffle();
    }

    public <E2> Neo4jTraversal<Edge, E2> value() {
        return this.start().value();
    }

    public <E2> Neo4jTraversal<Edge, E2> value(final String propertyKey, final Supplier<E2> defaultSupplier) {
        return this.start().value(propertyKey, defaultSupplier);
    }

    public Neo4jTraversal<Edge, Map<String, Object>> values(final String... propertyKeys) {
        return this.start().values(propertyKeys);
    }

    public Neo4jTraversal<Edge, Path> path(final SFunction... pathFunctions) {
        return this.start().path(pathFunctions);
    }

    public <E2> Neo4jTraversal<Edge, E2> back(final String stepLabel) {
        return this.start().back(stepLabel);
    }

    public <E2> Neo4jTraversal<Edge, Map<String, E2>> match(final String startLabel, final Traversal... traversals) {
        return this.start().match(startLabel, traversals);
    }

    public <E2> Neo4jTraversal<Edge, Map<String, E2>> select(final List<String> labels, SFunction... stepFunctions) {
        return this.start().select(labels, stepFunctions);
    }

    public <E2> Neo4jTraversal<Edge, Map<String, E2>> select(final SFunction... stepFunctions) {
        return this.start().select(stepFunctions);
    }

    public <E2> Neo4jTraversal<Edge, E2> select(final String label, SFunction stepFunction) {
        return this.start().select(label, stepFunction);
    }

    public <E2> Neo4jTraversal<Edge, E2> select(final String label) {
        return this.start().select(label, null);
    }

    /*public <E2> Neo4jTraversal<S, E2> union(final Traversal... traversals) {
        return (Neo4jTraversal) this.addStep(new UnionStep(this, traversals));
    }*/

    /*public <E2> Neo4jTraversal<S, E2> intersect(final Traversal... traversals) {
        return (Neo4jTraversal) this.addStep(new IntersectStep(this, traversals));
    }*/

    public Neo4jTraversal<Edge, Edge> unfold() {
        return this.start().unfold();
    }

    public Neo4jTraversal<Edge, List<Edge>> fold() {
        return this.start().fold();
    }

    public <E2> Neo4jTraversal<Edge, E2> fold(final E2 seed, final SBiFunction<E2, Edge, E2> foldFunction) {
        return this.start().fold(seed, foldFunction);
    }

    public <E2> Neo4jTraversal<Edge, E2> choose(final SPredicate<Traverser<Edge>> choosePredicate, final Traversal trueChoice, final Traversal falseChoice) {
        return this.start().choose(choosePredicate, trueChoice, falseChoice);
    }

    public <E2, M> Neo4jTraversal<Edge, E2> choose(final SFunction<Traverser<Edge>, M> mapFunction, final Map<M, Traversal<Edge, E2>> choices) {
        return this.start().choose(mapFunction, choices);
    }

    ///////////////////// FILTER STEPS /////////////////////

    public Neo4jTraversal<Edge, Edge> filter(final SPredicate<Traverser<Edge>> predicate) {
        return this.start().filter(predicate);
    }

    public Neo4jTraversal<Edge, Edge> inject(final Object... injections) {
        return this.start().inject((Edge[]) injections);
    }

    public Neo4jTraversal<Edge, Edge> dedup() {
        return this.start().dedup();
    }

    public Neo4jTraversal<Edge, Edge> dedup(final SFunction<Traverser<Edge>, ?> uniqueFunction) {
        return this.start().dedup(uniqueFunction);
    }

    public Neo4jTraversal<Edge, Edge> except(final String sideEffectKey) {
        return this.start().except(sideEffectKey);
    }

    public Neo4jTraversal<Edge, Edge> except(final Object exceptionObject) {
        return this.start().except((Edge) exceptionObject);
    }

    public Neo4jTraversal<Edge, Edge> except(final Collection<Edge> exceptionCollection) {
        return this.start().except(exceptionCollection);
    }

    public Neo4jTraversal<Edge, Edge> has(final String key) {
        return this.start().has(key);
    }

    public Neo4jTraversal<Edge, Edge> has(final String key, final Object value) {
        return this.start().has(key, value);
    }

    public Neo4jTraversal<Edge, Edge> has(final String key, final T t, final Object value) {
        return this.start().has(key, t, value);
    }

    public Neo4jTraversal<Edge, Edge> has(final String key, final SBiPredicate predicate, final Object value) {
        return this.start().has(key, predicate, value);
    }

    public Neo4jTraversal<Edge, Edge> has(final String label, final String key, final Object value) {
        return this.start().has(label, key, value);
    }

    public Neo4jTraversal<Edge, Edge> has(final String label, final String key, final T t, final Object value) {
        return this.start().has(label, key, t, value);
    }

    public Neo4jTraversal<Edge, Edge> has(final String label, final String key, final SBiPredicate predicate, final Object value) {
        return this.start().has(label, key, predicate, value);
    }

    public Neo4jTraversal<Edge, Edge> hasNot(final String key) {
        return this.start().hasNot(key);
    }

    public <E2> Neo4jTraversal<Edge, Map<String, E2>> where(final String firstKey, final String secondKey, final SBiPredicate predicate) {
        return this.start().where(firstKey, secondKey, predicate);
    }

    public <E2> Neo4jTraversal<Edge, Map<String, E2>> where(final String firstKey, final SBiPredicate predicate, final String secondKey) {
        return this.start().where(firstKey, predicate, secondKey);
    }

    public <E2> Neo4jTraversal<Edge, Map<String, E2>> where(final String firstKey, final T t, final String secondKey) {
        return this.start().where(firstKey, t, secondKey);
    }

    public <E2> Neo4jTraversal<Edge, Map<String, E2>> where(final Traversal constraint) {
        return this.start().where(constraint);
    }

    public Neo4jTraversal<Edge, Edge> interval(final String key, final Comparable startValue, final Comparable endValue) {
        return this.start().interval(key, startValue, endValue);
    }

    public Neo4jTraversal<Edge, Edge> random(final double probability) {
        return this.start().random(probability);
    }

    public Neo4jTraversal<Edge, Edge> range(final int low, final int high) {
        return this.start().range(low, high);
    }

    public Neo4jTraversal<Edge, Edge> retain(final String sideEffectKey) {
        return this.start().retain(sideEffectKey);
    }

    public Neo4jTraversal<Edge, Edge> retain(final Object retainObject) {
        return this.start().retain((Edge) retainObject);
    }

    public Neo4jTraversal<Edge, Edge> retain(final Collection<Edge> retainCollection) {
        return this.start().retain(retainCollection);
    }

    public Neo4jTraversal<Edge, Edge> simplePath() {
        return this.start().simplePath();
    }

    public Neo4jTraversal<Edge, Edge> cyclicPath() {
        return this.start().cyclicPath();
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public Neo4jTraversal<Edge, Edge> sideEffect(final SConsumer<Traverser<Edge>> consumer) {
        return this.start().sideEffect(consumer);
    }

    public <E2> Neo4jTraversal<Edge, E2> cap(final String sideEffectKey) {
        return this.start().cap(sideEffectKey);
    }

    public <E2> Neo4jTraversal<Edge, E2> cap() {
        return this.start().cap();
    }

    public Neo4jTraversal<Edge, Edge> subgraph(final String sideEffectKey, final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(sideEffectKey, edgeIdHolder, vertexMap, includeEdge);
    }

    public Neo4jTraversal<Edge, Edge> subgraph(final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(null, edgeIdHolder, vertexMap, includeEdge);
    }

    public Neo4jTraversal<Edge, Edge> subgraph(final String sideEffectKey, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(sideEffectKey, null, null, includeEdge);
    }

    public Neo4jTraversal<Edge, Edge> subgraph(final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(null, null, null, includeEdge);
    }

    public Neo4jTraversal<Edge, Edge> aggregate(final String sideEffectKey, final SFunction<Traverser<Edge>, ?> preAggregateFunction) {
        return this.start().aggregate(sideEffectKey, preAggregateFunction);
    }

    public Neo4jTraversal<Edge, Edge> aggregate(final SFunction<Traverser<Edge>, ?> preAggregateFunction) {
        return this.start().aggregate(null, preAggregateFunction);
    }

    public Neo4jTraversal<Edge, Edge> aggregate() {
        return this.start().aggregate(null, null);
    }

    public Neo4jTraversal<Edge, Edge> aggregate(final String sideEffectKey) {
        return this.start().aggregate(sideEffectKey, null);
    }

    public Neo4jTraversal<Edge, Edge> groupBy(final String sideEffectKey, final SFunction<Traverser<Edge>, ?> keyFunction, final SFunction<Traverser<Edge>, ?> valueFunction, final SFunction<Collection, ?> reduceFunction) {
        return this.start().groupBy(sideEffectKey, keyFunction, valueFunction, reduceFunction);
    }


    public Neo4jTraversal<Edge, Edge> groupBy(final SFunction<Traverser<Edge>, ?> keyFunction, final SFunction<Traverser<Edge>, ?> valueFunction, final SFunction<Collection, ?> reduceFunction) {
        return this.start().groupBy(null, keyFunction, valueFunction, reduceFunction);
    }

    public Neo4jTraversal<Edge, Edge> groupBy(final SFunction<Traverser<Edge>, ?> keyFunction, final SFunction<Traverser<Edge>, ?> valueFunction) {
        return this.start().groupBy(null, keyFunction, valueFunction, null);
    }

    public Neo4jTraversal<Edge, Edge> groupBy(final SFunction<Traverser<Edge>, ?> keyFunction) {
        return this.start().groupBy(null, keyFunction, null, null);
    }

    public Neo4jTraversal<Edge, Edge> groupBy(final String sideEffectKey, final SFunction<Traverser<Edge>, ?> keyFunction) {
        return this.start().groupBy(sideEffectKey, keyFunction, null, null);
    }

    public Neo4jTraversal<Edge, Edge> groupBy(final String sideEffectKey, final SFunction<Traverser<Edge>, ?> keyFunction, final SFunction<Traverser<Edge>, ?> valueFunction) {
        return this.start().groupBy(sideEffectKey, keyFunction, valueFunction, null);
    }

    public Neo4jTraversal<Edge, Edge> groupCount(final String sideEffectKey, final SFunction<Traverser<Edge>, ?> preGroupFunction) {
        return this.start().groupCount(sideEffectKey, preGroupFunction);
    }

    public Neo4jTraversal<Edge, Edge> groupCount(final SFunction<Traverser<Edge>, ?> preGroupFunction) {
        return this.start().groupCount(null, preGroupFunction);
    }

    public Neo4jTraversal<Edge, Edge> groupCount(final String sideEffectKey) {
        return this.start().groupCount(sideEffectKey, null);
    }

    public Neo4jTraversal<Edge, Edge> groupCount() {
        return this.start().groupCount(null, null);
    }

    public Neo4jTraversal<Edge, Vertex> addE(final Direction direction, final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.start().addE(direction, edgeLabel, stepLabel, propertyKeyValues);
    }

    public Neo4jTraversal<Edge, Vertex> addInE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.start().addInE(edgeLabel, stepLabel, propertyKeyValues);
    }

    public Neo4jTraversal<Edge, Vertex> addOutE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.start().addOutE(edgeLabel, stepLabel, propertyKeyValues);
    }

    public Neo4jTraversal<Edge, Vertex> addBothE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.start().addBothE(edgeLabel, stepLabel, propertyKeyValues);
    }

    public Neo4jTraversal<Edge, Edge> timeLimit(final long timeLimit) {
        return this.start().timeLimit(timeLimit);
    }

    public Neo4jTraversal<Edge, Edge> tree(final String sideEffectKey, final SFunction... branchFunctions) {
        return this.start().tree(sideEffectKey, branchFunctions);
    }

    public Neo4jTraversal<Edge, Edge> tree(final SFunction... branchFunctions) {
        return this.start().tree(null, branchFunctions);
    }

    public Neo4jTraversal<Edge, Edge> store(final String sideEffectKey, final SFunction<Traverser<Edge>, ?> preStoreFunction) {
        return this.start().store(sideEffectKey, preStoreFunction);
    }

    public Neo4jTraversal<Edge, Edge> store(final String sideEffectKey) {
        return this.start().store(sideEffectKey, null);
    }

    public Neo4jTraversal<Edge, Edge> store(final SFunction<Traverser<Edge>, ?> preStoreFunction) {
        return this.start().store(null, preStoreFunction);
    }

    public Neo4jTraversal<Edge, Edge> store() {
        return this.start().store(null, null);
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public Neo4jTraversal<Edge, Edge> jump(final String jumpLabel, final SPredicate<Traverser<Edge>> ifPredicate, final SPredicate<Traverser<Edge>> emitPredicate) {
        return this.start().jump(jumpLabel, ifPredicate, emitPredicate);
    }

    public Neo4jTraversal<Edge, Edge> jump(final String jumpLabel, final SPredicate<Traverser<Edge>> ifPredicate) {
        return this.start().jump(jumpLabel, ifPredicate);
    }

    public Neo4jTraversal<Edge, Edge> jump(final String jumpLabel, final int loops, final SPredicate<Traverser<Edge>> emitPredicate) {
        return this.start().jump(jumpLabel, loops, emitPredicate);
    }

    public Neo4jTraversal<Edge, Edge> jump(final String jumpLabel, final int loops) {
        return this.start().jump(jumpLabel, loops);
    }

    public Neo4jTraversal<Edge, Edge> jump(final String jumpLabel) {
        return this.start().jump(jumpLabel);
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public Neo4jTraversal<Edge, Edge> as(final String label) {
        return this.start().as(label);
    }

    public Neo4jTraversal<Edge, Edge> with(final Object... sideEffectKeyValues) {
        return this.start().with(sideEffectKeyValues);
    }
}
