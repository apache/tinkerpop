package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jTraversal;
import com.tinkerpop.gremlin.neo4j.process.graph.util.DefaultNeo4jTraversal;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;
import com.tinkerpop.gremlin.util.function.SBiConsumer;
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

    @Override
    public Neo4jTraversal<Edge, Edge> start() {
        final Neo4jTraversal<Edge, Edge> traversal = new DefaultNeo4jTraversal<>(this.graph);
        return (Neo4jTraversal) traversal.addStep(new StartStep<>(traversal, this));
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public Relationship getBaseEdge() {
        return (Relationship) this.baseElement;
    }

    //////////////////////////////////////////////////////////////////////

    @Override
    public Neo4jTraversal<Edge, Edge> trackPaths() {
        return this.start().trackPaths();
    }

    @Override
    public Neo4jTraversal<Edge, Long> count() {
        return this.start().count();
    }

    @Override
    public Neo4jTraversal<Edge, Edge> submit(final GraphComputer graphComputer) {
        return this.start().submit(graphComputer);
    }

    ///////////////////// TRANSFORM STEPS /////////////////////

    @Override
    public <E2> Neo4jTraversal<Edge, E2> map(final SFunction<Traverser<Edge>, E2> function) {
        return this.start().map(function);
    }

    @Override
    public <E2> Neo4jTraversal<Edge, E2> map(final SBiFunction<Traverser<Edge>, Traversal.SideEffects, E2> biFunction) {
        return this.start().map(biFunction);
    }

    @Override
    public <E2> Neo4jTraversal<Edge, E2> flatMap(final SFunction<Traverser<Edge>, Iterator<E2>> function) {
        return this.start().flatMap(function);
    }

    @Override
    public <E2> Neo4jTraversal<Edge, E2> flatMap(final SBiFunction<Traverser<Edge>, Traversal.SideEffects, Iterator<E2>> biFunction) {
        return this.start().flatMap(biFunction);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> identity() {
        return this.start().identity();
    }

    @Override
    public Neo4jTraversal<Edge, Vertex> to(final Direction direction, final int branchFactor, final String... labels) {
        return this.start().to(direction, branchFactor, labels);
    }

    @Override
    public Neo4jTraversal<Edge, Vertex> to(final Direction direction, final String... edgeLabels) {
        return this.start().to(direction, edgeLabels);
    }

    @Override
    public Neo4jTraversal<Edge, Vertex> out(final int branchFactor, final String... edgeLabels) {
        return this.start().out(branchFactor, edgeLabels);
    }

    @Override
    public Neo4jTraversal<Edge, Vertex> out(final String... edgeLabels) {
        return this.start().out(edgeLabels);
    }

    @Override
    public Neo4jTraversal<Edge, Vertex> in(final int branchFactor, final String... edgeLabels) {
        return this.start().in(branchFactor, edgeLabels);
    }

    @Override
    public Neo4jTraversal<Edge, Vertex> in(final String... edgeLabels) {
        return this.start().in(edgeLabels);
    }

    @Override
    public Neo4jTraversal<Edge, Vertex> both(final int branchFactor, final String... edgeLabels) {
        return this.start().both(branchFactor, edgeLabels);
    }

    @Override
    public Neo4jTraversal<Edge, Vertex> both(final String... edgeLabels) {
        return this.start().both(edgeLabels);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> toE(final Direction direction, final int branchFactor, final String... edgeLabels) {
        return this.start().toE(direction, branchFactor, edgeLabels);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> toE(final Direction direction, final String... edgeLabels) {
        return this.start().toE(direction, edgeLabels);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> outE(final int branchFactor, final String... edgeLabels) {
        return this.start().outE(branchFactor, edgeLabels);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> outE(final String... edgeLabels) {
        return this.start().outE(edgeLabels);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> inE(final int branchFactor, final String... edgeLabels) {
        return this.start().inE(branchFactor, edgeLabels);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> inE(final String... edgeLabels) {
        return this.start().inE(edgeLabels);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> bothE(final int branchFactor, final String... edgeLabels) {
        return this.start().bothE(branchFactor, edgeLabels);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> bothE(final String... edgeLabels) {
        return this.start().bothE(edgeLabels);
    }

    @Override
    public Neo4jTraversal<Edge, Vertex> toV(final Direction direction) {
        return this.start().toV(direction);
    }

    @Override
    public Neo4jTraversal<Edge, Vertex> inV() {
        return this.start().inV();
    }

    @Override
    public Neo4jTraversal<Edge, Vertex> outV() {
        return this.start().outV();
    }

    @Override
    public Neo4jTraversal<Edge, Vertex> bothV() {
        return this.start().bothV();
    }

    @Override
    public Neo4jTraversal<Edge, Vertex> otherV() {
        return this.start().otherV();
    }

    @Override
    public Neo4jTraversal<Edge, Edge> order() {
        return this.start().order();
    }

    @Override
    public Neo4jTraversal<Edge, Edge> order(final Comparator<Traverser<Edge>> comparator) {
        return this.start().order(comparator);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> shuffle() {
        return this.start().shuffle();
    }

    @Override
    public <E2> Neo4jTraversal<Edge, MetaProperty<E2>> metas(final String... metaPropertyKeys) {
        return this.start().metas(metaPropertyKeys);
    }

    @Override
    public <E2> Neo4jTraversal<Edge, E2> value() {
        return this.start().value();
    }

    @Override
    public <E2> Neo4jTraversal<Edge, E2> value(final String propertyKey, final Supplier<E2> defaultSupplier) {
        return this.start().value(propertyKey, defaultSupplier);
    }

    @Override
    public Neo4jTraversal<Edge, Map<String, Object>> values(final String... propertyKeys) {
        return this.start().values(propertyKeys);
    }

    @Override
    public Neo4jTraversal<Edge, Path> path(final SFunction... pathFunctions) {
        return this.start().path(pathFunctions);
    }

    @Override
    public <E2> Neo4jTraversal<Edge, E2> back(final String stepLabel) {
        return this.start().back(stepLabel);
    }

    @Override
    public <E2> Neo4jTraversal<Edge, Map<String, E2>> match(final String startLabel, final Traversal... traversals) {
        return this.start().match(startLabel, traversals);
    }

    @Override
    public <E2> Neo4jTraversal<Edge, Map<String, E2>> select(final List<String> labels, SFunction... stepFunctions) {
        return this.start().select(labels, stepFunctions);
    }

    @Override
    public <E2> Neo4jTraversal<Edge, Map<String, E2>> select(final SFunction... stepFunctions) {
        return this.start().select(stepFunctions);
    }

    @Override
    public <E2> Neo4jTraversal<Edge, E2> select(final String label, SFunction stepFunction) {
        return this.start().select(label, stepFunction);
    }

    @Override
    public <E2> Neo4jTraversal<Edge, E2> select(final String label) {
        return this.start().select(label, null);
    }

    /*public <E2> Neo4jTraversal<S, E2> union(final Traversal... traversals) {
        return (Neo4jTraversal) this.addStep(new UnionStep(this, traversals));
    }*/

    /*public <E2> Neo4jTraversal<S, E2> intersect(final Traversal... traversals) {
        return (Neo4jTraversal) this.addStep(new IntersectStep(this, traversals));
    }*/

    @Override
    public Neo4jTraversal<Edge, Edge> unfold() {
        return this.start().unfold();
    }

    @Override
    public Neo4jTraversal<Edge, List<Edge>> fold() {
        return this.start().fold();
    }

    @Override
    public <E2> Neo4jTraversal<Edge, E2> fold(final E2 seed, final SBiFunction<E2, Traverser<Edge>, E2> foldFunction) {
        return this.start().fold(seed, foldFunction);
    }

    @Override
    public <E2> Neo4jTraversal<Edge, E2> choose(final SPredicate<Traverser<Edge>> choosePredicate, final Traversal trueChoice, final Traversal falseChoice) {
        return this.start().choose(choosePredicate, trueChoice, falseChoice);
    }

    @Override
    public <E2, M> Neo4jTraversal<Edge, E2> choose(final SFunction<Traverser<Edge>, M> mapFunction, final Map<M, Traversal<Edge, E2>> choices) {
        return this.start().choose(mapFunction, choices);
    }

    ///////////////////// FILTER STEPS /////////////////////

    @Override
    public Neo4jTraversal<Edge, Edge> filter(final SPredicate<Traverser<Edge>> predicate) {
        return this.start().filter(predicate);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> filter(final SBiPredicate<Traverser<Edge>, Traversal.SideEffects> biPredicate) {
        return this.start().filter(biPredicate);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> inject(final Object... injections) {
        return this.start().inject((Edge[]) injections);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> dedup() {
        return this.start().dedup();
    }

    @Override
    public Neo4jTraversal<Edge, Edge> dedup(final SFunction<Traverser<Edge>, ?> uniqueFunction) {
        return this.start().dedup(uniqueFunction);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> except(final String sideEffectKey) {
        return this.start().except(sideEffectKey);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> except(final Object exceptionObject) {
        return this.start().except((Edge) exceptionObject);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> except(final Collection<Edge> exceptionCollection) {
        return this.start().except(exceptionCollection);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> has(final String key) {
        return this.start().has(key);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> has(final String key, final Object value) {
        return this.start().has(key, value);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> has(final String key, final T t, final Object value) {
        return this.start().has(key, t, value);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> has(final String key, final SBiPredicate predicate, final Object value) {
        return this.start().has(key, predicate, value);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> has(final String label, final String key, final Object value) {
        return this.start().has(label, key, value);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> has(final String label, final String key, final T t, final Object value) {
        return this.start().has(label, key, t, value);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> has(final String label, final String key, final SBiPredicate predicate, final Object value) {
        return this.start().has(label, key, predicate, value);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> hasNot(final String key) {
        return this.start().hasNot(key);
    }

    @Override
    public <E2> Neo4jTraversal<Edge, Map<String, E2>> where(final String firstKey, final String secondKey, final SBiPredicate predicate) {
        return this.start().where(firstKey, secondKey, predicate);
    }

    @Override
    public <E2> Neo4jTraversal<Edge, Map<String, E2>> where(final String firstKey, final SBiPredicate predicate, final String secondKey) {
        return this.start().where(firstKey, predicate, secondKey);
    }

    @Override
    public <E2> Neo4jTraversal<Edge, Map<String, E2>> where(final String firstKey, final T t, final String secondKey) {
        return this.start().where(firstKey, t, secondKey);
    }

    @Override
    public <E2> Neo4jTraversal<Edge, Map<String, E2>> where(final Traversal constraint) {
        return this.start().where(constraint);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> interval(final String key, final Comparable startValue, final Comparable endValue) {
        return this.start().interval(key, startValue, endValue);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> random(final double probability) {
        return this.start().random(probability);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> range(final int low, final int high) {
        return this.start().range(low, high);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> retain(final String sideEffectKey) {
        return this.start().retain(sideEffectKey);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> retain(final Object retainObject) {
        return this.start().retain((Edge) retainObject);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> retain(final Collection<Edge> retainCollection) {
        return this.start().retain(retainCollection);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> simplePath() {
        return this.start().simplePath();
    }

    @Override
    public Neo4jTraversal<Edge, Edge> cyclicPath() {
        return this.start().cyclicPath();
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    @Override
    public Neo4jTraversal<Edge, Edge> sideEffect(final SConsumer<Traverser<Edge>> consumer) {
        return this.start().sideEffect(consumer);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> sideEffect(final SBiConsumer<Traverser<Edge>, Traversal.SideEffects> biConsumer) {
        return this.start().sideEffect(biConsumer);
    }

    @Override
    public <E2> Neo4jTraversal<Edge, E2> cap(final String sideEffectKey) {
        return this.start().cap(sideEffectKey);
    }

    @Override
    public <E2> Neo4jTraversal<Edge, E2> cap() {
        return this.start().cap();
    }

    @Override
    public Neo4jTraversal<Edge, Edge> subgraph(final String sideEffectKey, final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(sideEffectKey, edgeIdHolder, vertexMap, includeEdge);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> subgraph(final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(null, edgeIdHolder, vertexMap, includeEdge);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> subgraph(final String sideEffectKey, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(sideEffectKey, null, null, includeEdge);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> subgraph(final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(null, null, null, includeEdge);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> aggregate(final String sideEffectKey, final SFunction<Traverser<Edge>, ?> preAggregateFunction) {
        return this.start().aggregate(sideEffectKey, preAggregateFunction);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> aggregate(final SFunction<Traverser<Edge>, ?> preAggregateFunction) {
        return this.start().aggregate(null, preAggregateFunction);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> aggregate() {
        return this.start().aggregate(null, null);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> aggregate(final String sideEffectKey) {
        return this.start().aggregate(sideEffectKey, null);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> groupBy(final String sideEffectKey, final SFunction<Traverser<Edge>, ?> keyFunction, final SFunction<Traverser<Edge>, ?> valueFunction, final SFunction<Collection, ?> reduceFunction) {
        return this.start().groupBy(sideEffectKey, keyFunction, valueFunction, reduceFunction);
    }


    @Override
    public Neo4jTraversal<Edge, Edge> groupBy(final SFunction<Traverser<Edge>, ?> keyFunction, final SFunction<Traverser<Edge>, ?> valueFunction, final SFunction<Collection, ?> reduceFunction) {
        return this.start().groupBy(null, keyFunction, valueFunction, reduceFunction);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> groupBy(final SFunction<Traverser<Edge>, ?> keyFunction, final SFunction<Traverser<Edge>, ?> valueFunction) {
        return this.start().groupBy(null, keyFunction, valueFunction, null);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> groupBy(final SFunction<Traverser<Edge>, ?> keyFunction) {
        return this.start().groupBy(null, keyFunction, null, null);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> groupBy(final String sideEffectKey, final SFunction<Traverser<Edge>, ?> keyFunction) {
        return this.start().groupBy(sideEffectKey, keyFunction, null, null);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> groupBy(final String sideEffectKey, final SFunction<Traverser<Edge>, ?> keyFunction, final SFunction<Traverser<Edge>, ?> valueFunction) {
        return this.start().groupBy(sideEffectKey, keyFunction, valueFunction, null);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> groupCount(final String sideEffectKey, final SFunction<Traverser<Edge>, ?> preGroupFunction) {
        return this.start().groupCount(sideEffectKey, preGroupFunction);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> groupCount(final SFunction<Traverser<Edge>, ?> preGroupFunction) {
        return this.start().groupCount(null, preGroupFunction);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> groupCount(final String sideEffectKey) {
        return this.start().groupCount(sideEffectKey, null);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> groupCount() {
        return this.start().groupCount(null, null);
    }

    @Override
    public Neo4jTraversal<Edge, Vertex> addE(final Direction direction, final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.start().addE(direction, edgeLabel, stepLabel, propertyKeyValues);
    }

    @Override
    public Neo4jTraversal<Edge, Vertex> addInE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.start().addInE(edgeLabel, stepLabel, propertyKeyValues);
    }

    @Override
    public Neo4jTraversal<Edge, Vertex> addOutE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.start().addOutE(edgeLabel, stepLabel, propertyKeyValues);
    }

    @Override
    public Neo4jTraversal<Edge, Vertex> addBothE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.start().addBothE(edgeLabel, stepLabel, propertyKeyValues);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> timeLimit(final long timeLimit) {
        return this.start().timeLimit(timeLimit);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> tree(final String sideEffectKey, final SFunction... branchFunctions) {
        return this.start().tree(sideEffectKey, branchFunctions);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> tree(final SFunction... branchFunctions) {
        return this.start().tree(null, branchFunctions);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> store(final String sideEffectKey, final SFunction<Traverser<Edge>, ?> preStoreFunction) {
        return this.start().store(sideEffectKey, preStoreFunction);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> store(final String sideEffectKey) {
        return this.start().store(sideEffectKey, null);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> store(final SFunction<Traverser<Edge>, ?> preStoreFunction) {
        return this.start().store(null, preStoreFunction);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> store() {
        return this.start().store(null, null);
    }

    ///////////////////// BRANCH STEPS /////////////////////

    @Override
    public Neo4jTraversal<Edge, Edge> jump(final String jumpLabel, final SPredicate<Traverser<Edge>> ifPredicate, final SPredicate<Traverser<Edge>> emitPredicate) {
        return this.start().jump(jumpLabel, ifPredicate, emitPredicate);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> jump(final String jumpLabel, final SPredicate<Traverser<Edge>> ifPredicate) {
        return this.start().jump(jumpLabel, ifPredicate);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> jump(final String jumpLabel, final int loops, final SPredicate<Traverser<Edge>> emitPredicate) {
        return this.start().jump(jumpLabel, loops, emitPredicate);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> jump(final String jumpLabel, final int loops) {
        return this.start().jump(jumpLabel, loops);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> jump(final String jumpLabel) {
        return this.start().jump(jumpLabel);
    }

    ///////////////////// UTILITY STEPS /////////////////////

    @Override
    public Neo4jTraversal<Edge, Edge> as(final String label) {
        return this.start().as(label);
    }

    @Override
    public Neo4jTraversal<Edge, Edge> with(final Object... sideEffectKeyValues) {
        return this.start().with(sideEffectKeyValues);
    }
}
