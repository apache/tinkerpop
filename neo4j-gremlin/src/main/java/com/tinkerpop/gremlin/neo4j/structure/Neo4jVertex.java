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
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import com.tinkerpop.gremlin.util.function.SBiFunction;
import com.tinkerpop.gremlin.util.function.SBiPredicate;
import com.tinkerpop.gremlin.util.function.SConsumer;
import com.tinkerpop.gremlin.util.function.SFunction;
import com.tinkerpop.gremlin.util.function.SPredicate;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
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
public class Neo4jVertex extends Neo4jElement implements Vertex, WrappedVertex<Node> {
    public Neo4jVertex(final Node node, final Neo4jGraph graph) {
        super(graph);
        this.baseElement = node;
    }

    @Override
    public void remove() {
        this.graph.tx().readWrite();

        try {
            final Node node = (Node) this.baseElement;
            for (final Relationship relationship : node.getRelationships(org.neo4j.graphdb.Direction.BOTH)) {
                relationship.delete();
            }
            node.delete();
        } catch (NotFoundException ignored) {
            // this one happens if the vertex is committed
        } catch (IllegalStateException ignored) {
            // this one happens if the vertex is still chilling in the tx
        }
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        if (label == null)
            throw Edge.Exceptions.edgeLabelCanNotBeNull();
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Edge.Exceptions.userSuppliedIdsNotSupported();

        this.graph.tx().readWrite();
        final Node node = (Node) this.baseElement;
        final Neo4jEdge edge = new Neo4jEdge(node.createRelationshipTo(((Neo4jVertex) inVertex).getBaseVertex(),
                DynamicRelationshipType.withName(label)), this.graph);
        ElementHelper.attachProperties(edge, keyValues);
        return edge;
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final int branchFactor, final String... labels) {
        this.graph.tx().readWrite();
        return (Iterator) StreamFactory.stream(Neo4jHelper.getVertices(this, direction, labels)).limit(branchFactor).iterator();
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final int branchFactor, final String... labels) {
        this.graph.tx().readWrite();
        return (Iterator) StreamFactory.stream(Neo4jHelper.getEdges(this, direction, labels)).limit(branchFactor).iterator();
    }

    public Neo4jTraversal<Vertex, Vertex> start() {
        final Neo4jTraversal<Vertex, Vertex> traversal = new DefaultNeo4jTraversal<>(this.graph);
        return (Neo4jTraversal) traversal.addStep(new StartStep<>(traversal, this));
    }

    public Node getBaseVertex() {
        return (Node) this.baseElement;
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

    //////////////////////////////////////////////////////////////////////

    public Neo4jTraversal<Vertex, Vertex> trackPaths() {
        return this.start().trackPaths();
    }

    public Neo4jTraversal<Vertex, Long> count() {
        return this.start().count();
    }

    public Neo4jTraversal<Vertex, Vertex> submit(final GraphComputer graphComputer) {
        return this.start().submit(graphComputer);
    }

    ///////////////////// TRANSFORM STEPS /////////////////////

    public <E2> Neo4jTraversal<Vertex, E2> map(final SFunction<Traverser<Vertex>, E2> function) {
        return this.start().map(function);
    }

    public <E2> Neo4jTraversal<Vertex, E2> flatMap(final SFunction<Traverser<Vertex>, Iterator<E2>> function) {
        return this.start().flatMap(function);
    }

    public Neo4jTraversal<Vertex, Vertex> identity() {
        return this.start().identity();
    }

    public Neo4jTraversal<Vertex, Vertex> to(final Direction direction, final int branchFactor, final String... edgeLabels) {
        return this.start().to(direction, branchFactor, edgeLabels);
    }

    public Neo4jTraversal<Vertex, Vertex> to(final Direction direction, final String... edgeLabels) {
        return this.start().to(direction, edgeLabels);
    }

    public Neo4jTraversal<Vertex, Vertex> out(final int branchFactor, final String... edgeLabels) {
        return this.start().out(branchFactor, edgeLabels);
    }

    public Neo4jTraversal<Vertex, Vertex> out(final String... edgeLabels) {
        return this.start().out(edgeLabels);
    }

    public Neo4jTraversal<Vertex, Vertex> in(final int branchFactor, final String... edgeLabels) {
        return this.start().in(branchFactor, edgeLabels);
    }

    public Neo4jTraversal<Vertex, Vertex> in(final String... edgeLabels) {
        return this.start().in(edgeLabels);
    }

    public Neo4jTraversal<Vertex, Vertex> both(final int branchFactor, final String... edgeLabels) {
        return this.start().both(branchFactor, edgeLabels);
    }

    public Neo4jTraversal<Vertex, Vertex> both(final String... edgeLabels) {
        return this.start().both(edgeLabels);
    }

    public Neo4jTraversal<Vertex, Edge> toE(final Direction direction, final int branchFactor, final String... edgeLabels) {
        return this.start().toE(direction, branchFactor, edgeLabels);
    }

    public Neo4jTraversal<Vertex, Edge> toE(final Direction direction, final String... edgeLabels) {
        return this.start().toE(direction, edgeLabels);
    }

    public Neo4jTraversal<Vertex, Edge> outE(final int branchFactor, final String... edgeLabels) {
        return this.start().outE(branchFactor, edgeLabels);
    }

    public Neo4jTraversal<Vertex, Edge> outE(final String... edgeLabels) {
        return this.start().outE(edgeLabels);
    }

    public Neo4jTraversal<Vertex, Edge> inE(final int branchFactor, final String... edgeLabels) {
        return this.start().inE(branchFactor, edgeLabels);
    }

    public Neo4jTraversal<Vertex, Edge> inE(final String... edgeLabels) {
        return this.start().inE(edgeLabels);
    }

    public Neo4jTraversal<Vertex, Edge> bothE(final int branchFactor, final String... edgeLabels) {
        return this.start().bothE(branchFactor, edgeLabels);
    }

    public Neo4jTraversal<Vertex, Edge> bothE(final String... edgeLabels) {
        return this.start().bothE(edgeLabels);
    }

    public Neo4jTraversal<Vertex, Vertex> toV(final Direction direction) {
        return this.start().toV(direction);
    }

    public Neo4jTraversal<Vertex, Vertex> inV() {
        return this.start().inV();
    }

    public Neo4jTraversal<Vertex, Vertex> outV() {
        return this.start().outV();
    }

    public Neo4jTraversal<Vertex, Vertex> bothV() {
        return this.start().bothV();
    }

    public Neo4jTraversal<Vertex, Vertex> otherV() {
        return this.start().otherV();
    }

    public Neo4jTraversal<Vertex, Vertex> order() {
        return this.start().order();
    }

    public Neo4jTraversal<Vertex, Vertex> order(final Comparator<Traverser<Vertex>> comparator) {
        return this.start().order(comparator);
    }

    public Neo4jTraversal<Vertex, Vertex> shuffle() {
        return this.start().shuffle();
    }

    public <E2> Neo4jTraversal<Vertex, E2> value() {
        return this.start().value();
    }

    public <E2> Neo4jTraversal<Vertex, E2> value(final String propertyKey, final Supplier<E2> defaultSupplier) {
        return this.start().value(propertyKey, defaultSupplier);
    }

    public Neo4jTraversal<Vertex, Map<String, Object>> values(final String... propertyKeys) {
        return this.start().values(propertyKeys);
    }

    public Neo4jTraversal<Vertex, Path> path(final SFunction... pathFunctions) {
        return this.start().path(pathFunctions);
    }

    public <E2> Neo4jTraversal<Vertex, E2> back(final String stepLabel) {
        return this.start().back(stepLabel);
    }

    public <E2> Neo4jTraversal<Vertex, Map<String, E2>> match(final String startLabel, final Traversal... traversals) {
        return this.start().match(startLabel, traversals);
    }

    public <E2> Neo4jTraversal<Vertex, Map<String, E2>> select(final List<String> labels, SFunction... stepFunctions) {
        return this.start().select(labels, stepFunctions);
    }

    public <E2> Neo4jTraversal<Vertex, Map<String, E2>> select(final SFunction... stepFunctions) {
        return this.start().select(stepFunctions);
    }

    public <E2> Neo4jTraversal<Vertex, E2> select(final String label, SFunction stepFunction) {
        return this.start().select(label, stepFunction);
    }

    public <E2> Neo4jTraversal<Vertex, E2> select(final String label) {
        return this.start().select(label, null);
    }

    /*public <E2> Neo4jTraversal<S, E2> union(final Traversal... traversals) {
        return (Neo4jTraversal) this.addStep(new UnionStep(this, traversals));
    }*/

    /*public <E2> Neo4jTraversal<S, E2> intersect(final Traversal... traversals) {
        return (Neo4jTraversal) this.addStep(new IntersectStep(this, traversals));
    }*/

    public Neo4jTraversal<Vertex, Vertex> unfold() {
        return this.start().unfold();
    }

    public Neo4jTraversal<Vertex, List<Vertex>> fold() {
        return this.start().fold();
    }

    public <E2> Neo4jTraversal<Vertex, E2> fold(final E2 seed, final SBiFunction<E2, Vertex, E2> foldFunction) {
        return this.start().fold(seed, foldFunction);
    }

    public <E2> Neo4jTraversal<Vertex, E2> choose(final SPredicate<Traverser<Vertex>> choosePredicate, final Traversal trueChoice, final Traversal falseChoice) {
        return this.start().choose(choosePredicate, trueChoice, falseChoice);
    }

    public <E2, M> Neo4jTraversal<Vertex, E2> choose(final SFunction<Traverser<Vertex>, M> mapFunction, final Map<M, Traversal<Vertex, E2>> choices) {
        return this.start().choose(mapFunction, choices);
    }

    ///////////////////// FILTER STEPS /////////////////////

    public Neo4jTraversal<Vertex, Vertex> filter(final SPredicate<Traverser<Vertex>> predicate) {
        return this.start().filter(predicate);
    }

    public Neo4jTraversal<Vertex, Vertex> inject(final Object... injections) {
        return this.start().inject((Vertex[]) injections);
    }

    public Neo4jTraversal<Vertex, Vertex> dedup() {
        return this.start().dedup();
    }

    public Neo4jTraversal<Vertex, Vertex> dedup(final SFunction<Vertex, ?> uniqueFunction) {
        return this.start().dedup(uniqueFunction);
    }

    public Neo4jTraversal<Vertex, Vertex> except(final String sideEffectKey) {
        return this.start().except(sideEffectKey);
    }

    public Neo4jTraversal<Vertex, Vertex> except(final Object exceptionObject) {
        return this.start().except((Vertex) exceptionObject);
    }

    public Neo4jTraversal<Vertex, Vertex> except(final Collection<Vertex> exceptionCollection) {
        return this.start().except(exceptionCollection);
    }

    public Neo4jTraversal<Vertex, Vertex> has(final String key) {
        return this.start().has(key);
    }

    public Neo4jTraversal<Vertex, Vertex> has(final String key, final Object value) {
        return this.start().has(key, value);
    }

    public Neo4jTraversal<Vertex, Vertex> has(final String key, final T t, final Object value) {
        return this.start().has(key, t, value);
    }

    public Neo4jTraversal<Vertex, Vertex> has(final String key, final SBiPredicate predicate, final Object value) {
        return this.start().has(key, predicate, value);
    }

    public Neo4jTraversal<Vertex, Vertex> hasNot(final String key) {
        return this.start().hasNot(key);
    }

    public <E2> Neo4jTraversal<Vertex, Map<String, E2>> where(final String firstKey, final String secondKey, final SBiPredicate predicate) {
        return this.start().where(firstKey, secondKey, predicate);
    }

    public <E2> Neo4jTraversal<Vertex, Map<String, E2>> where(final String firstKey, final SBiPredicate predicate, final String secondKey) {
        return this.start().where(firstKey, predicate, secondKey);
    }

    public <E2> Neo4jTraversal<Vertex, Map<String, E2>> where(final String firstKey, final T t, final String secondKey) {
        return this.start().where(firstKey, t, secondKey);
    }

    public <E2> Neo4jTraversal<Vertex, Map<String, E2>> where(final Traversal constraint) {
        return this.start().where(constraint);
    }

    public Neo4jTraversal<Vertex, Vertex> interval(final String key, final Comparable startValue, final Comparable endValue) {
        return this.start().interval(key, startValue, endValue);
    }

    public Neo4jTraversal<Vertex, Vertex> random(final double probability) {
        return this.start().random(probability);
    }

    public Neo4jTraversal<Vertex, Vertex> range(final int low, final int high) {
        return this.start().range(low, high);
    }

    public Neo4jTraversal<Vertex, Vertex> retain(final String sideEffectKey) {
        return this.start().retain(sideEffectKey);
    }

    public Neo4jTraversal<Vertex, Vertex> retain(final Object retainObject) {
        return this.start().retain((Vertex) retainObject);
    }

    public Neo4jTraversal<Vertex, Vertex> retain(final Collection<Vertex> retainCollection) {
        return this.start().retain(retainCollection);
    }

    public Neo4jTraversal<Vertex, Vertex> simplePath() {
        return this.start().simplePath();
    }

    public Neo4jTraversal<Vertex, Vertex> cyclicPath() {
        return this.start().cyclicPath();
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public Neo4jTraversal<Vertex, Vertex> sideEffect(final SConsumer<Traverser<Vertex>> consumer) {
        return this.start().sideEffect(consumer);
    }

    public <E2> Neo4jTraversal<Vertex, E2> cap(final String sideEffectKey) {
        return this.start().cap(sideEffectKey);
    }

    public <E2> Neo4jTraversal<Vertex, E2> cap() {
        return this.start().cap();
    }

    public Neo4jTraversal<Vertex, Vertex> subgraph(final String sideEffectKey, final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(sideEffectKey, edgeIdHolder, vertexMap, includeEdge);
    }

    public Neo4jTraversal<Vertex, Vertex> subgraph(final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(null, edgeIdHolder, vertexMap, includeEdge);
    }

    public Neo4jTraversal<Vertex, Vertex> subgraph(final String sideEffectKey, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(sideEffectKey, null, null, includeEdge);
    }

    public Neo4jTraversal<Vertex, Vertex> subgraph(final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(null, null, null, includeEdge);
    }

    public Neo4jTraversal<Vertex, Vertex> aggregate(final String sideEffectKey, final SFunction<Vertex, ?> preAggregateFunction) {
        return this.start().aggregate(sideEffectKey, preAggregateFunction);
    }

    public Neo4jTraversal<Vertex, Vertex> aggregate(final SFunction<Vertex, ?> preAggregateFunction) {
        return this.start().aggregate(null, preAggregateFunction);
    }

    public Neo4jTraversal<Vertex, Vertex> aggregate() {
        return this.start().aggregate(null, null);
    }

    public Neo4jTraversal<Vertex, Vertex> aggregate(final String sideEffectKey) {
        return this.start().aggregate(sideEffectKey, null);
    }

    public Neo4jTraversal<Vertex, Vertex> groupBy(final String sideEffectKey, final SFunction<Vertex, ?> keyFunction, final SFunction<Vertex, ?> valueFunction, final SFunction<Collection, ?> reduceFunction) {
        return this.start().groupBy(sideEffectKey, keyFunction, valueFunction, reduceFunction);
    }


    public Neo4jTraversal<Vertex, Vertex> groupBy(final SFunction<Vertex, ?> keyFunction, final SFunction<Vertex, ?> valueFunction, final SFunction<Collection, ?> reduceFunction) {
        return this.start().groupBy(null, keyFunction, valueFunction, reduceFunction);
    }

    public Neo4jTraversal<Vertex, Vertex> groupBy(final SFunction<Vertex, ?> keyFunction, final SFunction<Vertex, ?> valueFunction) {
        return this.start().groupBy(null, keyFunction, valueFunction, null);
    }

    public Neo4jTraversal<Vertex, Vertex> groupBy(final SFunction<Vertex, ?> keyFunction) {
        return this.start().groupBy(null, keyFunction, null, null);
    }

    public Neo4jTraversal<Vertex, Vertex> groupBy(final String sideEffectKey, final SFunction<Vertex, ?> keyFunction) {
        return this.start().groupBy(sideEffectKey, keyFunction, null, null);
    }

    public Neo4jTraversal<Vertex, Vertex> groupBy(final String sideEffectKey, final SFunction<Vertex, ?> keyFunction, final SFunction<Vertex, ?> valueFunction) {
        return this.start().groupBy(sideEffectKey, keyFunction, valueFunction, null);
    }

    public Neo4jTraversal<Vertex, Vertex> groupCount(final String sideEffectKey, final SFunction<Vertex, ?> preGroupFunction) {
        return this.start().groupCount(sideEffectKey, preGroupFunction);
    }

    public Neo4jTraversal<Vertex, Vertex> groupCount(final SFunction<Vertex, ?> preGroupFunction) {
        return this.start().groupCount(null, preGroupFunction);
    }

    public Neo4jTraversal<Vertex, Vertex> groupCount(final String sideEffectKey) {
        return this.start().groupCount(sideEffectKey, null);
    }

    public Neo4jTraversal<Vertex, Vertex> groupCount() {
        return this.start().groupCount(null, null);
    }

    public Neo4jTraversal<Vertex, Vertex> addE(final Direction direction, final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.start().addE(direction, edgeLabel, stepLabel, propertyKeyValues);
    }

    public Neo4jTraversal<Vertex, Vertex> addInE(final String edgeLabel, final String setLabel, final Object... propertyKeyValues) {
        return this.start().addInE(edgeLabel, setLabel, propertyKeyValues);
    }

    public Neo4jTraversal<Vertex, Vertex> addOutE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.start().addOutE(edgeLabel, stepLabel, propertyKeyValues);
    }

    public Neo4jTraversal<Vertex, Vertex> addBothE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.start().addBothE(edgeLabel, stepLabel, propertyKeyValues);
    }

    public Neo4jTraversal<Vertex, Vertex> timeLimit(final long timeLimit) {
        return this.start().timeLimit(timeLimit);
    }

    public Neo4jTraversal<Vertex, Vertex> tree(final String sideEffectKey, final SFunction... branchFunctions) {
        return this.start().tree(sideEffectKey, branchFunctions);
    }

    public Neo4jTraversal<Vertex, Vertex> tree(final SFunction... branchFunctions) {
        return this.start().tree(null, branchFunctions);
    }

    public Neo4jTraversal<Vertex, Vertex> store(final String sideEffectKey, final SFunction<Vertex, ?> preStoreFunction) {
        return this.start().store(sideEffectKey, preStoreFunction);
    }

    public Neo4jTraversal<Vertex, Vertex> store(final String sideEffectKey) {
        return this.start().store(sideEffectKey, null);
    }

    public Neo4jTraversal<Vertex, Vertex> store(final SFunction<Vertex, ?> preStoreFunction) {
        return this.start().store(null, preStoreFunction);
    }

    public Neo4jTraversal<Vertex, Vertex> store() {
        return this.start().store(null, null);
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public Neo4jTraversal<Vertex, Vertex> jump(final String jumpLabel, final SPredicate<Traverser<Vertex>> ifPredicate, final SPredicate<Traverser<Vertex>> emitPredicate) {
        return this.start().jump(jumpLabel, ifPredicate, emitPredicate);
    }

    public Neo4jTraversal<Vertex, Vertex> jump(final String jumpLabel, final SPredicate<Traverser<Vertex>> ifPredicate) {
        return this.start().jump(jumpLabel, ifPredicate);
    }

    public Neo4jTraversal<Vertex, Vertex> jump(final String jumpLabel, final int loops, final SPredicate<Traverser<Vertex>> emitPredicate) {
        return this.start().jump(jumpLabel, loops, emitPredicate);
    }

    public Neo4jTraversal<Vertex, Vertex> jump(final String jumpLabel, final int loops) {
        return this.start().jump(jumpLabel, loops);
    }

    public Neo4jTraversal<Vertex, Vertex> jump(final String jumpLabel) {
        return this.start().jump(jumpLabel);
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public Neo4jTraversal<Vertex, Vertex> as(final String label) {
        return this.start().as(label);
    }

    public Neo4jTraversal<Vertex, Vertex> with(final Object... sideEffectKeyValues) {
        return this.start().with(sideEffectKeyValues);
    }

}
