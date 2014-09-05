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
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import com.tinkerpop.gremlin.util.function.SBiConsumer;
import com.tinkerpop.gremlin.util.function.SBiFunction;
import com.tinkerpop.gremlin.util.function.SBiPredicate;
import com.tinkerpop.gremlin.util.function.SConsumer;
import com.tinkerpop.gremlin.util.function.SFunction;
import com.tinkerpop.gremlin.util.function.SPredicate;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;

import java.util.Arrays;
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
    public <V> MetaProperty<V> property(final String key) {
        this.graph.tx().readWrite();
        if (this.getBaseVertex().hasProperty(key)) {
            if (this.getBaseVertex().getProperty(key).equals(Neo4jMetaProperty.META_PROPERTY_TOKEN)) {
                if (this.getBaseVertex().getDegree(DynamicRelationshipType.withName(Neo4jMetaProperty.META_PROPERTY_PREFIX.concat(key)), org.neo4j.graphdb.Direction.OUTGOING) > 1)
                    throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
                else
                    return new Neo4jMetaProperty<>(this, this.getBaseVertex().getRelationships(org.neo4j.graphdb.Direction.OUTGOING, DynamicRelationshipType.withName(Neo4jMetaProperty.META_PROPERTY_PREFIX.concat(key))).iterator().next().getEndNode());
            } else {
                return new Neo4jMetaProperty<>(this, key, (V) this.baseElement.getProperty(key));
            }
        } else
            return MetaProperty.<V>empty();
    }

    @Override
    public <V> MetaProperty<V> property(final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        this.graph.tx().readWrite();
        try {
            final String prefixedKey = Neo4jMetaProperty.META_PROPERTY_PREFIX.concat(key);
            if (this.getBaseVertex().hasProperty(key)) {
                if (this.getBaseVertex().getProperty(key).equals(Neo4jMetaProperty.META_PROPERTY_TOKEN)) {
                    final Node node = this.graph.getBaseGraph().createNode(Neo4jMetaProperty.META_PROPERTY_LABEL);
                    node.setProperty(Neo4jMetaProperty.META_PROPERTY_KEY, key);
                    node.setProperty(Neo4jMetaProperty.META_PROPERTY_VALUE, value);
                    this.getBaseVertex().createRelationshipTo(node, DynamicRelationshipType.withName(prefixedKey));
                    return new Neo4jMetaProperty<>(this, node);
                } else {
                    Node node = this.graph.getBaseGraph().createNode(Neo4jMetaProperty.META_PROPERTY_LABEL);
                    node.setProperty(Neo4jMetaProperty.META_PROPERTY_KEY, key);
                    node.setProperty(Neo4jMetaProperty.META_PROPERTY_VALUE, this.getBaseVertex().removeProperty(key));
                    this.getBaseVertex().createRelationshipTo(node, DynamicRelationshipType.withName(prefixedKey));
                    this.baseElement.setProperty(key, Neo4jMetaProperty.META_PROPERTY_TOKEN);
                    node = this.graph.getBaseGraph().createNode(Neo4jMetaProperty.META_PROPERTY_LABEL);
                    node.setProperty(Neo4jMetaProperty.META_PROPERTY_KEY, key);
                    node.setProperty(Neo4jMetaProperty.META_PROPERTY_VALUE, value);
                    this.getBaseVertex().createRelationshipTo(node, DynamicRelationshipType.withName(prefixedKey));
                    return new Neo4jMetaProperty<>(this, node);
                }
            } else {
                this.getBaseVertex().setProperty(key, value);
                return new Neo4jMetaProperty<>(this, key, value);
            }
        } catch (IllegalArgumentException iae) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
        }
    }

    @Override
    public void remove() {
        this.graph.tx().readWrite();
        try {
            final Node node = this.getBaseVertex();
            for (final Relationship relationship : node.getRelationships(org.neo4j.graphdb.Direction.BOTH)) {
                final Node otherNode = relationship.getOtherNode(node);
                if (otherNode.getLabels().iterator().next().equals(Neo4jMetaProperty.META_PROPERTY_LABEL)) {
                    relationship.getOtherNode(node).getRelationships().forEach(Relationship::delete);
                    otherNode.delete(); // meta property node
                } else
                    relationship.delete();
            }
            node.delete();
        } catch (final NotFoundException ignored) {
            // this one happens if the vertex is committed
        } catch (final IllegalStateException ignored) {
            // this one happens if the vertex is still chilling in the tx
        }
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        if (label == null)
            throw Edge.Exceptions.edgeLabelCanNotBeNull();
        if (label.startsWith(Neo4jMetaProperty.META_PROPERTY_PREFIX))
            throw new IllegalArgumentException("The edge label prefix " + Neo4jMetaProperty.META_PROPERTY_PREFIX + " is reserved for Neo4jMetaProperties.");

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
    public Neo4jTraversal<Vertex, Vertex> start() {
        final Neo4jTraversal<Vertex, Vertex> traversal = new DefaultNeo4jTraversal<>(this.graph);
        return (Neo4jTraversal) traversal.addStep(new StartStep<>(traversal, this));
    }

    @Override
    public Node getBaseVertex() {
        return (Node) this.baseElement;
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Vertex.Iterators iterators() {
        return this.iterators;
    }

    private final Vertex.Iterators iterators = new Iterators(this);

    protected class Iterators extends Neo4jElement.Iterators implements Vertex.Iterators {

        public Iterators(final Neo4jVertex vertex) {
            super(vertex);
        }

        @Override
        public Iterator<Vertex> vertices(final Direction direction, final int branchFactor, final String... labels) {
            graph.tx().readWrite();
            return (Iterator) StreamFactory.stream(Neo4jHelper.getVertices((Neo4jVertex) this.element, direction, labels)).limit(branchFactor).iterator();
        }

        @Override
        public Iterator<Edge> edges(final Direction direction, final int branchFactor, final String... labels) {
            graph.tx().readWrite();
            return (Iterator) StreamFactory.stream(Neo4jHelper.getEdges((Neo4jVertex) this.element, direction, labels)).limit(branchFactor).iterator();
        }

        @Override
        public <V> Iterator<MetaProperty<V>> properties(final String... propertyKeys) {
            graph.tx().readWrite();
            return (Iterator) StreamFactory.stream(getBaseVertex().getPropertyKeys())
                    .filter(key -> propertyKeys.length == 0 || Arrays.binarySearch(propertyKeys, key) >= 0)
                    .filter(key -> !Graph.Key.isHidden(key))
                    .flatMap(key -> {
                        if (getBaseVertex().getProperty(key).equals(Neo4jMetaProperty.META_PROPERTY_TOKEN))
                            return StreamFactory.stream(getBaseVertex().getRelationships(org.neo4j.graphdb.Direction.OUTGOING, DynamicRelationshipType.withName(Neo4jMetaProperty.META_PROPERTY_PREFIX.concat(key))))
                                    .map(relationship -> new Neo4jMetaProperty((Neo4jVertex) this.element, relationship.getEndNode()));
                        else
                            return Arrays.asList(new Neo4jMetaProperty<>((Neo4jVertex) this.element, key, (V) getBaseVertex().getProperty(key))).stream();
                    }).iterator();
        }

        @Override
        public <V> Iterator<MetaProperty<V>> hiddens(final String... propertyKeys) {
            graph.tx().readWrite();
            return (Iterator) StreamFactory.stream(baseElement.getPropertyKeys())
                    .filter(key -> propertyKeys.length == 0 || Arrays.binarySearch(propertyKeys, key) >= 0)
                    .filter(Graph.Key::isHidden)
                    .map(key -> new Neo4jMetaProperty<>((Neo4jVertex) this.element, key, (V) baseElement.getProperty(key))).iterator();
        }
    }


    public Neo4jTraversal<Vertex, Vertex> trackPaths() {
        return this.start().trackPaths();
    }

    @Override
    public Neo4jTraversal<Vertex, Long> count() {
        return this.start().count();
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> submit(final GraphComputer graphComputer) {
        return this.start().submit(graphComputer);
    }

    ///////////////////// TRANSFORM STEPS /////////////////////

    @Override
    public <E2> Neo4jTraversal<Vertex, E2> map(final SFunction<Traverser<Vertex>, E2> function) {
        return this.start().map(function);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, E2> map(final SBiFunction<Traverser<Vertex>, Traversal.SideEffects, E2> biFunction) {
        return this.start().map(biFunction);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, E2> flatMap(final SFunction<Traverser<Vertex>, Iterator<E2>> function) {
        return this.start().flatMap(function);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, E2> flatMap(final SBiFunction<Traverser<Vertex>, Traversal.SideEffects, Iterator<E2>> biFunction) {
        return this.start().flatMap(biFunction);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> identity() {
        return this.start().identity();
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> to(final Direction direction, final int branchFactor, final String... edgeLabels) {
        return this.start().to(direction, branchFactor, edgeLabels);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> to(final Direction direction, final String... edgeLabels) {
        return this.start().to(direction, edgeLabels);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> out(final int branchFactor, final String... edgeLabels) {
        return this.start().out(branchFactor, edgeLabels);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> out(final String... edgeLabels) {
        return this.start().out(edgeLabels);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> in(final int branchFactor, final String... edgeLabels) {
        return this.start().in(branchFactor, edgeLabels);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> in(final String... edgeLabels) {
        return this.start().in(edgeLabels);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> both(final int branchFactor, final String... edgeLabels) {
        return this.start().both(branchFactor, edgeLabels);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> both(final String... edgeLabels) {
        return this.start().both(edgeLabels);
    }

    @Override
    public Neo4jTraversal<Vertex, Edge> toE(final Direction direction, final int branchFactor, final String... edgeLabels) {
        return this.start().toE(direction, branchFactor, edgeLabels);
    }

    @Override
    public Neo4jTraversal<Vertex, Edge> toE(final Direction direction, final String... edgeLabels) {
        return this.start().toE(direction, edgeLabels);
    }

    @Override
    public Neo4jTraversal<Vertex, Edge> outE(final int branchFactor, final String... edgeLabels) {
        return this.start().outE(branchFactor, edgeLabels);
    }

    @Override
    public Neo4jTraversal<Vertex, Edge> outE(final String... edgeLabels) {
        return this.start().outE(edgeLabels);
    }

    @Override
    public Neo4jTraversal<Vertex, Edge> inE(final int branchFactor, final String... edgeLabels) {
        return this.start().inE(branchFactor, edgeLabels);
    }

    @Override
    public Neo4jTraversal<Vertex, Edge> inE(final String... edgeLabels) {
        return this.start().inE(edgeLabels);
    }

    @Override
    public Neo4jTraversal<Vertex, Edge> bothE(final int branchFactor, final String... edgeLabels) {
        return this.start().bothE(branchFactor, edgeLabels);
    }

    @Override
    public Neo4jTraversal<Vertex, Edge> bothE(final String... edgeLabels) {
        return this.start().bothE(edgeLabels);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> toV(final Direction direction) {
        return this.start().toV(direction);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> inV() {
        return this.start().inV();
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> outV() {
        return this.start().outV();
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> bothV() {
        return this.start().bothV();
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> otherV() {
        return this.start().otherV();
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> order() {
        return this.start().order();
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> order(final Comparator<Traverser<Vertex>> comparator) {
        return this.start().order(comparator);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> shuffle() {
        return this.start().shuffle();
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, MetaProperty<E2>> properties(final String... propertyKeys) {
        return (Neo4jTraversal) this.start().properties(propertyKeys);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, Map<String, List<MetaProperty<E2>>>> propertyMap(final String... propertyKeys) {
        return (Neo4jTraversal) this.start().propertyMap(propertyKeys);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, MetaProperty<E2>> hiddens(final String... propertyKeys) {
        return (Neo4jTraversal) this.start().hiddens(propertyKeys);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, Map<String, List<E2>>> hiddenValueMap(final String... propertyKeys) {
        return this.start().hiddenValueMap(propertyKeys);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, Map<String, MetaProperty<E2>>> hiddenMap(final String... propertyKeys) {
        return (Neo4jTraversal) this.start().hiddenMap(propertyKeys);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, E2> hiddenValue(final String propertyKey) {
        return this.start().hiddenValue(propertyKey);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, E2> hiddenValue(final String propertyKey, final E2 defaultValue) {
        return this.start().hiddenValue(propertyKey, defaultValue);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, E2> hiddenValue(final String propertyKey, final Supplier<E2> defaultSupplier) {
        return this.start().hiddenValue(propertyKey, defaultSupplier);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, E2> value(final String propertyKey, final Supplier<E2> defaultSupplier) {
        return this.start().value(propertyKey, defaultSupplier);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, E2> value() {
        return this.start().value();
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, Map<String, List<E2>>> valueMap(final String... propertyKeys) {
        return this.start().valueMap(propertyKeys);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, E2> values(final String... propertyKeys) {
        return this.start().values(propertyKeys);
    }

    @Override
    public Neo4jTraversal<Vertex, Path> path(final SFunction... pathFunctions) {
        return this.start().path(pathFunctions);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, E2> back(final String stepLabel) {
        return this.start().back(stepLabel);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, Map<String, E2>> match(final String startLabel, final Traversal... traversals) {
        return this.start().match(startLabel, traversals);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, Map<String, E2>> select(final List<String> labels, SFunction... stepFunctions) {
        return this.start().select(labels, stepFunctions);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, Map<String, E2>> select(final SFunction... stepFunctions) {
        return this.start().select(stepFunctions);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, E2> select(final String label, SFunction stepFunction) {
        return this.start().select(label, stepFunction);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, E2> select(final String label) {
        return this.start().select(label, null);
    }

    /*public <E2> Neo4jTraversal<S, E2> union(final Traversal... traversals) {
        return (Neo4jTraversal) this.addStep(new UnionStep(this, traversals));
    }*/

    /*public <E2> Neo4jTraversal<S, E2> intersect(final Traversal... traversals) {
        return (Neo4jTraversal) this.addStep(new IntersectStep(this, traversals));
    }*/

    @Override
    public Neo4jTraversal<Vertex, Vertex> unfold() {
        return this.start().unfold();
    }

    @Override
    public Neo4jTraversal<Vertex, List<Vertex>> fold() {
        return this.start().fold();
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, E2> fold(final E2 seed, final SBiFunction<E2, Traverser<Vertex>, E2> foldFunction) {
        return this.start().fold(seed, foldFunction);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, E2> choose(final SPredicate<Traverser<Vertex>> choosePredicate, final Traversal trueChoice, final Traversal falseChoice) {
        return this.start().choose(choosePredicate, trueChoice, falseChoice);
    }

    @Override
    public <E2, M> Neo4jTraversal<Vertex, E2> choose(final SFunction<Traverser<Vertex>, M> mapFunction, final Map<M, Traversal<Vertex, E2>> choices) {
        return this.start().choose(mapFunction, choices);
    }

    ///////////////////// FILTER STEPS /////////////////////

    @Override
    public Neo4jTraversal<Vertex, Vertex> filter(final SPredicate<Traverser<Vertex>> predicate) {
        return this.start().filter(predicate);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> filter(final SBiPredicate<Traverser<Vertex>, Traversal.SideEffects> biPredicate) {
        return this.start().filter(biPredicate);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> inject(final Object... injections) {
        return this.start().inject((Vertex[]) injections);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> dedup() {
        return this.start().dedup();
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> dedup(final SFunction<Traverser<Vertex>, ?> uniqueFunction) {
        return this.start().dedup(uniqueFunction);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> except(final String sideEffectKey) {
        return this.start().except(sideEffectKey);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> except(final Object exceptionObject) {
        return this.start().except((Vertex) exceptionObject);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> except(final Collection<Vertex> exceptionCollection) {
        return this.start().except(exceptionCollection);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> has(final String key) {
        return this.start().has(key);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> has(final String key, final Object value) {
        return this.start().has(key, value);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> has(final String key, final T t, final Object value) {
        return this.start().has(key, t, value);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> has(final String key, final SBiPredicate predicate, final Object value) {
        return this.start().has(key, predicate, value);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> has(final String label, final String key, final Object value) {
        return this.start().has(label, key, value);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> has(final String label, final String key, final T t, final Object value) {
        return this.start().has(label, key, t, value);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> has(final String label, final String key, final SBiPredicate predicate, final Object value) {
        return this.start().has(label, key, predicate, value);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> hasNot(final String key) {
        return this.start().hasNot(key);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, Map<String, E2>> where(final String firstKey, final String secondKey, final SBiPredicate predicate) {
        return this.start().where(firstKey, secondKey, predicate);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, Map<String, E2>> where(final String firstKey, final SBiPredicate predicate, final String secondKey) {
        return this.start().where(firstKey, predicate, secondKey);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, Map<String, E2>> where(final String firstKey, final T t, final String secondKey) {
        return this.start().where(firstKey, t, secondKey);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, Map<String, E2>> where(final Traversal constraint) {
        return this.start().where(constraint);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> interval(final String key, final Comparable startValue, final Comparable endValue) {
        return this.start().interval(key, startValue, endValue);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> random(final double probability) {
        return this.start().random(probability);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> range(final int low, final int high) {
        return this.start().range(low, high);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> retain(final String sideEffectKey) {
        return this.start().retain(sideEffectKey);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> retain(final Object retainObject) {
        return this.start().retain((Vertex) retainObject);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> retain(final Collection<Vertex> retainCollection) {
        return this.start().retain(retainCollection);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> simplePath() {
        return this.start().simplePath();
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> cyclicPath() {
        return this.start().cyclicPath();
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    @Override
    public Neo4jTraversal<Vertex, Vertex> sideEffect(final SConsumer<Traverser<Vertex>> consumer) {
        return this.start().sideEffect(consumer);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> sideEffect(final SBiConsumer<Traverser<Vertex>, Traversal.SideEffects> biConsumer) {
        return this.start().sideEffect(biConsumer);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, E2> cap(final String sideEffectKey) {
        return this.start().cap(sideEffectKey);
    }

    @Override
    public <E2> Neo4jTraversal<Vertex, E2> cap() {
        return this.start().cap();
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> subgraph(final String sideEffectKey, final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(sideEffectKey, edgeIdHolder, vertexMap, includeEdge);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> subgraph(final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(null, edgeIdHolder, vertexMap, includeEdge);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> subgraph(final String sideEffectKey, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(sideEffectKey, null, null, includeEdge);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> subgraph(final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(null, null, null, includeEdge);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> aggregate(final String sideEffectKey, final SFunction<Traverser<Vertex>, ?> preAggregateFunction) {
        return this.start().aggregate(sideEffectKey, preAggregateFunction);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> aggregate(final SFunction<Traverser<Vertex>, ?> preAggregateFunction) {
        return this.start().aggregate(null, preAggregateFunction);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> aggregate() {
        return this.start().aggregate(null, null);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> aggregate(final String sideEffectKey) {
        return this.start().aggregate(sideEffectKey, null);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> groupBy(final String sideEffectKey, final SFunction<Traverser<Vertex>, ?> keyFunction, final SFunction<Traverser<Vertex>, ?> valueFunction, final SFunction<Collection, ?> reduceFunction) {
        return this.start().groupBy(sideEffectKey, keyFunction, valueFunction, reduceFunction);
    }


    @Override
    public Neo4jTraversal<Vertex, Vertex> groupBy(final SFunction<Traverser<Vertex>, ?> keyFunction, final SFunction<Traverser<Vertex>, ?> valueFunction, final SFunction<Collection, ?> reduceFunction) {
        return this.start().groupBy(null, keyFunction, valueFunction, reduceFunction);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> groupBy(final SFunction<Traverser<Vertex>, ?> keyFunction, final SFunction<Traverser<Vertex>, ?> valueFunction) {
        return this.start().groupBy(null, keyFunction, valueFunction, null);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> groupBy(final SFunction<Traverser<Vertex>, ?> keyFunction) {
        return this.start().groupBy(null, keyFunction, null, null);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> groupBy(final String sideEffectKey, final SFunction<Traverser<Vertex>, ?> keyFunction) {
        return this.start().groupBy(sideEffectKey, keyFunction, null, null);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> groupBy(final String sideEffectKey, final SFunction<Traverser<Vertex>, ?> keyFunction, final SFunction<Traverser<Vertex>, ?> valueFunction) {
        return this.start().groupBy(sideEffectKey, keyFunction, valueFunction, null);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> groupCount(final String sideEffectKey, final SFunction<Traverser<Vertex>, ?> preGroupFunction) {
        return this.start().groupCount(sideEffectKey, preGroupFunction);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> groupCount(final SFunction<Traverser<Vertex>, ?> preGroupFunction) {
        return this.start().groupCount(null, preGroupFunction);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> groupCount(final String sideEffectKey) {
        return this.start().groupCount(sideEffectKey, null);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> groupCount() {
        return this.start().groupCount(null, null);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> addE(final Direction direction, final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.start().addE(direction, edgeLabel, stepLabel, propertyKeyValues);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> addInE(final String edgeLabel, final String setLabel, final Object... propertyKeyValues) {
        return this.start().addInE(edgeLabel, setLabel, propertyKeyValues);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> addOutE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.start().addOutE(edgeLabel, stepLabel, propertyKeyValues);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> addBothE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.start().addBothE(edgeLabel, stepLabel, propertyKeyValues);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> timeLimit(final long timeLimit) {
        return this.start().timeLimit(timeLimit);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> tree(final String sideEffectKey, final SFunction... branchFunctions) {
        return this.start().tree(sideEffectKey, branchFunctions);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> tree(final SFunction... branchFunctions) {
        return this.start().tree(null, branchFunctions);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> store(final String sideEffectKey, final SFunction<Traverser<Vertex>, ?> preStoreFunction) {
        return this.start().store(sideEffectKey, preStoreFunction);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> store(final String sideEffectKey) {
        return this.start().store(sideEffectKey, null);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> store(final SFunction<Traverser<Vertex>, ?> preStoreFunction) {
        return this.start().store(null, preStoreFunction);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> store() {
        return this.start().store(null, null);
    }

    ///////////////////// BRANCH STEPS /////////////////////

    @Override
    public Neo4jTraversal<Vertex, Vertex> jump(final String jumpLabel, final SPredicate<Traverser<Vertex>> ifPredicate, final SPredicate<Traverser<Vertex>> emitPredicate) {
        return this.start().jump(jumpLabel, ifPredicate, emitPredicate);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> jump(final String jumpLabel, final SPredicate<Traverser<Vertex>> ifPredicate) {
        return this.start().jump(jumpLabel, ifPredicate);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> jump(final String jumpLabel, final int loops, final SPredicate<Traverser<Vertex>> emitPredicate) {
        return this.start().jump(jumpLabel, loops, emitPredicate);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> jump(final String jumpLabel, final int loops) {
        return this.start().jump(jumpLabel, loops);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> jump(final String jumpLabel) {
        return this.start().jump(jumpLabel);
    }

    ///////////////////// UTILITY STEPS /////////////////////

    @Override
    public Neo4jTraversal<Vertex, Vertex> as(final String label) {
        return this.start().as(label);
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> with(final Object... sideEffectKeyValues) {
        return this.start().with(sideEffectKeyValues);
    }

}
