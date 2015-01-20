////// THIS CLASS IS AUTO-GENERATED, DO NOT EDIT
////// TO ADD METHODS TO THIS CLASS, EDIT Neo4jGraphTraversalStub

package com.tinkerpop.gremlin.neo4j.process.graph;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Element;

import java.util.HashMap;
import java.util.Map;

/**
 * Neo4jGraphTraversal is merged with {@link GraphTraversal} via the Maven exec-plugin.
 * The Maven plugin yields Neo4jGraphTraversal which is ultimately what is depended on by user source.
 * This class maintains {@link Neo4jGraphTraversal} specific methods that extend {@link GraphTraversal}.
 * Adding {@link Element} to the JavaDoc so it sticks in the import during "optimize imports" code cleaning.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Neo4jGraphTraversal<S, E> extends GraphTraversal.Admin<S, E>, GraphTraversal<S, E> {

    @Override
    public default <E2> Neo4jGraphTraversal<S, E2> addStep(final Step<?, E2> step) {
        return (Neo4jGraphTraversal) GraphTraversal.Admin.super.addStep((Step) step);
    }

    public default <E2> Neo4jGraphTraversal<S, Map<String, E2>> cypher(final String query) {
        return this.cypher(query, new HashMap<>());
    }

    public <E2> Neo4jGraphTraversal<S, Map<String, E2>> cypher(final String query, final Map<String, Object> parameters);

	///////////////////////////////////////////////////////////////////////////////////
	//// METHODS INHERITED FROM com.tinkerpop.gremlin.process.graph.GraphTraversal ////
	///////////////////////////////////////////////////////////////////////////////////

	public default Neo4jGraphTraversal<S, com.tinkerpop.gremlin.structure.Vertex> addBothE(java.lang.String arg0, java.lang.String arg1, java.lang.Object... arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.addBothE(arg0, arg1, arg2);
	}

	public default Neo4jGraphTraversal<S, com.tinkerpop.gremlin.structure.Vertex> addE(com.tinkerpop.gremlin.structure.Direction arg0, java.lang.String arg1, java.lang.String arg2, java.lang.Object... arg3) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.addE(arg0, arg1, arg2, arg3);
	}

	public default Neo4jGraphTraversal<S, com.tinkerpop.gremlin.structure.Vertex> addInE(java.lang.String arg0, java.lang.String arg1, java.lang.Object... arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.addInE(arg0, arg1, arg2);
	}

	public default Neo4jGraphTraversal<S, com.tinkerpop.gremlin.structure.Vertex> addOutE(java.lang.String arg0, java.lang.String arg1, java.lang.Object... arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.addOutE(arg0, arg1, arg2);
	}

	public default Neo4jGraphTraversal<S, E> aggregate() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.aggregate();
	}

	public default Neo4jGraphTraversal<S, E> aggregate(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.aggregate(arg0);
	}

	public default Neo4jGraphTraversal<S, E> as(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.as(arg0);
	}

	public default <E2> Neo4jGraphTraversal<S, E2> back(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.back(arg0);
	}

	public default <E2 extends Element> Neo4jGraphTraversal<S, E2> between(java.lang.String arg0, java.lang.Comparable arg1, java.lang.Comparable arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.between(arg0, arg1, arg2);
	}

	public default Neo4jGraphTraversal<S, com.tinkerpop.gremlin.structure.Vertex> both(java.lang.String... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.both(arg0);
	}

	public default Neo4jGraphTraversal<S, com.tinkerpop.gremlin.structure.Edge> bothE(java.lang.String... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.bothE(arg0);
	}

	public default Neo4jGraphTraversal<S, com.tinkerpop.gremlin.structure.Vertex> bothV() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.bothV();
	}

	public default Neo4jGraphTraversal<S, E> branch(java.util.function.Function<com.tinkerpop.gremlin.process.Traverser<E>, java.util.Collection<java.lang.String>> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.branch(arg0);
	}

	public default Neo4jGraphTraversal<S, E> by() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.by();
	}

	public default <V> Neo4jGraphTraversal<S, E> by(java.util.function.Function<V, java.lang.Object> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.by(arg0);
	}

	public default Neo4jGraphTraversal<S, E> by(com.tinkerpop.gremlin.process.T arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.by(arg0);
	}

	public default Neo4jGraphTraversal<S, E> by(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.by(arg0);
	}

	public default Neo4jGraphTraversal<S, E> by(java.util.Comparator<E> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.by(arg0);
	}

	public default <V> Neo4jGraphTraversal<S, E> by(com.tinkerpop.gremlin.process.T arg0, java.util.Comparator<V> arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.by(arg0, arg1);
	}

	public default <V> Neo4jGraphTraversal<S, E> by(java.lang.String arg0, java.util.Comparator<V> arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.by(arg0, arg1);
	}

	public default <V> Neo4jGraphTraversal<S, E> by(java.util.function.Function<com.tinkerpop.gremlin.structure.Element, V> arg0, java.util.Comparator<V> arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.by(arg0, arg1);
	}

	public default <E2> Neo4jGraphTraversal<S, E2> cap() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.cap();
	}

	public default <E2> Neo4jGraphTraversal<S, E2> cap(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.cap(arg0);
	}

	public default <M,E2> Neo4jGraphTraversal<S, E2> choose(java.util.function.Function<E, M> arg0, java.util.Map<M, com.tinkerpop.gremlin.process.Traversal<?, E2>> arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.choose(arg0, arg1);
	}

	public default <E2> Neo4jGraphTraversal<S, E2> choose(java.util.function.Predicate<E> arg0, com.tinkerpop.gremlin.process.Traversal<?, E2> arg1, com.tinkerpop.gremlin.process.Traversal<?, E2> arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.choose(arg0, arg1, arg2);
	}

	public default Neo4jGraphTraversal<S, E> coin(double arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.coin(arg0);
	}

	public default Neo4jGraphTraversal<S, java.lang.Long> count() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.count();
	}

	public default Neo4jGraphTraversal<S, E> cyclicPath() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.cyclicPath();
	}

	public default Neo4jGraphTraversal<S, E> dedup() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.dedup();
	}

	public default Neo4jGraphTraversal<S, E> emit() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.emit();
	}

	public default Neo4jGraphTraversal<S, E> emit(com.tinkerpop.gremlin.process.Traversal<?, ?> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.emit(arg0);
	}

	public default Neo4jGraphTraversal<S, E> emit(java.util.function.Predicate<com.tinkerpop.gremlin.process.Traverser<E>> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.emit(arg0);
	}

	public default Neo4jGraphTraversal<S, E> except(E arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.except(arg0);
	}

	public default Neo4jGraphTraversal<S, E> except(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.except(arg0);
	}

	public default Neo4jGraphTraversal<S, E> except(java.util.Collection<E> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.except(arg0);
	}

	public default Neo4jGraphTraversal<S, E> filter(java.util.function.Predicate<com.tinkerpop.gremlin.process.Traverser<E>> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.filter(arg0);
	}

	public default <E2> Neo4jGraphTraversal<S, E2> flatMap(java.util.function.Function<com.tinkerpop.gremlin.process.Traverser<E>, java.util.Iterator<E2>> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.flatMap(arg0);
	}

	public default Neo4jGraphTraversal<S, java.util.List<E>> fold() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.fold();
	}

	public default <E2> Neo4jGraphTraversal<S, E2> fold(E2 arg0, java.util.function.BiFunction<E2, E, E2> arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.fold(arg0, arg1);
	}

	public default Neo4jGraphTraversal<S, E> group() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.group();
	}

	public default Neo4jGraphTraversal<S, E> group(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.group(arg0);
	}

	public default Neo4jGraphTraversal<S, E> groupCount() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.groupCount();
	}

	public default Neo4jGraphTraversal<S, E> groupCount(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.groupCount(arg0);
	}

	public default <E2 extends Element> Neo4jGraphTraversal<S, E2> has(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.has(arg0);
	}

	public default <E2 extends Element> Neo4jGraphTraversal<S, E2> has(com.tinkerpop.gremlin.process.T arg0, java.lang.Object arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.has(arg0, arg1);
	}

	public default <E2 extends Element> Neo4jGraphTraversal<S, E2> has(java.lang.String arg0, java.lang.Object arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.has(arg0, arg1);
	}

	public default <E2 extends Element> Neo4jGraphTraversal<S, E2> has(com.tinkerpop.gremlin.process.T arg0, java.util.function.BiPredicate arg1, java.lang.Object arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.has(arg0, arg1, arg2);
	}

	public default <E2 extends Element> Neo4jGraphTraversal<S, E2> has(java.lang.String arg0, java.lang.String arg1, java.lang.Object arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.has(arg0, arg1, arg2);
	}

	public default <E2 extends Element> Neo4jGraphTraversal<S, E2> has(java.lang.String arg0, java.util.function.BiPredicate arg1, java.lang.Object arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.has(arg0, arg1, arg2);
	}

	public default <E2 extends Element> Neo4jGraphTraversal<S, E2> has(java.lang.String arg0, java.lang.String arg1, java.util.function.BiPredicate arg2, java.lang.Object arg3) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.has(arg0, arg1, arg2, arg3);
	}

	public default <E2 extends Element> Neo4jGraphTraversal<S, E2> hasNot(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.hasNot(arg0);
	}

	public default Neo4jGraphTraversal<S, java.lang.Object> id() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.id();
	}

	public default Neo4jGraphTraversal<S, E> identity() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.identity();
	}

	public default Neo4jGraphTraversal<S, com.tinkerpop.gremlin.structure.Vertex> in(java.lang.String... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.in(arg0);
	}

	public default Neo4jGraphTraversal<S, com.tinkerpop.gremlin.structure.Edge> inE(java.lang.String... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.inE(arg0);
	}

	public default Neo4jGraphTraversal<S, com.tinkerpop.gremlin.structure.Vertex> inV() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.inV();
	}

	public default Neo4jGraphTraversal<S, E> inject(E... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.inject(arg0);
	}

	public default Neo4jGraphTraversal<S, java.lang.String> key() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.key();
	}

	public default Neo4jGraphTraversal<S, java.lang.String> label() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.label();
	}

	public default Neo4jGraphTraversal<S, E> limit(long arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.limit(arg0);
	}

	public default <E2> Neo4jGraphTraversal<S, E2> local(com.tinkerpop.gremlin.process.Traversal<?, E2> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.local(arg0);
	}

	public default <E2> Neo4jGraphTraversal<S, E2> map(java.util.function.Function<com.tinkerpop.gremlin.process.Traverser<E>, E2> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.map(arg0);
	}

	public default <E2> Neo4jGraphTraversal<S, java.util.Map<java.lang.String, E2>> match(java.lang.String arg0, com.tinkerpop.gremlin.process.Traversal... arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.match(arg0, arg1);
	}

	public default Neo4jGraphTraversal<S, E> order() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.order();
	}

	public default Neo4jGraphTraversal<S, com.tinkerpop.gremlin.structure.Vertex> otherV() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.otherV();
	}

	public default Neo4jGraphTraversal<S, com.tinkerpop.gremlin.structure.Vertex> out(java.lang.String... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.out(arg0);
	}

	public default Neo4jGraphTraversal<S, com.tinkerpop.gremlin.structure.Edge> outE(java.lang.String... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.outE(arg0);
	}

	public default Neo4jGraphTraversal<S, com.tinkerpop.gremlin.structure.Vertex> outV() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.outV();
	}

	public default Neo4jGraphTraversal<S, com.tinkerpop.gremlin.process.Path> path() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.path();
	}

	public default Neo4jGraphTraversal<S, E> profile() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.profile();
	}

	public default <E2> Neo4jGraphTraversal<S, ? extends com.tinkerpop.gremlin.structure.Property<E2>> properties(java.lang.String... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.properties(arg0);
	}

	public default <E2> Neo4jGraphTraversal<S, java.util.Map<java.lang.String, E2>> propertyMap(java.lang.String... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.propertyMap(arg0);
	}

	public default Neo4jGraphTraversal<S, E> range(long arg0, long arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.range(arg0, arg1);
	}

	public default Neo4jGraphTraversal<S, E> repeat(com.tinkerpop.gremlin.process.Traversal<?, E> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.repeat(arg0);
	}

	public default Neo4jGraphTraversal<S, E> retain(E arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.retain(arg0);
	}

	public default Neo4jGraphTraversal<S, E> retain(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.retain(arg0);
	}

	public default Neo4jGraphTraversal<S, E> retain(java.util.Collection<E> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.retain(arg0);
	}

	public default <E2> Neo4jGraphTraversal<S, E2> sack() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.sack();
	}

	public default <V> Neo4jGraphTraversal<S, E> sack(java.util.function.BiFunction<V, E, V> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.sack(arg0);
	}

	public default <E2 extends Element,V> Neo4jGraphTraversal<S, E2> sack(java.util.function.BinaryOperator<V> arg0, java.lang.String arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.sack(arg0, arg1);
	}

	public default Neo4jGraphTraversal<S, E> sample(int arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.sample(arg0);
	}

	public default <E2> Neo4jGraphTraversal<S, E2> select(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.select(arg0);
	}

	public default <E2> Neo4jGraphTraversal<S, java.util.Map<java.lang.String, E2>> select(java.lang.String... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.select(arg0);
	}

	public default Neo4jGraphTraversal<S, E> shuffle() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.shuffle();
	}

	public default Neo4jGraphTraversal<S, E> sideEffect(java.util.function.Consumer<com.tinkerpop.gremlin.process.Traverser<E>> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.sideEffect(arg0);
	}

	public default Neo4jGraphTraversal<S, E> simplePath() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.simplePath();
	}

	public default Neo4jGraphTraversal<S, E> store() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.store();
	}

	public default Neo4jGraphTraversal<S, E> store(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.store(arg0);
	}

	public default Neo4jGraphTraversal<S, E> subgraph(java.util.function.Predicate<com.tinkerpop.gremlin.structure.Edge> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.subgraph(arg0);
	}

	public default Neo4jGraphTraversal<S, E> subgraph(java.lang.String arg0, java.util.function.Predicate<com.tinkerpop.gremlin.structure.Edge> arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.subgraph(arg0, arg1);
	}

	public default Neo4jGraphTraversal<S, E> subgraph(java.util.Set<java.lang.Object> arg0, java.util.Map<java.lang.Object, com.tinkerpop.gremlin.structure.Vertex> arg1, java.util.function.Predicate<com.tinkerpop.gremlin.structure.Edge> arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.subgraph(arg0, arg1, arg2);
	}

	public default Neo4jGraphTraversal<S, E> subgraph(java.lang.String arg0, java.util.Set<java.lang.Object> arg1, java.util.Map<java.lang.Object, com.tinkerpop.gremlin.structure.Vertex> arg2, java.util.function.Predicate<com.tinkerpop.gremlin.structure.Edge> arg3) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.subgraph(arg0, arg1, arg2, arg3);
	}

	public default Neo4jGraphTraversal<S, E> submit(com.tinkerpop.gremlin.process.computer.GraphComputer arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.submit(arg0);
	}

	public default Neo4jGraphTraversal<S, java.lang.Double> sum() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.sum();
	}

	public default Neo4jGraphTraversal<S, E> timeLimit(long arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.timeLimit(arg0);
	}

	public default Neo4jGraphTraversal<S, E> times(int arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.times(arg0);
	}

	public default Neo4jGraphTraversal<S, com.tinkerpop.gremlin.structure.Vertex> to(com.tinkerpop.gremlin.structure.Direction arg0, java.lang.String... arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.to(arg0, arg1);
	}

	public default Neo4jGraphTraversal<S, com.tinkerpop.gremlin.structure.Edge> toE(com.tinkerpop.gremlin.structure.Direction arg0, java.lang.String... arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.toE(arg0, arg1);
	}

	public default Neo4jGraphTraversal<S, com.tinkerpop.gremlin.structure.Vertex> toV(com.tinkerpop.gremlin.structure.Direction arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.toV(arg0);
	}

	public default Neo4jGraphTraversal<S, E> tree() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.tree();
	}

	public default Neo4jGraphTraversal<S, E> tree(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.tree(arg0);
	}

	public default <E2> Neo4jGraphTraversal<S, E2> unfold() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.unfold();
	}

	public default <E2> Neo4jGraphTraversal<S, E2> union(com.tinkerpop.gremlin.process.Traversal<?, E2>... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.union(arg0);
	}

	public default Neo4jGraphTraversal<S, E> until(com.tinkerpop.gremlin.process.Traversal<?, ?> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.until(arg0);
	}

	public default Neo4jGraphTraversal<S, E> until(java.util.function.Predicate<com.tinkerpop.gremlin.process.Traverser<E>> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.until(arg0);
	}

	public default <E2> Neo4jGraphTraversal<S, E2> value() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.value();
	}

	public default <E2> Neo4jGraphTraversal<S, java.util.Map<java.lang.String, E2>> valueMap(java.lang.String... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.valueMap(arg0);
	}

	public default <E2> Neo4jGraphTraversal<S, java.util.Map<java.lang.String, E2>> valueMap(boolean arg0, java.lang.String... arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.valueMap(arg0, arg1);
	}

	public default <E2> Neo4jGraphTraversal<S, E2> values(java.lang.String... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.values(arg0);
	}

	public default <E2> Neo4jGraphTraversal<S, java.util.Map<java.lang.String, E2>> where(com.tinkerpop.gremlin.process.Traversal arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.where(arg0);
	}

	public default <E2> Neo4jGraphTraversal<S, java.util.Map<java.lang.String, E2>> where(java.lang.String arg0, java.lang.String arg1, java.util.function.BiPredicate arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.where(arg0, arg1, arg2);
	}

	public default <E2> Neo4jGraphTraversal<S, java.util.Map<java.lang.String, E2>> where(java.lang.String arg0, java.util.function.BiPredicate arg1, java.lang.String arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.where(arg0, arg1, arg2);
	}

	public default Neo4jGraphTraversal<S, E> withPath() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.withPath();
	}

	public default <A> Neo4jGraphTraversal<S, E> withSack(java.util.function.Supplier<A> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.withSack(arg0);
	}

	public default <A> Neo4jGraphTraversal<S, E> withSack(java.util.function.Supplier<A> arg0, java.util.function.UnaryOperator<A> arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.withSack(arg0, arg1);
	}

	public default Neo4jGraphTraversal<S, E> withSideEffect(java.lang.String arg0, java.util.function.Supplier arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.GraphTraversal.super.withSideEffect(arg0, arg1);
	}

}