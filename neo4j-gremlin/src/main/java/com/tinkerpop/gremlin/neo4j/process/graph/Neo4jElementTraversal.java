////// THIS CLASS IS AUTO-GENERATED, DO NOT EDIT
////// TO ADD METHODS TO THIS CLASS, EDIT Neo4jElementTraversalStub

package com.tinkerpop.gremlin.neo4j.process.graph;

import com.tinkerpop.gremlin.process.graph.ElementTraversal;
import com.tinkerpop.gremlin.structure.Element;

/**
 * Neo4jElementTraversal is merged with {@link com.tinkerpop.gremlin.process.graph.GraphTraversal} via the Maven exec-plugin.
 * The Maven plugin yields Neo4jElementTraversal.java which is ultimately what is depended on by user source.
 * This class maintains {@link Neo4jElementTraversal} specific methods that extend {@link ElementTraversal}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Neo4jElementTraversal<A extends Element> extends ElementTraversal<A> {


	///////////////////////////////////////////////////////////////////////////////////
	//// METHODS INHERITED FROM com.tinkerpop.gremlin.process.graph.ElementTraversal ////
	///////////////////////////////////////////////////////////////////////////////////

	public default Neo4jGraphTraversal<A, com.tinkerpop.gremlin.structure.Vertex> addBothE(java.lang.String arg0, java.lang.String arg1, java.lang.Object... arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.addBothE(arg0, arg1, arg2);
	}

	public default Neo4jGraphTraversal<A, com.tinkerpop.gremlin.structure.Vertex> addE(com.tinkerpop.gremlin.structure.Direction arg0, java.lang.String arg1, java.lang.String arg2, java.lang.Object... arg3) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.addE(arg0, arg1, arg2, arg3);
	}

	public default Neo4jGraphTraversal<A, com.tinkerpop.gremlin.structure.Vertex> addInE(java.lang.String arg0, java.lang.String arg1, java.lang.Object... arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.addInE(arg0, arg1, arg2);
	}

	public default Neo4jGraphTraversal<A, com.tinkerpop.gremlin.structure.Vertex> addOutE(java.lang.String arg0, java.lang.String arg1, java.lang.Object... arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.addOutE(arg0, arg1, arg2);
	}

	public default Neo4jGraphTraversal<A, A> aggregate() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.aggregate();
	}

	public default Neo4jGraphTraversal<A, A> aggregate(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.aggregate(arg0);
	}

	public default Neo4jGraphTraversal<A, A> as(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.as(arg0);
	}

	public default <E2> Neo4jGraphTraversal<A, E2> back(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.back(arg0);
	}

	public default Neo4jGraphTraversal<A, A> between(java.lang.String arg0, java.lang.Comparable arg1, java.lang.Comparable arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.between(arg0, arg1, arg2);
	}

	public default Neo4jGraphTraversal<A, com.tinkerpop.gremlin.structure.Vertex> both(java.lang.String... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.both(arg0);
	}

	public default Neo4jGraphTraversal<A, com.tinkerpop.gremlin.structure.Edge> bothE(java.lang.String... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.bothE(arg0);
	}

	public default Neo4jGraphTraversal<A, com.tinkerpop.gremlin.structure.Vertex> bothV() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.bothV();
	}

	public default Neo4jGraphTraversal<A, A> branch(java.util.function.Function<com.tinkerpop.gremlin.process.Traverser<A>, java.util.Collection<java.lang.String>> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.branch(arg0);
	}

	public default <E2> Neo4jGraphTraversal<A, E2> cap() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.cap();
	}

	public default <E2> Neo4jGraphTraversal<A, E2> cap(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.cap(arg0);
	}

	public default <E2,M> Neo4jGraphTraversal<A, E2> choose(java.util.function.Function<A, M> arg0, java.util.Map<M, com.tinkerpop.gremlin.process.Traversal<?, E2>> arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.choose(arg0, arg1);
	}

	public default <E2> Neo4jGraphTraversal<A, E2> choose(java.util.function.Predicate<A> arg0, com.tinkerpop.gremlin.process.Traversal<?, E2> arg1, com.tinkerpop.gremlin.process.Traversal<?, E2> arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.choose(arg0, arg1, arg2);
	}

	public default Neo4jGraphTraversal<A, A> coin(double arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.coin(arg0);
	}

	public default Neo4jGraphTraversal<A, java.lang.Long> count() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.count();
	}

	public default Neo4jGraphTraversal<A, A> cyclicPath() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.cyclicPath();
	}

	public default Neo4jGraphTraversal<A, A> dedup() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.dedup();
	}

	public default Neo4jGraphTraversal<A, A> emit() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.emit();
	}

	public default Neo4jGraphTraversal<A, A> emit(com.tinkerpop.gremlin.process.Traversal<?, ?> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.emit(arg0);
	}

	public default Neo4jGraphTraversal<A, A> emit(java.util.function.Predicate<com.tinkerpop.gremlin.process.Traverser<A>> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.emit(arg0);
	}

	public default Neo4jGraphTraversal<A, A> except(java.lang.Object arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.except(arg0);
	}

	public default Neo4jGraphTraversal<A, A> except(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.except(arg0);
	}

	public default Neo4jGraphTraversal<A, A> except(java.util.Collection<A> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.except(arg0);
	}

	public default Neo4jGraphTraversal<A, A> filter(java.util.function.Predicate<com.tinkerpop.gremlin.process.Traverser<A>> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.filter(arg0);
	}

	public default <E2> Neo4jGraphTraversal<A, E2> flatMap(java.util.function.Function<com.tinkerpop.gremlin.process.Traverser<A>, java.util.Iterator<E2>> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.flatMap(arg0);
	}

	public default Neo4jGraphTraversal<A, java.util.List<A>> fold() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.fold();
	}

	public default <E2> Neo4jGraphTraversal<A, E2> fold(E2 arg0, java.util.function.BiFunction<E2, A, E2> arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.fold(arg0, arg1);
	}

	public default Neo4jGraphTraversal<A, A> group() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.group();
	}

	public default Neo4jGraphTraversal<A, A> group(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.group(arg0);
	}

	public default Neo4jGraphTraversal<A, A> groupCount() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.groupCount();
	}

	public default Neo4jGraphTraversal<A, A> groupCount(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.groupCount(arg0);
	}

	public default Neo4jGraphTraversal<A, A> has(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.has(arg0);
	}

	public default Neo4jGraphTraversal<A, A> has(com.tinkerpop.gremlin.process.T arg0, java.lang.Object arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.has(arg0, arg1);
	}

	public default Neo4jGraphTraversal<A, A> has(java.lang.String arg0, java.lang.Object arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.has(arg0, arg1);
	}

	public default Neo4jGraphTraversal<A, A> has(com.tinkerpop.gremlin.process.T arg0, java.util.function.BiPredicate arg1, java.lang.Object arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.has(arg0, arg1, arg2);
	}

	public default Neo4jGraphTraversal<A, A> has(java.lang.String arg0, java.lang.String arg1, java.lang.Object arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.has(arg0, arg1, arg2);
	}

	public default Neo4jGraphTraversal<A, A> has(java.lang.String arg0, java.util.function.BiPredicate arg1, java.lang.Object arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.has(arg0, arg1, arg2);
	}

	public default Neo4jGraphTraversal<A, A> has(java.lang.String arg0, java.lang.String arg1, java.util.function.BiPredicate arg2, java.lang.Object arg3) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.has(arg0, arg1, arg2, arg3);
	}

	public default Neo4jGraphTraversal<A, A> hasNot(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.hasNot(arg0);
	}

	public default Neo4jGraphTraversal<A, A> identity() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.identity();
	}

	public default Neo4jGraphTraversal<A, com.tinkerpop.gremlin.structure.Vertex> in(java.lang.String... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.in(arg0);
	}

	public default Neo4jGraphTraversal<A, com.tinkerpop.gremlin.structure.Edge> inE(java.lang.String... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.inE(arg0);
	}

	public default Neo4jGraphTraversal<A, com.tinkerpop.gremlin.structure.Vertex> inV() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.inV();
	}

	public default Neo4jGraphTraversal<A, A> inject(java.lang.Object... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.inject(arg0);
	}

	public default Neo4jGraphTraversal<A, A> limit(long arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.limit(arg0);
	}

	public default <E2> Neo4jGraphTraversal<A, E2> local(com.tinkerpop.gremlin.process.Traversal<?, E2> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.local(arg0);
	}

	public default <E2> Neo4jGraphTraversal<A, E2> map(java.util.function.Function<com.tinkerpop.gremlin.process.Traverser<A>, E2> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.map(arg0);
	}

	public default <E2> Neo4jGraphTraversal<A, java.util.Map<java.lang.String, E2>> match(java.lang.String arg0, com.tinkerpop.gremlin.process.Traversal... arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.match(arg0, arg1);
	}

	public default Neo4jGraphTraversal<A, A> order() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.order();
	}

	public default Neo4jGraphTraversal<A, com.tinkerpop.gremlin.structure.Vertex> otherV() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.otherV();
	}

	public default Neo4jGraphTraversal<A, com.tinkerpop.gremlin.structure.Vertex> out(java.lang.String... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.out(arg0);
	}

	public default Neo4jGraphTraversal<A, com.tinkerpop.gremlin.structure.Edge> outE(java.lang.String... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.outE(arg0);
	}

	public default Neo4jGraphTraversal<A, com.tinkerpop.gremlin.structure.Vertex> outV() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.outV();
	}

	public default Neo4jGraphTraversal<A, com.tinkerpop.gremlin.process.Path> path() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.path();
	}

	public default Neo4jGraphTraversal<A, A> profile() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.profile();
	}

	public default <E2> Neo4jGraphTraversal<A, ? extends com.tinkerpop.gremlin.structure.Property<E2>> properties(java.lang.String... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.properties(arg0);
	}

	public default Neo4jGraphTraversal<A, A> range(long arg0, long arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.range(arg0, arg1);
	}

	public default Neo4jGraphTraversal<A, A> repeat(com.tinkerpop.gremlin.process.Traversal<?, A> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.repeat(arg0);
	}

	public default Neo4jGraphTraversal<A, A> retain(java.lang.Object arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.retain(arg0);
	}

	public default Neo4jGraphTraversal<A, A> retain(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.retain(arg0);
	}

	public default Neo4jGraphTraversal<A, A> retain(java.util.Collection<A> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.retain(arg0);
	}

	public default <E2> Neo4jGraphTraversal<A, E2> sack() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.sack();
	}

	public default <V> Neo4jGraphTraversal<A, A> sack(java.util.function.BiFunction<V, A, V> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.sack(arg0);
	}

	public default <V> Neo4jGraphTraversal<A, A> sack(java.util.function.BinaryOperator<V> arg0, java.lang.String arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.sack(arg0, arg1);
	}

	public default Neo4jGraphTraversal<A, A> sample(int arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.sample(arg0);
	}

	public default <E2> Neo4jGraphTraversal<A, E2> select(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.select(arg0);
	}

	public default <E2> Neo4jGraphTraversal<A, java.util.Map<java.lang.String, E2>> select(java.lang.String... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.select(arg0);
	}

	public default Neo4jGraphTraversal<A, A> shuffle() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.shuffle();
	}

	public default Neo4jGraphTraversal<A, A> sideEffect(java.util.function.Consumer<com.tinkerpop.gremlin.process.Traverser<A>> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.sideEffect(arg0);
	}

	public default Neo4jGraphTraversal<A, A> simplePath() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.simplePath();
	}

	public default Neo4jGraphTraversal<A, A> start() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.start();
	}

	public default Neo4jGraphTraversal<A, A> store() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.store();
	}

	public default Neo4jGraphTraversal<A, A> store(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.store(arg0);
	}

	public default Neo4jGraphTraversal<A, A> subgraph(java.util.function.Predicate<com.tinkerpop.gremlin.structure.Edge> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.subgraph(arg0);
	}

	public default Neo4jGraphTraversal<A, A> subgraph(java.lang.String arg0, java.util.function.Predicate<com.tinkerpop.gremlin.structure.Edge> arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.subgraph(arg0, arg1);
	}

	public default Neo4jGraphTraversal<A, A> subgraph(java.util.Set<java.lang.Object> arg0, java.util.Map<java.lang.Object, com.tinkerpop.gremlin.structure.Vertex> arg1, java.util.function.Predicate<com.tinkerpop.gremlin.structure.Edge> arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.subgraph(arg0, arg1, arg2);
	}

	public default Neo4jGraphTraversal<A, A> subgraph(java.lang.String arg0, java.util.Set<java.lang.Object> arg1, java.util.Map<java.lang.Object, com.tinkerpop.gremlin.structure.Vertex> arg2, java.util.function.Predicate<com.tinkerpop.gremlin.structure.Edge> arg3) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.subgraph(arg0, arg1, arg2, arg3);
	}

	public default Neo4jGraphTraversal<A, A> submit(com.tinkerpop.gremlin.process.computer.GraphComputer arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.submit(arg0);
	}

	public default Neo4jGraphTraversal<A, java.lang.Double> sum() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.sum();
	}

	public default Neo4jGraphTraversal<A, A> timeLimit(long arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.timeLimit(arg0);
	}

	public default Neo4jGraphTraversal<A, A> times(int arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.times(arg0);
	}

	public default Neo4jGraphTraversal<A, com.tinkerpop.gremlin.structure.Vertex> to(com.tinkerpop.gremlin.structure.Direction arg0, java.lang.String... arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.to(arg0, arg1);
	}

	public default Neo4jGraphTraversal<A, com.tinkerpop.gremlin.structure.Edge> toE(com.tinkerpop.gremlin.structure.Direction arg0, java.lang.String... arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.toE(arg0, arg1);
	}

	public default Neo4jGraphTraversal<A, com.tinkerpop.gremlin.structure.Vertex> toV(com.tinkerpop.gremlin.structure.Direction arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.toV(arg0);
	}

	public default Neo4jGraphTraversal<A, A> tree() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.tree();
	}

	public default Neo4jGraphTraversal<A, A> tree(java.lang.String arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.tree(arg0);
	}

	public default Neo4jGraphTraversal<A, A> unfold() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.unfold();
	}

	public default <E2> Neo4jGraphTraversal<A, E2> union(com.tinkerpop.gremlin.process.Traversal<?, E2>... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.union(arg0);
	}

	public default Neo4jGraphTraversal<A, A> until(com.tinkerpop.gremlin.process.Traversal<?, ?> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.until(arg0);
	}

	public default Neo4jGraphTraversal<A, A> until(java.util.function.Predicate<com.tinkerpop.gremlin.process.Traverser<A>> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.until(arg0);
	}

	public default <E2> Neo4jGraphTraversal<A, E2> values(java.lang.String... arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.values(arg0);
	}

	public default <E2> Neo4jGraphTraversal<A, java.util.Map<java.lang.String, E2>> where(com.tinkerpop.gremlin.process.Traversal arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.where(arg0);
	}

	public default <E2> Neo4jGraphTraversal<A, java.util.Map<java.lang.String, E2>> where(java.lang.String arg0, java.lang.String arg1, java.util.function.BiPredicate arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.where(arg0, arg1, arg2);
	}

	public default <E2> Neo4jGraphTraversal<A, java.util.Map<java.lang.String, E2>> where(java.lang.String arg0, java.util.function.BiPredicate arg1, java.lang.String arg2) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.where(arg0, arg1, arg2);
	}

	public default Neo4jGraphTraversal<A, A> withPath() {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.withPath();
	}

	public default <B> Neo4jGraphTraversal<A, A> withSack(java.util.function.Supplier<B> arg0) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.withSack(arg0);
	}

	public default <B> Neo4jGraphTraversal<A, A> withSack(java.util.function.Supplier<B> arg0, java.util.function.UnaryOperator<B> arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.withSack(arg0, arg1);
	}

	public default Neo4jGraphTraversal<A, A> withSideEffect(java.lang.String arg0, java.util.function.Supplier arg1) {
		return (Neo4jGraphTraversal) com.tinkerpop.gremlin.process.graph.ElementTraversal.super.withSideEffect(arg0, arg1);
	}

}