/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.dsl.graph;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.T;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.FilterTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.FilterTraverserTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.LoopTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.MapTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.MapTraverserTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TrueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.CoinStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.CyclicPathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ExceptStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.LambdaFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RetainStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SimplePathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TimeLimitStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeByPathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.BackStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CoalesceStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.DedupLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupCountStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.KeyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LabelStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaFlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MinGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MinLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.RangeLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SackStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SampleLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.UnfoldStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.match.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.LambdaSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackElementValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackObjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StoreStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementFunctionComparator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementValueComparator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.PathIdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.TraversalComparator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.Compare;
import org.apache.tinkerpop.gremlin.structure.Contains;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Order;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.function.ConstantSupplier;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

    public interface Admin<S, E> extends Traversal.Admin<S, E>, GraphTraversal<S, E> {

        @Override
        public default <E2> GraphTraversal.Admin<S, E2> addStep(final Step<?, E2> step) {
            return (GraphTraversal.Admin<S, E2>) Traversal.Admin.super.addStep((Step) step);
        }

        @Override
        public default GraphTraversal<S, E> iterate() {
            return GraphTraversal.super.iterate();
        }

        @Override
        public GraphTraversal.Admin<S, E> clone();
    }

    @Override
    public default GraphTraversal.Admin<S, E> asAdmin() {
        return (GraphTraversal.Admin<S, E>) this;
    }

    ///////////////////// MAP STEPS /////////////////////

    public default <E2> GraphTraversal<S, E2> map(final Function<Traverser<E>, E2> function) {
        return this.asAdmin().addStep(new LambdaMapStep<>(this.asAdmin(), function));
    }

    public default <E2> GraphTraversal<S, E2> flatMap(final Function<Traverser<E>, Iterator<E2>> function) {
        return this.asAdmin().addStep(new LambdaFlatMapStep<>(this.asAdmin(), function));
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
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new OrderGlobalStep<>(this.asAdmin()) : new OrderLocalStep<>(this.asAdmin()));
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
        return (GraphTraversal) this.asAdmin().addStep(new MatchStep<E, Map<String, E2>>(this.asAdmin(), startLabel, traversals));
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
        return this.asAdmin().addStep(new FoldStep<>(this.asAdmin(), new ConstantSupplier<>(seed), foldFunction)); // TODO: User should provide supplier?
    }

    public default GraphTraversal<S, Long> count() {
        return this.count(Scope.global);
    }

    public default GraphTraversal<S, Long> count(final Scope scope) {
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new CountGlobalStep<>(this.asAdmin()) : new CountLocalStep<>(this.asAdmin()));
    }

    public default GraphTraversal<S, Double> sum() {
        return this.sum(Scope.global);
    }

    public default GraphTraversal<S, Double> sum(final Scope scope) {
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new SumGlobalStep(this.asAdmin()) : new SumLocalStep<>(this.asAdmin()));
    }

    public default <E2 extends Number> GraphTraversal<S, E2> max() {
        return this.max(Scope.global);
    }

    public default <E2 extends Number> GraphTraversal<S, E2> max(final Scope scope) {
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new MaxGlobalStep<E2>(this.asAdmin()) : new MaxLocalStep(this.asAdmin()));
    }

    public default <E2 extends Number> GraphTraversal<S, E2> min() {
        return this.min(Scope.global);
    }

    public default <E2 extends Number> GraphTraversal<S, E2> min(final Scope scope) {
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new MinGlobalStep<E2>(this.asAdmin()) : new MinLocalStep(this.asAdmin()));
    }


    public default GraphTraversal<S, Double> mean() {
        return this.mean(Scope.global);
    }

    public default GraphTraversal<S, Double> mean(final Scope scope) {
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new MeanGlobalStep<>(this.asAdmin()) : new MeanLocalStep<>(this.asAdmin()));
    }

    public default <K, R> GraphTraversal<S, Map<K, R>> group() {
        return this.asAdmin().addStep(new GroupStep<>(this.asAdmin()));
    }

    public default <E2> GraphTraversal<S, Map<E2, Long>> groupCount() {
        return this.asAdmin().addStep(new GroupCountStep<>(this.asAdmin()));
    }

    public default GraphTraversal<S, Tree> tree() {
        return this.asAdmin().addStep(new TreeStep<>(this.asAdmin()));
    }

    public default GraphTraversal<S, Vertex> addV(final Object... keyValues) {
        return this.asAdmin().addStep(new AddVertexStep<>(this.asAdmin(), keyValues));
    }

    public default GraphTraversal<S, Edge> addE(final Direction direction, final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.asAdmin().addStep(new AddEdgeByPathStep(this.asAdmin(), direction, edgeLabel, stepLabel, propertyKeyValues));
    }

    public default GraphTraversal<S, Edge> addE(final Direction direction, final String edgeLabel, final Vertex otherVertex, final Object... propertyKeyValues) {
        return this.asAdmin().addStep(new AddEdgeStep(this.asAdmin(), direction, edgeLabel, otherVertex, propertyKeyValues));
    }

    public default GraphTraversal<S, Edge> addE(final Direction direction, final String edgeLabel, final Iterator<Vertex> otherVertices, final Object... propertyKeyValues) {
        return this.asAdmin().addStep(new AddEdgeStep(this.asAdmin(), direction, edgeLabel, otherVertices, propertyKeyValues));
    }

    public default GraphTraversal<S, Edge> addInE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.addE(Direction.IN, edgeLabel, stepLabel, propertyKeyValues);
    }

    public default GraphTraversal<S, Edge> addInE(final String edgeLabel, final Vertex otherVertex, final Object... propertyKeyValues) {
        return this.addE(Direction.IN, edgeLabel, otherVertex, propertyKeyValues);
    }

    public default GraphTraversal<S, Edge> addInE(final String edgeLabel, final Iterator<Vertex> otherVertices, final Object... propertyKeyValues) {
        return this.addE(Direction.IN, edgeLabel, otherVertices, propertyKeyValues);
    }

    public default GraphTraversal<S, Edge> addOutE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.addE(Direction.OUT, edgeLabel, stepLabel, propertyKeyValues);
    }

    public default GraphTraversal<S, Edge> addOutE(final String edgeLabel, final Vertex otherVertex, final Object... propertyKeyValues) {
        return this.addE(Direction.OUT, edgeLabel, otherVertex, propertyKeyValues);
    }

    public default GraphTraversal<S, Edge> addOutE(final String edgeLabel, final Iterator<Vertex> otherVertices, final Object... propertyKeyValues) {
        return this.addE(Direction.OUT, edgeLabel, otherVertices, propertyKeyValues);
    }

    ///////////////////// FILTER STEPS /////////////////////

    public default GraphTraversal<S, E> filter(final Predicate<Traverser<E>> predicate) {
        return this.asAdmin().addStep(new LambdaFilterStep<>(this.asAdmin(), predicate));
    }

    public default GraphTraversal<S, E> or(final Traversal<?, ?>... orTraversals) {
        return this.asAdmin().addStep(0 == orTraversals.length ?
                new OrStep.OrMarker<>(this.asAdmin()) :
                new OrStep(this.asAdmin(), Arrays.copyOf(orTraversals, orTraversals.length, Traversal.Admin[].class)));
    }

    public default GraphTraversal<S, E> and(final Traversal<?, ?>... andTraversals) {
        return this.asAdmin().addStep(0 == andTraversals.length ?
                new AndStep.AndMarker<>(this.asAdmin()) :
                new AndStep(this.asAdmin(), Arrays.copyOf(andTraversals, andTraversals.length, Traversal.Admin[].class)));
    }

    public default GraphTraversal<S, E> inject(final E... injections) {
        return this.asAdmin().addStep(new InjectStep<>(this.asAdmin(), injections));
    }

    public default GraphTraversal<S, E> dedup() {
        return this.dedup(Scope.global);
    }

    public default GraphTraversal<S, E> dedup(final Scope scope) {
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new DedupGlobalStep<>(this.asAdmin()) : new DedupLocalStep(this.asAdmin()));
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
        return this.asAdmin().addStep(new HasTraversalStep<>(this.asAdmin(), (Traversal.Admin<E, ?>) hasNextTraversal, false));
    }

    public default GraphTraversal<S, E> hasNot(final Traversal<?, ?> hasNotNextTraversal) {
        return this.asAdmin().addStep(new HasTraversalStep<>(this.asAdmin(), (Traversal.Admin<E, ?>) hasNotNextTraversal, true));
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

    public default GraphTraversal<S, E> hasLabel(final String... labels) {
        return labels.length == 1 ? this.has(T.label, labels[0]) : this.has(T.label, Contains.within, Arrays.asList(labels));
    }

    public default GraphTraversal<S, E> hasId(final Object... ids) {
        return ids.length == 1 ? this.has(T.id, ids[0]) : this.has(T.id, Contains.within, Arrays.asList(ids));
    }

    public default GraphTraversal<S, E> hasKey(final String... keys) {
        return keys.length == 1 ? this.has(T.key, keys[0]) : this.has(T.key, Contains.within, Arrays.asList(keys));
    }

    public default GraphTraversal<S, E> hasValue(final Object... values) {
        return values.length == 1 ? this.has(T.value, values[0]) : this.has(T.value, Contains.within, Arrays.asList(values));
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
        return this.range(Scope.global, low, high);
    }

    public default GraphTraversal<S, E> range(final Scope scope, final long low, final long high) {
        return this.asAdmin().addStep(scope.equals(Scope.global)
                ? new RangeGlobalStep<>(this.asAdmin(), low, high)
                : new RangeLocalStep<>(this.asAdmin(), low, high));
    }

    public default GraphTraversal<S, E> limit(final long limit) {
        return this.range(Scope.global, 0, limit);
    }

    public default GraphTraversal<S, E> limit(final Scope scope, final long limit) {
        return this.range(scope, 0, limit);
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
        return this.sample(Scope.global, amountToSample);
    }

    public default GraphTraversal<S, E> sample(final Scope scope, final int amountToSample) {
        return this.asAdmin().addStep(scope.equals(Scope.global)
                ? new SampleGlobalStep<>(this.asAdmin(), amountToSample)
                : new SampleLocalStep<>(this.asAdmin(), amountToSample));
    }

    public default GraphTraversal<S, E> drop() {
        return this.asAdmin().addStep(new DropStep<>(this.asAdmin()));
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public default GraphTraversal<S, E> sideEffect(final Consumer<Traverser<E>> consumer) {
        return this.asAdmin().addStep(new LambdaSideEffectStep<>(this.asAdmin(), consumer));
    }

    public default <E2> GraphTraversal<S, E2> cap(final String... sideEffectKeys) {
        return this.asAdmin().addStep(new SideEffectCapStep<>(this.asAdmin(), sideEffectKeys));
    }

    public default GraphTraversal<S, Edge> subgraph(final String sideEffectKey) {
        return this.asAdmin().addStep(new SubgraphStep(this.asAdmin(), sideEffectKey));
    }

    public default GraphTraversal<S, E> aggregate(final String sideEffectKey) {
        return this.asAdmin().addStep(new AggregateStep<>(this.asAdmin(), sideEffectKey));
    }

    public default GraphTraversal<S, E> group(final String sideEffectKey) {
        return this.asAdmin().addStep(new GroupSideEffectStep<>(this.asAdmin(), sideEffectKey));
    }

    public default GraphTraversal<S, E> groupCount(final String sideEffectKey) {
        return this.asAdmin().addStep(new GroupCountSideEffectStep<>(this.asAdmin(), sideEffectKey));
    }

    public default GraphTraversal<S, E> timeLimit(final long timeLimit) {
        return this.asAdmin().addStep(new TimeLimitStep<E>(this.asAdmin(), timeLimit));
    }

    public default GraphTraversal<S, E> tree(final String sideEffectKey) {
        return this.asAdmin().addStep(new TreeSideEffectStep<>(this.asAdmin(), sideEffectKey));
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

    public default GraphTraversal<S, E> profile() {
        return this.asAdmin().addStep(new ProfileStep<>(this.asAdmin()));
    }

    public default GraphTraversal<S, E> property(final String key, final Object value, final Object... keyValues) {
        return this.asAdmin().addStep(new AddPropertyStep(this.asAdmin(), key, value, keyValues));
    }

    public default GraphTraversal<S, E> property(final VertexProperty.Cardinality cardinality, final String key, final Object value, final Object... keyValues) {
        return this.asAdmin().addStep(new AddPropertyStep(this.asAdmin(), cardinality, key, value, keyValues));
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

    public default <E2> GraphTraversal<S, E2> coalesce(final Traversal<?, E2>... coalesceTraversals) {
        return this.asAdmin().addStep(new CoalesceStep(this.asAdmin(), Arrays.copyOf(coalesceTraversals, coalesceTraversals.length, Traversal.Admin[].class)));
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

    public default <A> GraphTraversal<S, E> withSack(final A initialValue, final UnaryOperator<A> splitOperator) {
        this.asAdmin().getSideEffects().setSack(new ConstantSupplier<>(initialValue), Optional.of(splitOperator));
        return this;
    }

    public default <A> GraphTraversal<S, E> withSack(A initialValue) {
        this.asAdmin().getSideEffects().setSack(new ConstantSupplier<>(initialValue), Optional.empty());
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
        return this.asAdmin().addStep(new NoOpBarrierStep<>(this.asAdmin()));
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
        ((TraversalParent) this.asAdmin().getEndStep()).addLocalChild(new TokenTraversal<>(tokenProjection));
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

    public default <V> GraphTraversal<S, E> by(final Traversal<?, ?> traversal, final Comparator<V> endComparator) {
        ((ComparatorHolder<E>) this.asAdmin().getEndStep()).addComparator(new TraversalComparator(traversal.asAdmin(), endComparator));
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
    public default GraphTraversal<S, E> iterate() {
        Traversal.super.iterate();
        return this;
    }
}
