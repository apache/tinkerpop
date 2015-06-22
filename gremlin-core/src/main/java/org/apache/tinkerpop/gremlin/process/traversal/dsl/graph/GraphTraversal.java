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

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.FunctionTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.LoopTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.PredicateTraverser;
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
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.LambdaFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SimplePathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TimeLimitStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CoalesceStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ConstantStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.DedupLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupCountStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LabelStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaFlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyKeyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.RangeLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SackStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SampleLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TailLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalFlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.UnfoldStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
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
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TraversalSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementFunctionComparator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementValueComparator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.TraversalComparator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.function.ConstantSupplier;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

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

    /**
     * Map a traverser referencing an object of type <code>E</code> to an object of type <code>E2</code>.
     *
     * @param function the lambda expression that does the functional mapping
     * @param <E2>the  mapping end type
     * @return the traversal with an appended {@link LambdaMapStep}.
     */
    public default <E2> GraphTraversal<S, E2> map(final Function<Traverser<E>, E2> function) {
        return this.asAdmin().addStep(new LambdaMapStep<>(this.asAdmin(), function));
    }

    public default <E2> GraphTraversal<S, E2> map(final Traversal<?, E2> mapTraversal) {
        return this.asAdmin().addStep(new TraversalMapStep<>(this.asAdmin(), mapTraversal));
    }

    /**
     * Map a traverser referencing an object of type <code>E</code> to an iterator of objects of type <code>E2</code>.
     * The resultant iterator is drained one-by-one before a new <code>E</code> object is pulled in for processing.
     *
     * @param function the lambda expression that does the functional mapping
     * @param <E2>     the type of the returned iterator objects
     * @return the traversal with an appended {@link LambdaFlatMapStep}.
     */
    public default <E2> GraphTraversal<S, E2> flatMap(final Function<Traverser<E>, Iterator<E2>> function) {
        return this.asAdmin().addStep(new LambdaFlatMapStep<>(this.asAdmin(), function));
    }

    public default <E2> GraphTraversal<S, E2> flatMap(final Traversal<?, E2> flatMapTraversal) {
        return this.asAdmin().addStep(new TraversalFlatMapStep<>(this.asAdmin(), flatMapTraversal));
    }

    /**
     * Map the {@link Element} to its {@link Element#id}.
     *
     * @return the traversal with an appended {@link IdStep}.
     */
    public default GraphTraversal<S, Object> id() {
        return this.asAdmin().addStep(new IdStep<>(this.asAdmin()));
    }

    /**
     * Map the {@link Element} to its {@link Element#label}.
     *
     * @return the traversal with an appended {@link LabelStep}.
     */
    public default GraphTraversal<S, String> label() {
        return this.asAdmin().addStep(new LabelStep<>(this.asAdmin()));
    }

    /**
     * Map the <code>E</code> object to itself. In other words, a "no op."
     *
     * @return the traversal with an appended {@link IdentityStep}.
     */
    public default GraphTraversal<S, E> identity() {
        return this.asAdmin().addStep(new IdentityStep<>(this.asAdmin()));
    }

    /**
     * Map any object to a fixed <code>E</code> value.
     *
     * @return the traversal with an appended {@link ConstantStep}.
     */
    public default <E2> GraphTraversal<S, E2> constant(final E2 e) {
        return this.asAdmin().addStep(new ConstantStep<E, E2>(this.asAdmin(), e));
    }

    /**
     * Map the {@link Vertex} to its adjacent vertices given a direction and edge labels.
     *
     * @param direction  the direction to traverse from the current vertex
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     */
    public default GraphTraversal<S, Vertex> to(final Direction direction, final String... edgeLabels) {
        return this.asAdmin().addStep(new VertexStep<>(this.asAdmin(), Vertex.class, direction, edgeLabels));
    }

    /**
     * Map the {@link Vertex} to its outgoing adjacent vertices given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     */
    public default GraphTraversal<S, Vertex> out(final String... edgeLabels) {
        return this.to(Direction.OUT, edgeLabels);
    }

    /**
     * Map the {@link Vertex} to its incoming adjacent vertices given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     */
    public default GraphTraversal<S, Vertex> in(final String... edgeLabels) {
        return this.to(Direction.IN, edgeLabels);
    }

    /**
     * Map the {@link Vertex} to its adjacent vertices given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     */
    public default GraphTraversal<S, Vertex> both(final String... edgeLabels) {
        return this.to(Direction.BOTH, edgeLabels);
    }

    /**
     * Map the {@link Vertex} to its incident edges given the direction and edge labels.
     *
     * @param direction  the direction to traverse from the current vertex
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     */
    public default GraphTraversal<S, Edge> toE(final Direction direction, final String... edgeLabels) {
        return this.asAdmin().addStep(new VertexStep<>(this.asAdmin(), Edge.class, direction, edgeLabels));
    }

    /**
     * Map the {@link Vertex} to its outgoing incident edges given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     */
    public default GraphTraversal<S, Edge> outE(final String... edgeLabels) {
        return this.toE(Direction.OUT, edgeLabels);
    }

    /**
     * Map the {@link Vertex} to its incoming incident edges given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     */
    public default GraphTraversal<S, Edge> inE(final String... edgeLabels) {
        return this.toE(Direction.IN, edgeLabels);
    }

    /**
     * Map the {@link Vertex} to its incident edges given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     */
    public default GraphTraversal<S, Edge> bothE(final String... edgeLabels) {
        return this.toE(Direction.BOTH, edgeLabels);
    }

    /**
     * Map the {@link Edge} to its incident vertices given the direction.
     *
     * @param direction the direction to traverser from the current edge
     * @return the traversal with an appended {@link EdgeVertexStep}.
     */
    public default GraphTraversal<S, Vertex> toV(final Direction direction) {
        return this.asAdmin().addStep(new EdgeVertexStep(this.asAdmin(), direction));
    }

    /**
     * Map the {@link Edge} to its incoming/head incident {@link Vertex}.
     *
     * @return the traversal with an appended {@link EdgeVertexStep}.
     */
    public default GraphTraversal<S, Vertex> inV() {
        return this.toV(Direction.IN);
    }

    /**
     * Map the {@link Edge} to its outgoing/tail incident {@link Vertex}.
     *
     * @return the traversal with an appended {@link EdgeVertexStep}.
     */
    public default GraphTraversal<S, Vertex> outV() {
        return this.toV(Direction.OUT);
    }

    /**
     * Map the {@link Edge} to its incident vertices.
     *
     * @return the traversal with an appended {@link EdgeVertexStep}.
     */
    public default GraphTraversal<S, Vertex> bothV() {
        return this.toV(Direction.BOTH);
    }

    /**
     * Map the {@link Edge} to the incident vertex that was not just traversed from in the path history.
     *
     * @return the traversal with an appended {@link EdgeOtherVertexStep}.
     */
    public default GraphTraversal<S, Vertex> otherV() {
        return this.asAdmin().addStep(new EdgeOtherVertexStep(this.asAdmin()));
    }

    /**
     * Order all the objects in the traversal up to this point and then emit them one-by-one in their ordered sequence.
     *
     * @return the traversal with an appended {@link OrderGlobalStep}.
     */
    public default GraphTraversal<S, E> order() {
        return this.order(Scope.global);
    }

    /**
     * Order either the {@link Scope#local} object (e.g. a list, map, etc.) or the entire {@link Scope#global} traversal stream.
     *
     * @param scope whether the ordering is the current local object or the entire global stream.
     * @return the traversal with an appended {@link OrderGlobalStep} or {@link OrderLocalStep}.
     */
    public default GraphTraversal<S, E> order(final Scope scope) {
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new OrderGlobalStep<>(this.asAdmin()) : new OrderLocalStep<>(this.asAdmin()));
    }

    /**
     * Map the {@link Element} to its associated properties given the provide property keys.
     * If no property keys are provided, then all properties are emitted.
     *
     * @param propertyKeys the properties to retrieve
     * @param <E2>         the value type of the returned properties
     * @return the traversal with an appended {@link PropertiesStep}.
     */
    public default <E2> GraphTraversal<S, ? extends Property<E2>> properties(final String... propertyKeys) {
        return this.asAdmin().addStep(new PropertiesStep<>(this.asAdmin(), PropertyType.PROPERTY, propertyKeys));
    }

    /**
     * Map the {@link Element} to the values of the associated properties given the provide property keys.
     * If no property keys are provided, then all property values are emitted.
     *
     * @param propertyKeys the properties to retrieve their value from
     * @param <E2>         the value type of the properties
     * @return the traversal with an appended {@link PropertiesStep}.
     */
    public default <E2> GraphTraversal<S, E2> values(final String... propertyKeys) {
        return this.asAdmin().addStep(new PropertiesStep<>(this.asAdmin(), PropertyType.VALUE, propertyKeys));
    }

    /**
     * Map the {@link Element} to a {@link Map} of the properties key'd according to their {@link Property#key}.
     * If no property keys are provided, then all properties are retrieved.
     *
     * @param propertyKeys the properties to retrieve
     * @param <E2>         the value type of the returned properties
     * @return the traversal with an appended {@link PropertyMapStep}.
     */
    public default <E2> GraphTraversal<S, Map<String, E2>> propertyMap(final String... propertyKeys) {
        return this.asAdmin().addStep(new PropertyMapStep<>(this.asAdmin(), false, PropertyType.PROPERTY, propertyKeys));
    }

    /**
     * Map the {@link Element} to a {@link Map} of the property values key'd according to their {@link Property#key}.
     * If no property keys are provided, then all property values are retrieved.
     *
     * @param propertyKeys the properties to retrieve
     * @param <E2>         the value type of the returned properties
     * @return the traversal with an appended {@link PropertyMapStep}.
     */
    public default <E2> GraphTraversal<S, Map<String, E2>> valueMap(final String... propertyKeys) {
        return this.asAdmin().addStep(new PropertyMapStep<>(this.asAdmin(), false, PropertyType.VALUE, propertyKeys));
    }

    /**
     * Map the {@link Element} to a {@link Map} of the property values key'd according to their {@link Property#key}.
     * If no property keys are provided, then all property values are retrieved.
     *
     * @param includeTokens whether to include {@link T} tokens in the emitted map.
     * @param propertyKeys  the properties to retrieve
     * @param <E2>          the value type of the returned properties
     * @return the traversal with an appended {@link PropertyMapStep}.
     */
    public default <E2> GraphTraversal<S, Map<String, E2>> valueMap(final boolean includeTokens, final String... propertyKeys) {
        return this.asAdmin().addStep(new PropertyMapStep<>(this.asAdmin(), includeTokens, PropertyType.VALUE, propertyKeys));
    }

    /**
     * Map the {@link Property} to its {@link Property#key}.
     *
     * @return the traversal with an appended {@link PropertyKeyStep}.
     */
    public default GraphTraversal<S, String> key() {
        return this.asAdmin().addStep(new PropertyKeyStep(this.asAdmin()));
    }

    /**
     * Map the {@link Property} to its {@link Property#value}.
     *
     * @return the traversal with an appended {@link PropertyValueStep}.
     */
    public default <E2> GraphTraversal<S, E2> value() {
        return this.asAdmin().addStep(new PropertyValueStep<>(this.asAdmin()));
    }

    /**
     * Map the {@link Traverser} to its {@link Path} history via {@link Traverser#path}.
     *
     * @return the traversal with an appended {@link PathStep}.
     */
    public default GraphTraversal<S, Path> path() {
        return this.asAdmin().addStep(new PathStep<>(this.asAdmin()));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> match(final String startKey, final Traversal<?, ?>... matchTraversals) {
        return this.asAdmin().addStep(new MatchStep<>(this.asAdmin(), startKey, MatchStep.Conjunction.AND, matchTraversals));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> match(final Traversal<?, ?>... matchTraversals) {
        return this.match(null, matchTraversals);
    }

    /**
     * Map the {@link Traverser} to its {@link Traverser#sack} value.
     *
     * @param <E2> the sack value type
     * @return the traversal with an appended {@link SackStep}.
     */
    public default <E2> GraphTraversal<S, E2> sack() {
        return this.asAdmin().addStep(new SackStep<>(this.asAdmin()));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> select(final Scope scope, final Pop pop, final String... stepLabels) {
        return this.asAdmin().addStep(new SelectStep<>(this.asAdmin(), scope, pop, stepLabels));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> select(final Scope scope, final String... stepLabels) {
        return this.asAdmin().addStep(new SelectStep<>(this.asAdmin(), scope, stepLabels));
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> select(final Pop pop, final String... stepLabels) {
        return this.select(Scope.global, pop, stepLabels);
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> select(final String... stepLabels) {
        return this.select(Scope.global, stepLabels);
    }

    public default <E2> GraphTraversal<S, E2> select(final Scope scope, final Pop pop, final String stepLabel) {
        return this.asAdmin().addStep(new SelectOneStep(this.asAdmin(), scope, pop, stepLabel));
    }

    public default <E2> GraphTraversal<S, E2> select(final Scope scope, final String stepLabel) {
        return this.asAdmin().addStep(new SelectOneStep(this.asAdmin(), scope, stepLabel));
    }

    public default <E2> GraphTraversal<S, E2> select(final Pop pop, final String stepLabel) {
        return this.select(Scope.global, pop, stepLabel);
    }

    public default <E2> GraphTraversal<S, E2> select(final String stepLabel) {
        return this.select(Scope.global, stepLabel);
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

    /**
     * Map the traversal stream to its reduction as a sum of the {@link Traverser#bulk} values (i.e. count the number of traversers up to this point).
     *
     * @return the traversal with an appended {@link CountGlobalStep}.
     */
    public default GraphTraversal<S, Long> count() {
        return this.count(Scope.global);
    }

    public default GraphTraversal<S, Long> count(final Scope scope) {
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new CountGlobalStep<>(this.asAdmin()) : new CountLocalStep<>(this.asAdmin()));
    }

    /**
     * Map the traversal stream to its reduction as a sum of the {@link Traverser#get} values multiplied by their {@link Traverser#bulk} (i.e. sum the traverser values up to this point).
     *
     * @return the traversal with an appended {@link SumGlobalStep}.
     */
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

    public default GraphTraversal<S, Edge> addE(final Scope scope, final Direction direction, final String firstVertexKeyOrEdgeLabel, final String edgeLabelOrSecondVertexKey, final Object... propertyKeyValues) {
        if (propertyKeyValues.length % 2 == 0)
            return this.asAdmin().addStep(new AddEdgeStep<>(this.asAdmin(), scope, direction, null, firstVertexKeyOrEdgeLabel, edgeLabelOrSecondVertexKey, propertyKeyValues));
        else
            return this.asAdmin().addStep(new AddEdgeStep<>(this.asAdmin(), scope, direction, firstVertexKeyOrEdgeLabel, edgeLabelOrSecondVertexKey, (String) propertyKeyValues[0], Arrays.copyOfRange(propertyKeyValues, 1, propertyKeyValues.length)));
    }

    public default GraphTraversal<S, Edge> addE(final Direction direction, final String firstVertexKeyOrEdgeLabel, final String edgeLabelOrSecondVertexKey, final Object... propertyKeyValues) {
        return this.addE(Scope.global, direction, firstVertexKeyOrEdgeLabel, edgeLabelOrSecondVertexKey, propertyKeyValues);
    }

    public default GraphTraversal<S, Edge> addOutE(final String firstVertexKeyOrEdgeLabel, final String edgeLabelOrSecondVertexKey, final Object... propertyKeyValues) {
        return this.addE(Scope.global, Direction.OUT, firstVertexKeyOrEdgeLabel, edgeLabelOrSecondVertexKey, propertyKeyValues);
    }

    public default GraphTraversal<S, Edge> addInE(final String firstVertexKeyOrEdgeLabel, final String edgeLabelOrSecondVertexKey, final Object... propertyKeyValues) {
        return this.addE(Scope.global, Direction.IN, firstVertexKeyOrEdgeLabel, edgeLabelOrSecondVertexKey, propertyKeyValues);
    }

    ///////////////////// FILTER STEPS /////////////////////

    public default GraphTraversal<S, E> filter(final Predicate<Traverser<E>> predicate) {
        return this.asAdmin().addStep(new LambdaFilterStep<>(this.asAdmin(), predicate));
    }

    public default GraphTraversal<S, E> filter(final Traversal<?, ?> filterTraversal) {
        return this.asAdmin().addStep(new TraversalFilterStep<>(this.asAdmin(), (Traversal) filterTraversal));
    }

    public default GraphTraversal<S, E> or(final Traversal<?, ?>... orTraversals) {
        return this.asAdmin().addStep(new OrStep(this.asAdmin(), orTraversals));
    }

    public default GraphTraversal<S, E> and(final Traversal<?, ?>... andTraversals) {
        return this.asAdmin().addStep(new AndStep(this.asAdmin(), andTraversals));
    }

    public default GraphTraversal<S, E> inject(final E... injections) {
        return this.asAdmin().addStep(new InjectStep<>(this.asAdmin(), injections));
    }

    /**
     * Remove all duplicates in the traversal stream up to this point.
     *
     * @return the traversal with an appended {@link DedupGlobalStep}.
     */
    public default GraphTraversal<S, E> dedup() {
        return this.dedup(Scope.global);
    }

    public default GraphTraversal<S, E> dedup(final Scope scope) {
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new DedupGlobalStep<>(this.asAdmin()) : new DedupLocalStep(this.asAdmin()));
    }

    public default GraphTraversal<S, E> where(final Scope scope, final String startKey, final P<?> predicate) {
        return this.asAdmin().addStep(new WhereStep<>(this.asAdmin(), scope, Optional.ofNullable(startKey), predicate));
    }

    public default GraphTraversal<S, E> where(final Scope scope, final P<?> predicate) {
        return this.where(scope, null, predicate);
    }

    public default GraphTraversal<S, E> where(final Scope scope, final Traversal<?, ?> whereTraversal) {
        return TraversalHelper.getVariableLocations(whereTraversal.asAdmin()).isEmpty() ?
                this.filter(whereTraversal) :
                this.asAdmin().addStep(new WhereStep(this.asAdmin(), scope, whereTraversal));
    }

    public default GraphTraversal<S, E> where(final String startKey, final P<?> predicate) {
        return this.where(Scope.global, startKey, predicate);
    }

    public default GraphTraversal<S, E> where(final P<?> predicate) {
        return this.where(Scope.global, null, predicate);
    }

    public default GraphTraversal<S, E> where(final Traversal<?, ?> whereTraversal) {
        return this.where(Scope.global, whereTraversal);
    }

    public default GraphTraversal<S, E> has(final String key, final P<?> predicate) {
        return this.asAdmin().addStep(new HasStep(this.asAdmin(), HasContainer.makeHasContainers(key, predicate)));
    }

    public default GraphTraversal<S, E> has(final T accessor, final P<?> predicate) {
        return this.has(accessor.getAccessor(), predicate);
    }

    public default GraphTraversal<S, E> has(final String key, final Object value) {
        return this.has(key, value instanceof P ? (P) value : P.eq(value));
    }

    public default GraphTraversal<S, E> has(final T accessor, final Object value) {
        return this.has(accessor.getAccessor(), value);
    }

    public default GraphTraversal<S, E> has(final String label, final String key, final P<?> predicate) {
        return this.has(T.label, label).has(key, predicate);
    }

    public default GraphTraversal<S, E> has(final String label, final String key, final Object value) {
        return this.has(T.label, label).has(key, value);
    }

    public default GraphTraversal<S, E> has(final String key, final Traversal<?, ?> propertyTraversal) {
        return this.filter(propertyTraversal.asAdmin().addStep(0, new PropertiesStep(propertyTraversal.asAdmin(), PropertyType.VALUE, key)));
    }

    public default GraphTraversal<S, E> has(final String key) {
        return this.filter(__.values(key));
    }

    public default GraphTraversal<S, E> hasNot(final String key) {
        return this.not(__.values(key));
    }

    public default GraphTraversal<S, E> hasLabel(final String... labels) {
        return labels.length == 1 ? this.has(T.label, labels[0]) : this.has(T.label, P.within(labels));
    }

    public default GraphTraversal<S, E> hasId(final Object... ids) {
        return ids.length == 1 ? this.has(T.id, ids[0]) : this.has(T.id, P.within(ids));
    }

    public default GraphTraversal<S, E> hasKey(final String... keys) {
        return keys.length == 1 ? this.has(T.key, keys[0]) : this.has(T.key, P.within(keys));
    }

    public default GraphTraversal<S, E> hasValue(final Object... values) {
        return values.length == 1 ? this.has(T.value, values[0]) : this.has(T.value, P.within(values));
    }

    public default GraphTraversal<S, E> is(final P<E> predicate) {
        return this.asAdmin().addStep(new IsStep<>(this.asAdmin(), predicate));
    }

    /**
     * Filter the <code>E</code> object if it is not {@link P#eq} to the provided value.
     *
     * @param value the value that the object must equal.
     * @return the traversal with an appended {@link IsStep}.
     */
    public default GraphTraversal<S, E> is(final Object value) {
        return this.is(value instanceof P ? (P<E>) value : P.eq((E) value));
    }

    public default GraphTraversal<S, E> not(final Traversal<E, ?> notTraversal) {
        return this.asAdmin().addStep(new NotStep<>(this.asAdmin(), notTraversal));
    }

    /**
     * Filter the <code>E</code> object given a biased coin toss.
     *
     * @param probability the probability that the object will pass through
     * @return the traversal with an appended {@link CoinStep}.
     */
    public default GraphTraversal<S, E> coin(final double probability) {
        return this.asAdmin().addStep(new CoinStep<>(this.asAdmin(), probability));
    }

    public default GraphTraversal<S, E> range(final long low, final long high) {
        return this.<E>range(Scope.global, low, high);
    }

    public default <E2> GraphTraversal<S, E2> range(final Scope scope, final long low, final long high) {
        return this.asAdmin().addStep(scope.equals(Scope.global)
                ? new RangeGlobalStep<>(this.asAdmin(), low, high)
                : new RangeLocalStep<>(this.asAdmin(), low, high));
    }

    public default GraphTraversal<S, E> limit(final long limit) {
        return this.<E>range(Scope.global, 0, limit);
    }

    public default <E2> GraphTraversal<S, E2> limit(final Scope scope, final long limit) {
        return this.range(scope, 0, limit);
    }

    public default GraphTraversal<S, E> tail() {
        return this.tail(1);
    }

    public default GraphTraversal<S, E> tail(final long limit) {
        return this.tail(Scope.global, limit);
    }

    public default <E2> GraphTraversal<S, E2> tail(final Scope scope) {
        return this.<E2>tail(scope, 1);
    }

    public default <E2> GraphTraversal<S, E2> tail(final Scope scope, final long limit) {
        return this.asAdmin().addStep(scope.equals(Scope.global)
                ? new TailGlobalStep<>(this.asAdmin(), limit)
                : new TailLocalStep<>(this.asAdmin(), limit));
    }

    /**
     * Filter the <code>E</code> object if its {@link Traverser#path} is not {@link Path#isSimple}.
     *
     * @return the traversal with an appended {@link SimplePathStep}.
     */
    public default GraphTraversal<S, E> simplePath() {
        return this.asAdmin().addStep(new SimplePathStep<>(this.asAdmin()));
    }

    /**
     * Filter the <code>E</code> object if its {@link Traverser#path} is {@link Path#isSimple}.
     *
     * @return the traversal with an appended {@link CyclicPathStep}.
     */
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

    public default GraphTraversal<S, E> sideEffect(final Traversal<?, ?> sideEffectTraversal) {
        return this.asAdmin().addStep(new TraversalSideEffectStep<>(this.asAdmin(), (Traversal) sideEffectTraversal));
    }

    public default <E2> GraphTraversal<S, E2> cap(final String sideEffectKey, final String... sideEffectKeys) {
        return this.asAdmin().addStep(new SideEffectCapStep<>(this.asAdmin(), sideEffectKey, sideEffectKeys));
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
        return this.branch(__.map(function));
    }

    public default <M, E2> GraphTraversal<S, E2> choose(final Traversal<?, M> choiceTraversal) {
        return this.asAdmin().addStep(new ChooseStep<>(this.asAdmin(), (Traversal.Admin<E, M>) choiceTraversal));
    }

    public default <E2> GraphTraversal<S, E2> choose(final Traversal<?, ?> traversalPredicate, final Traversal<?, E2> trueChoice, final Traversal<?, E2> falseChoice) {
        return this.asAdmin().addStep(new ChooseStep<E, E2, Boolean>(this.asAdmin(), (Traversal.Admin<E, ?>) traversalPredicate, (Traversal.Admin<E, E2>) trueChoice, (Traversal.Admin<E, E2>) falseChoice));
    }

    public default <M, E2> GraphTraversal<S, E2> choose(final Function<E, M> choiceFunction) {
        return this.choose(__.map(new FunctionTraverser<>(choiceFunction)));
    }

    public default <E2> GraphTraversal<S, E2> choose(final Predicate<E> choosePredicate, final Traversal<?, E2> trueChoice, final Traversal<?, E2> falseChoice) {
        return this.choose(__.filter(new PredicateTraverser<>(choosePredicate)), trueChoice, falseChoice);
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
        return this.emit(__.filter(emitPredicate));
    }

    public default GraphTraversal<S, E> emit() {
        return this.emit(TrueTraversal.instance());
    }

    public default GraphTraversal<S, E> until(final Traversal<?, ?> untilTraversal) {
        return RepeatStep.addUntilToTraversal(this, (Traversal.Admin<E, ?>) untilTraversal);
    }

    public default GraphTraversal<S, E> until(final Predicate<Traverser<E>> untilPredicate) {
        return this.until(__.filter(untilPredicate));
    }

    public default GraphTraversal<S, E> times(final int maxLoops) {
        return this.until(new LoopTraversal(maxLoops));
    }

    public default <E2> GraphTraversal<S, E2> local(final Traversal<?, E2> localTraversal) {
        return this.asAdmin().addStep(new LocalStep<>(this.asAdmin(), localTraversal.asAdmin()));
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public default GraphTraversal<S, E> as(final String stepLabel, final String... stepLabels) {
        if (this.asAdmin().getSteps().size() == 0) this.asAdmin().addStep(new StartStep<>(this.asAdmin()));
        final Step<?, E> endStep = this.asAdmin().getEndStep();
        endStep.addLabel(stepLabel);
        for (final String label : stepLabels) {
            endStep.addLabel(label);
        }
        return this;
    }

    public default GraphTraversal<S, E> barrier() {
        return this.asAdmin().addStep(new NoOpBarrierStep<>(this.asAdmin()));
    }

    public default GraphTraversal<S, E> barrier(final int maxBarrierSize) {
        return this.asAdmin().addStep(new NoOpBarrierStep<>(this.asAdmin(), maxBarrierSize));
    }

    ////

    public default GraphTraversal<S, E> by(final Traversal<?, ?> byTraversal) {
        ((TraversalParent) this.asAdmin().getEndStep()).addLocalChild(byTraversal.asAdmin());
        return this;
    }

    public default GraphTraversal<S, E> by() {
        return this.by(new IdentityTraversal<>());
    }

    public default <V> GraphTraversal<S, E> by(final Function<V, Object> functionProjection) {
        return this.by(__.map(new FunctionTraverser<>(functionProjection)));
    }

    public default GraphTraversal<S, E> by(final T tokenProjection) {
        return this.by(new TokenTraversal<>(tokenProjection));
    }

    public default GraphTraversal<S, E> by(final String elementPropertyKey) {
        return this.by(new ElementValueTraversal<>(elementPropertyKey));
    }

    ////

    public default GraphTraversal<S, E> by(final Comparator<E> comparator) {
        ((ComparatorHolder<E>) this.asAdmin().getEndStep()).addComparator(comparator);
        return this;
    }

    public default GraphTraversal<S, E> by(final Order order) {
        return this.by((Comparator) order);
    }

    public default <V> GraphTraversal<S, E> by(final Function<Element, V> elementFunctionProjection, final Comparator<V> elementFunctionValueComparator) {
        return this.by((Comparator) new ElementFunctionComparator<>(elementFunctionProjection, elementFunctionValueComparator));
    }

    public default <V> GraphTraversal<S, E> by(final String elementPropertyProjection, final Comparator<V> propertyValueComparator) {
        return this.by((Comparator) new ElementValueComparator<>(elementPropertyProjection, propertyValueComparator));
    }

    public default <V> GraphTraversal<S, E> by(final Traversal<?, ?> traversal, final Comparator<V> endComparator) {
        return this.by(new TraversalComparator(traversal.asAdmin(), endComparator));
    }

    ////

    public default <M, E2> GraphTraversal<S, E> option(final M pickToken, final Traversal<E, E2> traversalOption) {
        ((TraversalOptionParent<M, E, E2>) this.asAdmin().getEndStep()).addGlobalChildOption(pickToken, traversalOption.asAdmin());
        return this;
    }

    public default <E2> GraphTraversal<S, E> option(final Traversal<E, E2> traversalOption) {
        return this.option(TraversalOptionParent.Pick.any, traversalOption.asAdmin());
    }

    ////

    @Override
    public default GraphTraversal<S, E> iterate() {
        Traversal.super.iterate();
        return this;
    }
}
