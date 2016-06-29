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

import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.CoinStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.CyclicPathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SimplePathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TimeLimitStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ConstantStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LabelStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaFlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyKeyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SackStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalFlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
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
     * @return the traversal with an appended {@link LambdaMapStep}.
     */
    public default <E2> GraphTraversal<S, E2> map(final Function<Traverser<E>, E2> function) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.map, function);
        return (GraphTraversal) this;
    }

    public default <E2> GraphTraversal<S, E2> map(final Traversal<?, E2> mapTraversal) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.map, mapTraversal);
        return (GraphTraversal) this;
    }

    /**
     * Map a {@link Traverser} referencing an object of type <code>E</code> to an iterator of objects of type <code>E2</code>.
     * The resultant iterator is drained one-by-one before a new <code>E</code> object is pulled in for processing.
     *
     * @param function the lambda expression that does the functional mapping
     * @param <E2>     the type of the returned iterator objects
     * @return the traversal with an appended {@link LambdaFlatMapStep}.
     */
    public default <E2> GraphTraversal<S, E2> flatMap(final Function<Traverser<E>, Iterator<E2>> function) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.flatMap, function);
        return (GraphTraversal) this;
    }

    /**
     * Map a {@link Traverser} referencing an object of type <code>E</code> to an iterator of objects of type <code>E2</code>.
     * The internal traversal is drained one-by-one before a new <code>E</code> object is pulled in for processing.
     *
     * @param flatMapTraversal the traversal generating objects of type <code>E2</code>
     * @param <E2>             the end type of the internal traversal
     * @return the traversal with an appended {@link TraversalFlatMapStep}.
     */
    public default <E2> GraphTraversal<S, E2> flatMap(final Traversal<?, E2> flatMapTraversal) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.flatMap, flatMapTraversal);
        return (GraphTraversal) this;
    }

    /**
     * Map the {@link Element} to its {@link Element#id}.
     *
     * @return the traversal with an appended {@link IdStep}.
     */
    public default GraphTraversal<S, Object> id() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.id);
        return (GraphTraversal) this;
    }

    /**
     * Map the {@link Element} to its {@link Element#label}.
     *
     * @return the traversal with an appended {@link LabelStep}.
     */
    public default GraphTraversal<S, String> label() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.label);
        return (GraphTraversal) this;
    }

    /**
     * Map the <code>E</code> object to itself. In other words, a "no op."
     *
     * @return the traversal with an appended {@link IdentityStep}.
     */
    public default GraphTraversal<S, E> identity() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.identity);
        return this;
    }

    /**
     * Map any object to a fixed <code>E</code> value.
     *
     * @return the traversal with an appended {@link ConstantStep}.
     */
    public default <E2> GraphTraversal<S, E2> constant(final E2 e) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.constant, e);
        return (GraphTraversal) this;
    }

    public default GraphTraversal<S, Vertex> V(final Object... vertexIdsOrElements) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.V, vertexIdsOrElements);
        return (GraphTraversal) this;
    }

    /**
     * Map the {@link Vertex} to its adjacent vertices given a direction and edge labels.
     *
     * @param direction  the direction to traverse from the current vertex
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     */
    public default GraphTraversal<S, Vertex> to(final Direction direction, final String... edgeLabels) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.to, direction, edgeLabels);
        return (GraphTraversal) this;
    }

    /**
     * Map the {@link Vertex} to its outgoing adjacent vertices given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     */
    public default GraphTraversal<S, Vertex> out(final String... edgeLabels) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.out, edgeLabels);
        return (GraphTraversal) this;
    }

    /**
     * Map the {@link Vertex} to its incoming adjacent vertices given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     */
    public default GraphTraversal<S, Vertex> in(final String... edgeLabels) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.in, edgeLabels);
        return (GraphTraversal) this;
    }

    /**
     * Map the {@link Vertex} to its adjacent vertices given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     */
    public default GraphTraversal<S, Vertex> both(final String... edgeLabels) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.both, edgeLabels);
        return (GraphTraversal) this;
    }

    /**
     * Map the {@link Vertex} to its incident edges given the direction and edge labels.
     *
     * @param direction  the direction to traverse from the current vertex
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     */
    public default GraphTraversal<S, Edge> toE(final Direction direction, final String... edgeLabels) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.toE, direction, edgeLabels);
        return (GraphTraversal) this;
    }

    /**
     * Map the {@link Vertex} to its outgoing incident edges given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     */
    public default GraphTraversal<S, Edge> outE(final String... edgeLabels) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.outE, edgeLabels);
        return (GraphTraversal) this;
    }

    /**
     * Map the {@link Vertex} to its incoming incident edges given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     */
    public default GraphTraversal<S, Edge> inE(final String... edgeLabels) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.inE, edgeLabels);
        return (GraphTraversal) this;
    }

    /**
     * Map the {@link Vertex} to its incident edges given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     */
    public default GraphTraversal<S, Edge> bothE(final String... edgeLabels) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.bothE, edgeLabels);
        return (GraphTraversal) this;
    }

    /**
     * Map the {@link Edge} to its incident vertices given the direction.
     *
     * @param direction the direction to traverser from the current edge
     * @return the traversal with an appended {@link EdgeVertexStep}.
     */
    public default GraphTraversal<S, Vertex> toV(final Direction direction) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.toV, direction);
        return (GraphTraversal) this;
    }

    /**
     * Map the {@link Edge} to its incoming/head incident {@link Vertex}.
     *
     * @return the traversal with an appended {@link EdgeVertexStep}.
     */
    public default GraphTraversal<S, Vertex> inV() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.inV);
        return (GraphTraversal) this;
    }

    /**
     * Map the {@link Edge} to its outgoing/tail incident {@link Vertex}.
     *
     * @return the traversal with an appended {@link EdgeVertexStep}.
     */
    public default GraphTraversal<S, Vertex> outV() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.outV);
        return (GraphTraversal) this;
    }

    /**
     * Map the {@link Edge} to its incident vertices.
     *
     * @return the traversal with an appended {@link EdgeVertexStep}.
     */
    public default GraphTraversal<S, Vertex> bothV() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.bothV);
        return (GraphTraversal) this;
    }

    /**
     * Map the {@link Edge} to the incident vertex that was not just traversed from in the path history.
     *
     * @return the traversal with an appended {@link EdgeOtherVertexStep}.
     */
    public default GraphTraversal<S, Vertex> otherV() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.otherV);
        return (GraphTraversal) this;
    }

    /**
     * Order all the objects in the traversal up to this point and then emit them one-by-one in their ordered sequence.
     *
     * @return the traversal with an appended {@link OrderGlobalStep}.
     */
    public default GraphTraversal<S, E> order() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.order);
        return this;
    }

    /**
     * Order either the {@link Scope#local} object (e.g. a list, map, etc.) or the entire {@link Scope#global} traversal stream.
     *
     * @param scope whether the ordering is the current local object or the entire global stream.
     * @return the traversal with an appended {@link OrderGlobalStep} or {@link OrderLocalStep}.
     */
    public default GraphTraversal<S, E> order(final Scope scope) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.order, scope);
        return this;
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
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.properties, propertyKeys);
        return (GraphTraversal) this;
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
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.values, propertyKeys);
        return (GraphTraversal) this;
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
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.propertyMap, propertyKeys);
        return (GraphTraversal) this;
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
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.valueMap, propertyKeys);
        return (GraphTraversal) this;
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
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.valueMap, includeTokens, propertyKeys);
        return (GraphTraversal) this;
    }

    public default <E2> GraphTraversal<S, Collection<E2>> select(final Column column) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.select, column);
        return (GraphTraversal) this;
    }

    /**
     * @deprecated As of release 3.1.0, replaced by {@link GraphTraversal#select(Column)}
     */
    @Deprecated
    public default <E2> GraphTraversal<S, E2> mapValues() {
        return this.select(Column.values).unfold();
    }

    /**
     * @deprecated As of release 3.1.0, replaced by {@link GraphTraversal#select(Column)}
     */
    @Deprecated
    public default <E2> GraphTraversal<S, E2> mapKeys() {
        return this.select(Column.keys).unfold();
    }

    /**
     * Map the {@link Property} to its {@link Property#key}.
     *
     * @return the traversal with an appended {@link PropertyKeyStep}.
     */
    public default GraphTraversal<S, String> key() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.key);
        return (GraphTraversal) this;
    }

    /**
     * Map the {@link Property} to its {@link Property#value}.
     *
     * @return the traversal with an appended {@link PropertyValueStep}.
     */
    public default <E2> GraphTraversal<S, E2> value() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.value);
        return (GraphTraversal) this;
    }

    /**
     * Map the {@link Traverser} to its {@link Path} history via {@link Traverser#path}.
     *
     * @return the traversal with an appended {@link PathStep}.
     */
    public default GraphTraversal<S, Path> path() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.path);
        return (GraphTraversal) this;
    }

    /**
     * Map the {@link Traverser} to a {@link Map} of bindings as specified by the provided match traversals.
     *
     * @param matchTraversals the traversal that maintain variables which must hold for the life of the traverser
     * @param <E2>            the type of the obejcts bound in the variables
     * @return the traversal with an appended {@link MatchStep}.
     */
    public default <E2> GraphTraversal<S, Map<String, E2>> match(final Traversal<?, ?>... matchTraversals) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.match, matchTraversals);
        return (GraphTraversal) this;
    }

    /**
     * Map the {@link Traverser} to its {@link Traverser#sack} value.
     *
     * @param <E2> the sack value type
     * @return the traversal with an appended {@link SackStep}.
     */
    public default <E2> GraphTraversal<S, E2> sack() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.sack);
        return (GraphTraversal) this;
    }

    public default GraphTraversal<S, Integer> loops() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.loops);
        return (GraphTraversal) this;
    }

    public default <E2> GraphTraversal<S, Map<String, E2>> project(final String projectKey, final String... otherProjectKeys) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.project, projectKey, otherProjectKeys);
        return (GraphTraversal) this;
    }

    /**
     * Map the {@link Traverser} to a {@link Map} projection of sideEffect values, map values, and/or path values.
     *
     * @param pop             if there are multiple objects referenced in the path, the {@link Pop} to use.
     * @param selectKey1      the first key to project
     * @param selectKey2      the second key to project
     * @param otherSelectKeys the third+ keys to project
     * @param <E2>            the type of the objects projected
     * @return the traversal with an appended {@link SelectStep}.
     */
    public default <E2> GraphTraversal<S, Map<String, E2>> select(final Pop pop, final String selectKey1, final String selectKey2, String... otherSelectKeys) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.select, pop, selectKey1, selectKey2, otherSelectKeys);
        return (GraphTraversal) this;
    }

    /**
     * Map the {@link Traverser} to a {@link Map} projection of sideEffect values, map values, and/or path values.
     *
     * @param selectKey1      the first key to project
     * @param selectKey2      the second key to project
     * @param otherSelectKeys the third+ keys to project
     * @param <E2>            the type of the objects projected
     * @return the traversal with an appended {@link SelectStep}.
     */
    public default <E2> GraphTraversal<S, Map<String, E2>> select(final String selectKey1, final String selectKey2, String... otherSelectKeys) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.select, selectKey1, selectKey2, otherSelectKeys);
        return (GraphTraversal) this;
    }

    public default <E2> GraphTraversal<S, E2> select(final Pop pop, final String selectKey) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.select, pop, selectKey);
        return (GraphTraversal) this;
    }

    public default <E2> GraphTraversal<S, E2> select(final String selectKey) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.select, selectKey);
        return (GraphTraversal) this;
    }

    public default <E2> GraphTraversal<S, E2> unfold() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.unfold);
        return (GraphTraversal) this;
    }

    public default GraphTraversal<S, List<E>> fold() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.fold);
        return (GraphTraversal) this;
    }

    public default <E2> GraphTraversal<S, E2> fold(final E2 seed, final BiFunction<E2, E, E2> foldFunction) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.fold, seed, foldFunction);
        return (GraphTraversal) this;
    }

    /**
     * Map the traversal stream to its reduction as a sum of the {@link Traverser#bulk} values (i.e. count the number of traversers up to this point).
     *
     * @return the traversal with an appended {@link CountGlobalStep}.
     */
    public default GraphTraversal<S, Long> count() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.count);
        return (GraphTraversal) this;
    }

    public default GraphTraversal<S, Long> count(final Scope scope) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.count, scope);
        return (GraphTraversal) this;
    }

    /**
     * Map the traversal stream to its reduction as a sum of the {@link Traverser#get} values multiplied by their {@link Traverser#bulk} (i.e. sum the traverser values up to this point).
     *
     * @return the traversal with an appended {@link SumGlobalStep}.
     */
    public default <E2 extends Number> GraphTraversal<S, E2> sum() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.sum);
        return (GraphTraversal) this;
    }

    public default <E2 extends Number> GraphTraversal<S, E2> sum(final Scope scope) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.sum, scope);
        return (GraphTraversal) this;
    }

    public default <E2 extends Number> GraphTraversal<S, E2> max() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.max);
        return (GraphTraversal) this;
    }

    public default <E2 extends Number> GraphTraversal<S, E2> max(final Scope scope) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.max, scope);
        return (GraphTraversal) this;
    }

    public default <E2 extends Number> GraphTraversal<S, E2> min() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.min);
        return (GraphTraversal) this;
    }

    public default <E2 extends Number> GraphTraversal<S, E2> min(final Scope scope) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.min, scope);
        return (GraphTraversal) this;
    }

    public default <E2 extends Number> GraphTraversal<S, E2> mean() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.mean);
        return (GraphTraversal) this;
    }

    public default <E2 extends Number> GraphTraversal<S, E2> mean(final Scope scope) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.mean, scope);
        return (GraphTraversal) this;
    }

    public default <K, V> GraphTraversal<S, Map<K, V>> group() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.group);
        return (GraphTraversal) this;
    }

    /**
     * @deprecated As of release 3.1.0, replaced by {@link #group()}
     */
    @Deprecated
    public default <K, V> GraphTraversal<S, Map<K, V>> groupV3d0() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.groupV3d0);
        return (GraphTraversal) this;
    }

    public default <K> GraphTraversal<S, Map<K, Long>> groupCount() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.groupCount);
        return (GraphTraversal) this;
    }

    public default GraphTraversal<S, Tree> tree() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.tree);
        return (GraphTraversal) this;
    }

    public default GraphTraversal<S, Vertex> addV(final String vertexLabel) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.addV, vertexLabel);
        return (GraphTraversal) this;
    }

    public default GraphTraversal<S, Vertex> addV() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.addV);
        return (GraphTraversal) this;
    }

    /**
     * @deprecated As of release 3.1.0, replaced by {@link #addV()}
     */
    @Deprecated
    public default GraphTraversal<S, Vertex> addV(final Object... propertyKeyValues) {
        this.addV();
        for (int i = 0; i < propertyKeyValues.length; i = i + 2) {
            this.property(propertyKeyValues[i], propertyKeyValues[i + 1]);
        }
        return (GraphTraversal<S, Vertex>) this;
    }

    public default GraphTraversal<S, Edge> addE(final String edgeLabel) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.addE, edgeLabel);
        return (GraphTraversal) this;
    }

    public default GraphTraversal<S, E> to(final String toStepLabel) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.to, toStepLabel);
        return this;
    }

    public default GraphTraversal<S, E> from(final String fromStepLabel) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.from, fromStepLabel);
        return this;
    }

    public default GraphTraversal<S, E> to(final Traversal<E, Vertex> toVertex) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.to, toVertex);
        return this;
    }

    public default GraphTraversal<S, E> from(final Traversal<E, Vertex> fromVertex) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.from, fromVertex);
        return this;
    }

    /**
     * @deprecated As of release 3.1.0, replaced by {@link #addE(String)}
     */
    @Deprecated
    public default GraphTraversal<S, Edge> addE(final Direction direction, final String firstVertexKeyOrEdgeLabel, final String edgeLabelOrSecondVertexKey, final Object... propertyKeyValues) {
        if (propertyKeyValues.length % 2 == 0) {
            // addOutE("createdBy", "a")
            this.addE(firstVertexKeyOrEdgeLabel);
            if (direction.equals(Direction.OUT))
                this.to(edgeLabelOrSecondVertexKey);
            else
                this.from(edgeLabelOrSecondVertexKey);

            for (int i = 0; i < propertyKeyValues.length; i = i + 2) {
                this.property(propertyKeyValues[i], propertyKeyValues[i + 1]);
            }
            //((Mutating) this.asAdmin().getEndStep()).addPropertyMutations(propertyKeyValues);
            return (GraphTraversal<S, Edge>) this;
        } else {
            // addInE("a", "co-developer", "b", "year", 2009)
            this.addE(edgeLabelOrSecondVertexKey);
            if (direction.equals(Direction.OUT))
                this.from(firstVertexKeyOrEdgeLabel).to((String) propertyKeyValues[0]);
            else
                this.to(firstVertexKeyOrEdgeLabel).from((String) propertyKeyValues[0]);

            for (int i = 1; i < propertyKeyValues.length; i = i + 2) {
                this.property(propertyKeyValues[i], propertyKeyValues[i + 1]);
            }
            //((Mutating) this.asAdmin().getEndStep()).addPropertyMutations(Arrays.copyOfRange(propertyKeyValues, 1, propertyKeyValues.length));
            return (GraphTraversal<S, Edge>) this;
        }
    }

    /**
     * @deprecated As of release 3.1.0, replaced by {@link #addE(String)}
     */
    @Deprecated
    public default GraphTraversal<S, Edge> addOutE(final String firstVertexKeyOrEdgeLabel, final String edgeLabelOrSecondVertexKey, final Object... propertyKeyValues) {
        return this.addE(Direction.OUT, firstVertexKeyOrEdgeLabel, edgeLabelOrSecondVertexKey, propertyKeyValues);
    }

    /**
     * @deprecated As of release 3.1.0, replaced by {@link #addE(String)}
     */
    @Deprecated
    public default GraphTraversal<S, Edge> addInE(final String firstVertexKeyOrEdgeLabel, final String edgeLabelOrSecondVertexKey, final Object... propertyKeyValues) {
        return this.addE(Direction.IN, firstVertexKeyOrEdgeLabel, edgeLabelOrSecondVertexKey, propertyKeyValues);
    }

    ///////////////////// FILTER STEPS /////////////////////

    public default GraphTraversal<S, E> filter(final Predicate<Traverser<E>> predicate) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.filter, predicate);
        return this;
    }

    public default GraphTraversal<S, E> filter(final Traversal<?, ?> filterTraversal) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.filter, filterTraversal);
        return this;
    }

    public default GraphTraversal<S, E> or(final Traversal<?, ?>... orTraversals) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.or, orTraversals);
        return this;
    }

    public default GraphTraversal<S, E> and(final Traversal<?, ?>... andTraversals) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.and, andTraversals);
        return this;
    }

    public default GraphTraversal<S, E> inject(final E... injections) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.inject, injections);
        return this;
    }

    /**
     * Remove all duplicates in the traversal stream up to this point.
     *
     * @param scope       whether the deduplication is on the stream (global) or the current object (local).
     * @param dedupLabels if labels are provided, then the scope labels determine de-duplication. No labels implies current object.
     * @return the traversal with an appended {@link DedupGlobalStep}.
     */
    public default GraphTraversal<S, E> dedup(final Scope scope, final String... dedupLabels) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.dedup, scope, dedupLabels);
        return this;
    }

    /**
     * Remove all duplicates in the traversal stream up to this point.
     *
     * @param dedupLabels if labels are provided, then the scoped object's labels determine de-duplication. No labels implies current object.
     * @return the traversal with an appended {@link DedupGlobalStep}.
     */
    public default GraphTraversal<S, E> dedup(final String... dedupLabels) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.dedup, dedupLabels);
        return this;
    }

    public default GraphTraversal<S, E> where(final String startKey, final P<String> predicate) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.where, startKey, predicate);
        return this;
    }

    public default GraphTraversal<S, E> where(final P<String> predicate) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.where, predicate);
        return this;
    }

    public default GraphTraversal<S, E> where(final Traversal<?, ?> whereTraversal) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.where, whereTraversal);
        return this;
    }

    public default GraphTraversal<S, E> has(final String propertyKey, final P<?> predicate) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.has, propertyKey, predicate);
        return this;
    }

    public default GraphTraversal<S, E> has(final T accessor, final P<?> predicate) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.has, accessor, predicate);
        return this;
    }

    public default GraphTraversal<S, E> has(final String propertyKey, final Object value) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.has, propertyKey, value);
        return this;
    }

    public default GraphTraversal<S, E> has(final T accessor, final Object value) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.has, accessor, value);
        return this;
    }

    public default GraphTraversal<S, E> has(final String label, final String propertyKey, final P<?> predicate) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.has, label, propertyKey, predicate);
        return this;
    }

    public default GraphTraversal<S, E> has(final String label, final String propertyKey, final Object value) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.has, label, propertyKey, value);
        return this;
    }

    public default GraphTraversal<S, E> has(final T accessor, final Traversal<?, ?> propertyTraversal) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.has, accessor, propertyTraversal);
        return this;
    }

    public default GraphTraversal<S, E> has(final String propertyKey, final Traversal<?, ?> propertyTraversal) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.has, propertyKey, propertyTraversal);
        return this;
    }

    public default GraphTraversal<S, E> has(final String propertyKey) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.has, propertyKey);
        return this;
    }

    public default GraphTraversal<S, E> hasNot(final String propertyKey) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.hasNot, propertyKey);
        return this;
    }

    public default GraphTraversal<S, E> hasLabel(final String... labels) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.hasLabel, labels);
        return this;
    }

    public default GraphTraversal<S, E> hasId(final Object... ids) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.hasId, ids);
        return this;
    }

    public default GraphTraversal<S, E> hasKey(final String... keys) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.hasKey, keys);
        return this;
    }

    public default GraphTraversal<S, E> hasValue(final Object... values) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.hasValue, values);
        return this;
    }

    public default GraphTraversal<S, E> is(final P<E> predicate) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.is, predicate);
        return this;
    }

    /**
     * Filter the <code>E</code> object if it is not {@link P#eq} to the provided value.
     *
     * @param value the value that the object must equal.
     * @return the traversal with an appended {@link IsStep}.
     */
    public default GraphTraversal<S, E> is(final Object value) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.is, value);
        return this;
    }

    public default GraphTraversal<S, E> not(final Traversal<?, ?> notTraversal) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.not, notTraversal);
        return this;
    }

    /**
     * Filter the <code>E</code> object given a biased coin toss.
     *
     * @param probability the probability that the object will pass through
     * @return the traversal with an appended {@link CoinStep}.
     */
    public default GraphTraversal<S, E> coin(final double probability) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.coin, probability);
        return this;
    }

    public default GraphTraversal<S, E> range(final long low, final long high) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.range, low, high);
        return this;
    }

    public default <E2> GraphTraversal<S, E2> range(final Scope scope, final long low, final long high) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.range, scope, low, high);
        return (GraphTraversal) this;
    }

    public default GraphTraversal<S, E> limit(final long limit) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.limit, limit);
        return this;
    }

    public default <E2> GraphTraversal<S, E2> limit(final Scope scope, final long limit) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.limit, scope, limit);
        return (GraphTraversal) this;
    }

    public default GraphTraversal<S, E> tail() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.tail);
        return this;
    }

    public default GraphTraversal<S, E> tail(final long limit) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.tail, limit);
        return this;
    }

    public default <E2> GraphTraversal<S, E2> tail(final Scope scope) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.tail, scope);
        return (GraphTraversal) this;
    }

    public default <E2> GraphTraversal<S, E2> tail(final Scope scope, final long limit) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.tail, scope, limit);
        return (GraphTraversal) this;
    }

    /**
     * Once the first {@link Traverser} hits this step, a count down is started. Once the time limit is up, all remaining traversers are filtered out.
     *
     * @param timeLimit the count down time
     * @return the traversal with an appended {@link TimeLimitStep}
     */
    public default GraphTraversal<S, E> timeLimit(final long timeLimit) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.timeLimit, timeLimit);
        return this;
    }

    /**
     * Filter the <code>E</code> object if its {@link Traverser#path} is not {@link Path#isSimple}.
     *
     * @return the traversal with an appended {@link SimplePathStep}.
     */
    public default GraphTraversal<S, E> simplePath() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.simplePath);
        return this;
    }

    /**
     * Filter the <code>E</code> object if its {@link Traverser#path} is {@link Path#isSimple}.
     *
     * @return the traversal with an appended {@link CyclicPathStep}.
     */
    public default GraphTraversal<S, E> cyclicPath() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.cyclicPath);
        return this;
    }

    public default GraphTraversal<S, E> sample(final int amountToSample) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.sample, amountToSample);
        return this;
    }

    public default GraphTraversal<S, E> sample(final Scope scope, final int amountToSample) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.sample, scope, amountToSample);
        return this;
    }

    public default GraphTraversal<S, E> drop() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.drop);
        return this;
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public default GraphTraversal<S, E> sideEffect(final Consumer<Traverser<E>> consumer) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.sideEffect, consumer);
        return this;
    }

    public default GraphTraversal<S, E> sideEffect(final Traversal<?, ?> sideEffectTraversal) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.sideEffect, sideEffectTraversal);
        return this;
    }

    public default <E2> GraphTraversal<S, E2> cap(final String sideEffectKey, final String... sideEffectKeys) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.cap, sideEffectKey, sideEffectKeys);
        return (GraphTraversal) this;
    }

    public default GraphTraversal<S, Edge> subgraph(final String sideEffectKey) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.subgraph, sideEffectKey);
        return (GraphTraversal) this;
    }

    public default GraphTraversal<S, E> aggregate(final String sideEffectKey) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.aggregate, sideEffectKey);
        return this;
    }

    public default GraphTraversal<S, E> group(final String sideEffectKey) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.group, sideEffectKey);
        return this;
    }

    /**
     * @deprecated As of release 3.1.0, replaced by {@link #group(String)}.
     */
    public default GraphTraversal<S, E> groupV3d0(final String sideEffectKey) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.groupV3d0, sideEffectKey);
        return this;
    }

    public default GraphTraversal<S, E> groupCount(final String sideEffectKey) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.groupCount, sideEffectKey);
        return this;
    }

    public default GraphTraversal<S, E> tree(final String sideEffectKey) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.tree, sideEffectKey);
        return this;
    }

    public default <V, U> GraphTraversal<S, E> sack(final BiFunction<V, U, V> sackOperator) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.sack, sackOperator);
        return this;
    }

    /**
     * @deprecated As of release 3.1.0, replaced by {@link #sack(BiFunction)} with {@link #by(String)}.
     */
    @Deprecated
    public default <V, U> GraphTraversal<S, E> sack(final BiFunction<V, U, V> sackOperator, final String elementPropertyKey) {
        return this.sack(sackOperator).by(elementPropertyKey);
    }

    public default GraphTraversal<S, E> store(final String sideEffectKey) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.store, sideEffectKey);
        return this;
    }

    public default GraphTraversal<S, E> profile(final String sideEffectKey) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.profile, sideEffectKey);
        return this;
    }

    @Override
    public default GraphTraversal<S, TraversalMetrics> profile() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.profile);
        return (GraphTraversal) this;
    }

    /**
     * Sets a {@link Property} value and related meta properties if supplied, if supported by the {@link Graph}
     * and if the {@link Element} is a {@link VertexProperty}.  This method is the long-hand version of
     * {@link #property(Object, Object, Object...)} with the difference that the
     * {@link org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality} can be supplied.
     * <p/>
     * Generally speaking, this method will append an {@link AddPropertyStep} to the {@link Traversal} but when
     * possible, this method will attempt to fold key/value pairs into an {@link AddVertexStep}, {@link AddEdgeStep} or
     * {@link AddVertexStartStep}.  This potential optimization can only happen if cardinality is not supplied
     * and when meta-properties are not included.
     *
     * @param cardinality the specified cardinality of the property where {@code null} will allow the {@link Graph}
     *                    to use its default settings
     * @param key         the key for the property
     * @param value       the value for the property
     * @param keyValues   any meta properties to be assigned to this property
     */
    public default GraphTraversal<S, E> property(final VertexProperty.Cardinality cardinality, final Object key, final Object value, final Object... keyValues) {
        //if (null == cardinality)
         //   TraversalHelper.addStepToCreationStrategies(this.asAdmin(), key, value, keyValues);
        //else
        //    TraversalHelper.addStepToCreationStrategies(this.asAdmin(), cardinality, key, value, keyValues);
        // if it can be detected that this call to property() is related to an addV/E() then we can attempt to fold
        // the properties into that step to gain an optimization for those graphs that support such capabilities.
        if ((this.asAdmin().getEndStep() instanceof AddVertexStep || this.asAdmin().getEndStep() instanceof AddEdgeStep
                || this.asAdmin().getEndStep() instanceof AddVertexStartStep) && keyValues.length == 0 && null == cardinality) {
            ((Mutating) this.asAdmin().getEndStep()).addPropertyMutations(key, value);
        } else {
            this.asAdmin().addStep(new AddPropertyStep(this.asAdmin(), cardinality, key, value));
            ((AddPropertyStep) this.asAdmin().getEndStep()).addPropertyMutations(keyValues);
        }
        return this;
    }

    /**
     * Sets the key and value of a {@link Property}. If the {@link Element} is a {@link VertexProperty} and the
     * {@link Graph} supports it, meta properties can be set.  Use of this method assumes that the
     * {@link org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality} is defaulted to {@code null} which
     * means that the default cardinality for the {@link Graph} will be used.
     * <p/>
     * This method is effectively calls
     * {@link #property(org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality, Object, Object, Object...)}
     * as {@code property(null, key, value, keyValues}.
     *
     * @param key       the key for the property
     * @param value     the value for the property
     * @param keyValues any meta properties to be assigned to this property
     */
    public default GraphTraversal<S, E> property(final Object key, final Object value, final Object... keyValues) {
        return key instanceof VertexProperty.Cardinality ?
                this.property((VertexProperty.Cardinality) key, value, keyValues[0],
                        keyValues.length > 1 ?
                                Arrays.copyOfRange(keyValues, 1, keyValues.length) :
                                new Object[]{}) :
                this.property(null, key, value, keyValues);
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public default <M, E2> GraphTraversal<S, E2> branch(final Traversal<?, M> branchTraversal) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.branch, branchTraversal);
        return (GraphTraversal) this;
    }

    public default <M, E2> GraphTraversal<S, E2> branch(final Function<Traverser<E>, M> function) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.branch, function);
        return (GraphTraversal) this;
    }

    public default <M, E2> GraphTraversal<S, E2> choose(final Traversal<?, M> choiceTraversal) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.choose, choiceTraversal);
        return (GraphTraversal) this;
    }

    public default <E2> GraphTraversal<S, E2> choose(final Traversal<?, ?> traversalPredicate,
                                                     final Traversal<?, E2> trueChoice, final Traversal<?, E2> falseChoice) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.choose, traversalPredicate, trueChoice, falseChoice);
        return (GraphTraversal) this;
    }

    public default <M, E2> GraphTraversal<S, E2> choose(final Function<E, M> choiceFunction) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.choose, choiceFunction);
        return (GraphTraversal) this;
    }

    public default <E2> GraphTraversal<S, E2> choose(final Predicate<E> choosePredicate,
                                                     final Traversal<?, E2> trueChoice, final Traversal<?, E2> falseChoice) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.choose, choosePredicate, trueChoice, falseChoice);
        return (GraphTraversal) this;
    }

    public default GraphTraversal<S, E> optional(final Traversal<?, E> optionalTraversal) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.optional, optionalTraversal);
        return this;
    }

    public default <E2> GraphTraversal<S, E2> union(final Traversal<?, E2>... unionTraversals) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.union, unionTraversals);
        return (GraphTraversal) this;
    }

    public default <E2> GraphTraversal<S, E2> coalesce(final Traversal<?, E2>... coalesceTraversals) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.coalesce, coalesceTraversals);
        return (GraphTraversal) this;
    }

    public default GraphTraversal<S, E> repeat(final Traversal<?, E> repeatTraversal) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.repeat, repeatTraversal);
        return this;
    }

    public default GraphTraversal<S, E> emit(final Traversal<?, ?> emitTraversal) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.emit, emitTraversal);
        return this;
    }

    public default GraphTraversal<S, E> emit(final Predicate<Traverser<E>> emitPredicate) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.emit, emitPredicate);
        return this;
    }

    public default GraphTraversal<S, E> emit() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.emit);
        return this;
    }

    public default GraphTraversal<S, E> until(final Traversal<?, ?> untilTraversal) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.until, untilTraversal);
        return this;
    }

    public default GraphTraversal<S, E> until(final Predicate<Traverser<E>> untilPredicate) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.until, untilPredicate);
        return this;
    }

    public default GraphTraversal<S, E> times(final int maxLoops) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.times, maxLoops);
        return this;
    }

    public default <E2> GraphTraversal<S, E2> local(final Traversal<?, E2> localTraversal) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.local, localTraversal);
        return (GraphTraversal) this;
    }

    /////////////////// VERTEX PROGRAM STEPS ////////////////

    public default GraphTraversal<S, E> pageRank() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.pageRank);
        return this;
    }

    public default GraphTraversal<S, E> pageRank(final double alpha) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.pageRank, alpha);
        return this;
    }

    public default GraphTraversal<S, E> peerPressure() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.peerPressure);
        return this;
    }

    public default GraphTraversal<S, E> program(final VertexProgram<?> vertexProgram) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.program, vertexProgram);
        return this;
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public default GraphTraversal<S, E> as(final String stepLabel, final String... stepLabels) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.as, stepLabel, stepLabels);
        return this;
    }

    public default GraphTraversal<S, E> barrier() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.barrier);
        return this;
    }

    public default GraphTraversal<S, E> barrier(final int maxBarrierSize) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.barrier, maxBarrierSize);
        return this;
    }

    public default GraphTraversal<S, E> barrier(final Consumer<TraverserSet<Object>> barrierConsumer) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.barrier, barrierConsumer);
        return this;
    }


    //// BY-MODULATORS

    public default GraphTraversal<S, E> by() {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.by);
        return this;
    }

    public default GraphTraversal<S, E> by(final Traversal<?, ?> traversal) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.by, traversal);
        return this;
    }

    public default GraphTraversal<S, E> by(final T token) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.by, token);
        return this;
    }

    public default GraphTraversal<S, E> by(final String key) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.by, key);
        return this;
    }

    public default <V> GraphTraversal<S, E> by(final Function<V, Object> function) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.by, function);
        return this;
    }

    //// COMPARATOR BY-MODULATORS

    public default <V> GraphTraversal<S, E> by(final Traversal<?, ?> traversal, final Comparator<V> comparator) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.by, traversal, comparator);
        return this;
    }

    public default GraphTraversal<S, E> by(final Comparator<E> comparator) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.by, comparator);
        return this;
    }

    public default GraphTraversal<S, E> by(final Order order) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.by, order);
        return this;
    }

    public default <V> GraphTraversal<S, E> by(final String key, final Comparator<V> comparator) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.by, key, comparator);
        return this;
    }

    public default <U> GraphTraversal<S, E> by(final Function<U, Object> function, final Comparator comparator) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.by, function, comparator);
        return this;
    }

    ////

    public default <M, E2> GraphTraversal<S, E> option(final M pickToken, final Traversal<E, E2> traversalOption) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.option, pickToken, traversalOption);
        return this;
    }

    public default <E2> GraphTraversal<S, E> option(final Traversal<E, E2> traversalOption) {
        this.asAdmin().getStrategies().getTranslator().addStep(this.asAdmin(), Symbols.option, traversalOption);
        return this;
    }

    ////

    @Override
    public default GraphTraversal<S, E> iterate() {
        Traversal.super.iterate();
        return this;
    }
}
