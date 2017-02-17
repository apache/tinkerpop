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
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRankVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PeerPressureVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ProgramVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ColumnTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.FunctionTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.LoopTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.PredicateTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TrueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.TimesModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.CoinStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep;
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
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CoalesceStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ConstantStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.DedupLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupCountStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStepV3d0;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LabelStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaCollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaFlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LoopsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MinGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MinLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProjectStep;
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
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupSideEffectStepV3d0;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.LambdaSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StoreStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TraversalSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
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
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.function.ConstantSupplier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
     * Map a {@link Traverser} referencing an object of type <code>E</code> to an object of type <code>E2</code>.
     *
     * @param function the lambda expression that does the functional mapping
     * @return the traversal with an appended {@link LambdaMapStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#general-steps">Reference Documentation - General Steps</a>
     */
    public default <E2> GraphTraversal<S, E2> map(final Function<Traverser<E>, E2> function) {
        this.asAdmin().getBytecode().addStep(Symbols.map, function);
        return this.asAdmin().addStep(new LambdaMapStep<>(this.asAdmin(), function));
    }

    /**
     * Map a {@link Traverser} referencing an object of type <code>E</code> to an object of type <code>E2</code>.
     *
     * @param mapTraversal the traversal expression that does the functional mapping
     * @return the traversal with an appended {@link LambdaMapStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#general-steps">Reference Documentation - General Steps</a>
     */
    public default <E2> GraphTraversal<S, E2> map(final Traversal<?, E2> mapTraversal) {
        this.asAdmin().getBytecode().addStep(Symbols.map, mapTraversal);
        return this.asAdmin().addStep(new TraversalMapStep<>(this.asAdmin(), mapTraversal));
    }

    /**
     * Map a {@link Traverser} referencing an object of type <code>E</code> to an iterator of objects of type <code>E2</code>.
     * The resultant iterator is drained one-by-one before a new <code>E</code> object is pulled in for processing.
     *
     * @param function the lambda expression that does the functional mapping
     * @param <E2>     the type of the returned iterator objects
     * @return the traversal with an appended {@link LambdaFlatMapStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#general-steps">Reference Documentation - General Steps</a>
     */
    public default <E2> GraphTraversal<S, E2> flatMap(final Function<Traverser<E>, Iterator<E2>> function) {
        this.asAdmin().getBytecode().addStep(Symbols.flatMap, function);
        return this.asAdmin().addStep(new LambdaFlatMapStep<>(this.asAdmin(), function));
    }

    /**
     * Map a {@link Traverser} referencing an object of type <code>E</code> to an iterator of objects of type <code>E2</code>.
     * The internal traversal is drained one-by-one before a new <code>E</code> object is pulled in for processing.
     *
     * @param flatMapTraversal the traversal generating objects of type <code>E2</code>
     * @param <E2>             the end type of the internal traversal
     * @return the traversal with an appended {@link TraversalFlatMapStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#general-steps">Reference Documentation - General Steps</a>
     */
    public default <E2> GraphTraversal<S, E2> flatMap(final Traversal<?, E2> flatMapTraversal) {
        this.asAdmin().getBytecode().addStep(Symbols.flatMap, flatMapTraversal);
        return this.asAdmin().addStep(new TraversalFlatMapStep<>(this.asAdmin(), flatMapTraversal));
    }

    /**
     * Map the {@link Element} to its {@link Element#id}.
     *
     * @return the traversal with an appended {@link IdStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#id-step">Reference Documentation - Id Step</a>
     */
    public default GraphTraversal<S, Object> id() {
        this.asAdmin().getBytecode().addStep(Symbols.id);
        return this.asAdmin().addStep(new IdStep<>(this.asAdmin()));
    }

    /**
     * Map the {@link Element} to its {@link Element#label}.
     *
     * @return the traversal with an appended {@link LabelStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#label-step">Reference Documentation - Label Step</a>
     */
    public default GraphTraversal<S, String> label() {
        this.asAdmin().getBytecode().addStep(Symbols.label);
        return this.asAdmin().addStep(new LabelStep<>(this.asAdmin()));
    }

    /**
     * Map the <code>E</code> object to itself. In other words, a "no op."
     *
     * @return the traversal with an appended {@link IdentityStep}.
     */
    public default GraphTraversal<S, E> identity() {
        this.asAdmin().getBytecode().addStep(Symbols.identity);
        return this.asAdmin().addStep(new IdentityStep<>(this.asAdmin()));
    }

    /**
     * Map any object to a fixed <code>E</code> value.
     *
     * @return the traversal with an appended {@link ConstantStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#constant-step">Reference Documentation - Constant Step</a>
     */
    public default <E2> GraphTraversal<S, E2> constant(final E2 e) {
        this.asAdmin().getBytecode().addStep(Symbols.constant, e);
        return this.asAdmin().addStep(new ConstantStep<E, E2>(this.asAdmin(), e));
    }

    /**
     * A {@code V} step is usually used to start a traversal but it may also be used mid-traversal.
     *
     * @param vertexIdsOrElements vertices to inject into the traversal
     * @return the traversal with an appended {@link GraphStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#graph-step">Reference Documentation - Graph Step</a>
     */
    public default GraphTraversal<S, Vertex> V(final Object... vertexIdsOrElements) {
        this.asAdmin().getBytecode().addStep(Symbols.V, vertexIdsOrElements);
        return this.asAdmin().addStep(new GraphStep<>(this.asAdmin(), Vertex.class, false, vertexIdsOrElements));
    }

    /**
     * Map the {@link Vertex} to its adjacent vertices given a direction and edge labels.
     *
     * @param direction  the direction to traverse from the current vertex
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps">Reference Documentation - Vertex Step</a>
     */
    public default GraphTraversal<S, Vertex> to(final Direction direction, final String... edgeLabels) {
        this.asAdmin().getBytecode().addStep(Symbols.to, direction, edgeLabels);
        return this.asAdmin().addStep(new VertexStep<>(this.asAdmin(), Vertex.class, direction, edgeLabels));
    }

    /**
     * Map the {@link Vertex} to its outgoing adjacent vertices given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps">Reference Documentation - Vertex Step</a>
     */
    public default GraphTraversal<S, Vertex> out(final String... edgeLabels) {
        this.asAdmin().getBytecode().addStep(Symbols.out, edgeLabels);
        return this.asAdmin().addStep(new VertexStep<>(this.asAdmin(), Vertex.class, Direction.OUT, edgeLabels));
    }

    /**
     * Map the {@link Vertex} to its incoming adjacent vertices given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps">Reference Documentation - Vertex Step</a>
     */
    public default GraphTraversal<S, Vertex> in(final String... edgeLabels) {
        this.asAdmin().getBytecode().addStep(Symbols.in, edgeLabels);
        return this.asAdmin().addStep(new VertexStep<>(this.asAdmin(), Vertex.class, Direction.IN, edgeLabels));
    }

    /**
     * Map the {@link Vertex} to its adjacent vertices given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps">Reference Documentation - Vertex Step</a>
     */
    public default GraphTraversal<S, Vertex> both(final String... edgeLabels) {
        this.asAdmin().getBytecode().addStep(Symbols.both, edgeLabels);
        return this.asAdmin().addStep(new VertexStep<>(this.asAdmin(), Vertex.class, Direction.BOTH, edgeLabels));
    }

    /**
     * Map the {@link Vertex} to its incident edges given the direction and edge labels.
     *
     * @param direction  the direction to traverse from the current vertex
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps">Reference Documentation - Vertex Step</a>
     */
    public default GraphTraversal<S, Edge> toE(final Direction direction, final String... edgeLabels) {
        this.asAdmin().getBytecode().addStep(Symbols.toE, direction, edgeLabels);
        return this.asAdmin().addStep(new VertexStep<>(this.asAdmin(), Edge.class, direction, edgeLabels));
    }

    /**
     * Map the {@link Vertex} to its outgoing incident edges given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps">Reference Documentation - Vertex Step</a>
     */
    public default GraphTraversal<S, Edge> outE(final String... edgeLabels) {
        this.asAdmin().getBytecode().addStep(Symbols.outE, edgeLabels);
        return this.asAdmin().addStep(new VertexStep<>(this.asAdmin(), Edge.class, Direction.OUT, edgeLabels));
    }

    /**
     * Map the {@link Vertex} to its incoming incident edges given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps">Reference Documentation - Vertex Step</a>
     */
    public default GraphTraversal<S, Edge> inE(final String... edgeLabels) {
        this.asAdmin().getBytecode().addStep(Symbols.inE, edgeLabels);
        return this.asAdmin().addStep(new VertexStep<>(this.asAdmin(), Edge.class, Direction.IN, edgeLabels));
    }

    /**
     * Map the {@link Vertex} to its incident edges given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps">Reference Documentation - Vertex Step</a>
     */
    public default GraphTraversal<S, Edge> bothE(final String... edgeLabels) {
        this.asAdmin().getBytecode().addStep(Symbols.bothE, edgeLabels);
        return this.asAdmin().addStep(new VertexStep<>(this.asAdmin(), Edge.class, Direction.BOTH, edgeLabels));
    }

    /**
     * Map the {@link Edge} to its incident vertices given the direction.
     *
     * @param direction the direction to traverser from the current edge
     * @return the traversal with an appended {@link EdgeVertexStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps">Reference Documentation - Vertex Step</a>
     */
    public default GraphTraversal<S, Vertex> toV(final Direction direction) {
        this.asAdmin().getBytecode().addStep(Symbols.toV, direction);
        return this.asAdmin().addStep(new EdgeVertexStep(this.asAdmin(), direction));
    }

    /**
     * Map the {@link Edge} to its incoming/head incident {@link Vertex}.
     *
     * @return the traversal with an appended {@link EdgeVertexStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps">Reference Documentation - Vertex Step</a>
     */
    public default GraphTraversal<S, Vertex> inV() {
        this.asAdmin().getBytecode().addStep(Symbols.inV);
        return this.asAdmin().addStep(new EdgeVertexStep(this.asAdmin(), Direction.IN));
    }

    /**
     * Map the {@link Edge} to its outgoing/tail incident {@link Vertex}.
     *
     * @return the traversal with an appended {@link EdgeVertexStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps">Reference Documentation - Vertex Step</a>
     */
    public default GraphTraversal<S, Vertex> outV() {
        this.asAdmin().getBytecode().addStep(Symbols.outV);
        return this.asAdmin().addStep(new EdgeVertexStep(this.asAdmin(), Direction.OUT));
    }

    /**
     * Map the {@link Edge} to its incident vertices.
     *
     * @return the traversal with an appended {@link EdgeVertexStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps">Reference Documentation - Vertex Step</a>
     */
    public default GraphTraversal<S, Vertex> bothV() {
        this.asAdmin().getBytecode().addStep(Symbols.bothV);
        return this.asAdmin().addStep(new EdgeVertexStep(this.asAdmin(), Direction.BOTH));
    }

    /**
     * Map the {@link Edge} to the incident vertex that was not just traversed from in the path history.
     *
     * @return the traversal with an appended {@link EdgeOtherVertexStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps">Reference Documentation - Vertex Step</a>
     */
    public default GraphTraversal<S, Vertex> otherV() {
        this.asAdmin().getBytecode().addStep(Symbols.otherV);
        return this.asAdmin().addStep(new EdgeOtherVertexStep(this.asAdmin()));
    }

    /**
     * Order all the objects in the traversal up to this point and then emit them one-by-one in their ordered sequence.
     *
     * @return the traversal with an appended {@link OrderGlobalStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#order-step">Reference Documentation - Order Step</a>
     */
    public default GraphTraversal<S, E> order() {
        this.asAdmin().getBytecode().addStep(Symbols.order);
        return this.asAdmin().addStep(new OrderGlobalStep<>(this.asAdmin()));
    }

    /**
     * Order either the {@link Scope#local} object (e.g. a list, map, etc.) or the entire {@link Scope#global} traversal stream.
     *
     * @param scope whether the ordering is the current local object or the entire global stream.
     * @return the traversal with an appended {@link OrderGlobalStep} or {@link OrderLocalStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#order-step">Reference Documentation - Order Step</a>
     */
    public default GraphTraversal<S, E> order(final Scope scope) {
        this.asAdmin().getBytecode().addStep(Symbols.order, scope);
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new OrderGlobalStep<>(this.asAdmin()) : new OrderLocalStep<>(this.asAdmin()));
    }

    /**
     * Map the {@link Element} to its associated properties given the provide property keys.
     * If no property keys are provided, then all properties are emitted.
     *
     * @param propertyKeys the properties to retrieve
     * @param <E2>         the value type of the returned properties
     * @return the traversal with an appended {@link PropertiesStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#properties-step">Reference Documentation - Properties Step</a>
     */
    public default <E2> GraphTraversal<S, ? extends Property<E2>> properties(final String... propertyKeys) {
        this.asAdmin().getBytecode().addStep(Symbols.properties, propertyKeys);
        return this.asAdmin().addStep(new PropertiesStep<>(this.asAdmin(), PropertyType.PROPERTY, propertyKeys));
    }

    /**
     * Map the {@link Element} to the values of the associated properties given the provide property keys.
     * If no property keys are provided, then all property values are emitted.
     *
     * @param propertyKeys the properties to retrieve their value from
     * @param <E2>         the value type of the properties
     * @return the traversal with an appended {@link PropertiesStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#values-step">Reference Documentation - Values Step</a>
     */
    public default <E2> GraphTraversal<S, E2> values(final String... propertyKeys) {
        this.asAdmin().getBytecode().addStep(Symbols.values, propertyKeys);
        return this.asAdmin().addStep(new PropertiesStep<>(this.asAdmin(), PropertyType.VALUE, propertyKeys));
    }

    /**
     * Map the {@link Element} to a {@link Map} of the properties key'd according to their {@link Property#key}.
     * If no property keys are provided, then all properties are retrieved.
     *
     * @param propertyKeys the properties to retrieve
     * @param <E2>         the value type of the returned properties
     * @return the traversal with an appended {@link PropertyMapStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#propertymap-step">Reference Documentation - PropertyMap Step</a>
     */
    public default <E2> GraphTraversal<S, Map<String, E2>> propertyMap(final String... propertyKeys) {
        this.asAdmin().getBytecode().addStep(Symbols.propertyMap, propertyKeys);
        return this.asAdmin().addStep(new PropertyMapStep<>(this.asAdmin(), false, PropertyType.PROPERTY, propertyKeys));
    }

    /**
     * Map the {@link Element} to a {@link Map} of the property values key'd according to their {@link Property#key}.
     * If no property keys are provided, then all property values are retrieved.
     *
     * @param propertyKeys the properties to retrieve
     * @param <E2>         the value type of the returned properties
     * @return the traversal with an appended {@link PropertyMapStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#valuemap-step">Reference Documentation - ValueMap Step</a>
     */
    public default <E2> GraphTraversal<S, Map<String, E2>> valueMap(final String... propertyKeys) {
        this.asAdmin().getBytecode().addStep(Symbols.valueMap, propertyKeys);
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
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#valuemap-step">Reference Documentation - ValueMap Step</a>
     */
    public default <E2> GraphTraversal<S, Map<String, E2>> valueMap(final boolean includeTokens, final String... propertyKeys) {
        this.asAdmin().getBytecode().addStep(Symbols.valueMap, includeTokens, propertyKeys);
        return this.asAdmin().addStep(new PropertyMapStep<>(this.asAdmin(), includeTokens, PropertyType.VALUE, propertyKeys));
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
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#key-step">Reference Documentation - Key Step</a>
     */
    public default GraphTraversal<S, String> key() {
        this.asAdmin().getBytecode().addStep(Symbols.key);
        return this.asAdmin().addStep(new PropertyKeyStep(this.asAdmin()));
    }

    /**
     * Map the {@link Property} to its {@link Property#value}.
     *
     * @return the traversal with an appended {@link PropertyValueStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#value-step">Reference Documentation - Value Step</a>
     */
    public default <E2> GraphTraversal<S, E2> value() {
        this.asAdmin().getBytecode().addStep(Symbols.value);
        return this.asAdmin().addStep(new PropertyValueStep<>(this.asAdmin()));
    }

    /**
     * Map the {@link Traverser} to its {@link Path} history via {@link Traverser#path}.
     *
     * @return the traversal with an appended {@link PathStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#path-step">Reference Documentation - Path Step</a>
     */
    public default GraphTraversal<S, Path> path() {
        this.asAdmin().getBytecode().addStep(Symbols.path);
        return this.asAdmin().addStep(new PathStep<>(this.asAdmin()));
    }

    /**
     * Map the {@link Traverser} to a {@link Map} of bindings as specified by the provided match traversals.
     *
     * @param matchTraversals the traversal that maintain variables which must hold for the life of the traverser
     * @param <E2>            the type of the obejcts bound in the variables
     * @return the traversal with an appended {@link MatchStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#match-step">Reference Documentation - Match Step</a>
     */
    public default <E2> GraphTraversal<S, Map<String, E2>> match(final Traversal<?, ?>... matchTraversals) {
        this.asAdmin().getBytecode().addStep(Symbols.match, matchTraversals);
        return this.asAdmin().addStep(new MatchStep<>(this.asAdmin(), ConnectiveStep.Connective.AND, matchTraversals));
    }

    /**
     * Map the {@link Traverser} to its {@link Traverser#sack} value.
     *
     * @param <E2> the sack value type
     * @return the traversal with an appended {@link SackStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#sack-step">Reference Documentation - Sack Step</a>
     */
    public default <E2> GraphTraversal<S, E2> sack() {
        this.asAdmin().getBytecode().addStep(Symbols.sack);
        return this.asAdmin().addStep(new SackStep<>(this.asAdmin()));
    }

    /**
     * If the {@link Traverser} supports looping then calling this method will extract the number of loops for that
     * traverser.
     *
     * @return the traversal with an appended {@link LoopsStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#loops-step">Reference Documentation - Loops Step</a>
     */
    public default GraphTraversal<S, Integer> loops() {
        this.asAdmin().getBytecode().addStep(Symbols.loops);
        return this.asAdmin().addStep(new LoopsStep<>(this.asAdmin()));
    }

    /**
     * Projects the current object in the stream into a {@code Map} that is keyed by the provided labels.
     *
     * @return the traversal with an appended {@link ProjectStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#project-step">Reference Documentation - Project Step</a>
     */
    public default <E2> GraphTraversal<S, Map<String, E2>> project(final String projectKey, final String... otherProjectKeys) {
        final String[] projectKeys = new String[otherProjectKeys.length + 1];
        projectKeys[0] = projectKey;
        System.arraycopy(otherProjectKeys, 0, projectKeys, 1, otherProjectKeys.length);
        this.asAdmin().getBytecode().addStep(Symbols.project, projectKey, otherProjectKeys);
        return this.asAdmin().addStep(new ProjectStep<>(this.asAdmin(), projectKeys));
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
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#select-step">Reference Documentation - Select Step</a>
     */
    public default <E2> GraphTraversal<S, Map<String, E2>> select(final Pop pop, final String selectKey1, final String selectKey2, String... otherSelectKeys) {
        final String[] selectKeys = new String[otherSelectKeys.length + 2];
        selectKeys[0] = selectKey1;
        selectKeys[1] = selectKey2;
        System.arraycopy(otherSelectKeys, 0, selectKeys, 2, otherSelectKeys.length);
        this.asAdmin().getBytecode().addStep(Symbols.select, pop, selectKey1, selectKey2, otherSelectKeys);
        return this.asAdmin().addStep(new SelectStep<>(this.asAdmin(), pop, selectKeys));
    }

    /**
     * Map the {@link Traverser} to a {@link Map} projection of sideEffect values, map values, and/or path values.
     *
     * @param selectKey1      the first key to project
     * @param selectKey2      the second key to project
     * @param otherSelectKeys the third+ keys to project
     * @param <E2>            the type of the objects projected
     * @return the traversal with an appended {@link SelectStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#select-step">Reference Documentation - Select Step</a>
     */
    public default <E2> GraphTraversal<S, Map<String, E2>> select(final String selectKey1, final String selectKey2, String... otherSelectKeys) {
        final String[] selectKeys = new String[otherSelectKeys.length + 2];
        selectKeys[0] = selectKey1;
        selectKeys[1] = selectKey2;
        System.arraycopy(otherSelectKeys, 0, selectKeys, 2, otherSelectKeys.length);
        this.asAdmin().getBytecode().addStep(Symbols.select, selectKey1, selectKey2, otherSelectKeys);
        return this.asAdmin().addStep(new SelectStep<>(this.asAdmin(), null, selectKeys));
    }

    /**
     * Map the {@link Traverser} to the object specified by the {@code selectKey} and apply the {@link Pop} operation
     * to it.
     *
     * @param selectKey the key to project
     * @return the traversal with an appended {@link SelectStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#select-step">Reference Documentation - Select Step</a>
     */
    public default <E2> GraphTraversal<S, E2> select(final Pop pop, final String selectKey) {
        this.asAdmin().getBytecode().addStep(Symbols.select, pop, selectKey);
        return this.asAdmin().addStep(new SelectOneStep<>(this.asAdmin(), pop, selectKey));
    }

    /**
     * Map the {@link Traverser} to the object specified by the {@code selectKey}. Note that unlike other uses of
     * {@code select} where there are multiple keys, this use of {@code select} with a single key does not produce a
     * {@code Map}.
     *
     * @param selectKey the key to project
     * @return the traversal with an appended {@link SelectStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#select-step">Reference Documentation - Select Step</a>
     */
    public default <E2> GraphTraversal<S, E2> select(final String selectKey) {
        this.asAdmin().getBytecode().addStep(Symbols.select, selectKey);
        return this.asAdmin().addStep(new SelectOneStep<>(this.asAdmin(), null, selectKey));
    }

    /**
     * A version of {@code select} that allows for the extraction of a {@link Column} from objects in the traversal.
     *
     * @param column the column to extract
     * @return the traversal with an appended {@link TraversalMapStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#select-step">Reference Documentation - Select Step</a>
     */
    public default <E2> GraphTraversal<S, Collection<E2>> select(final Column column) {
        this.asAdmin().getBytecode().addStep(Symbols.select, column);
        return this.asAdmin().addStep(new TraversalMapStep<>(this.asAdmin(), new ColumnTraversal(column)));
    }

    /**
     * Unrolls a {@code Iterator}, {@code Iterable} or {@code Map} into a linear form or simply emits the object if it
     * is not one of those types.
     *
     * @return the traversal with an appended {@link UnfoldStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#unfold-step">Reference Documentation - Unfold Step</a>
     */
    public default <E2> GraphTraversal<S, E2> unfold() {
        this.asAdmin().getBytecode().addStep(Symbols.unfold);
        return this.asAdmin().addStep(new UnfoldStep<>(this.asAdmin()));
    }

    /**
     * Rolls up objects in the stream into an aggregate list.
     *
     * @return the traversal with an appended {@link FoldStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#fold-step">Reference Documentation - Fold Step</a>
     */
    public default GraphTraversal<S, List<E>> fold() {
        this.asAdmin().getBytecode().addStep(Symbols.fold);
        return this.asAdmin().addStep(new FoldStep<>(this.asAdmin()));
    }

    /**
     * Rolls up objects in the stream into an aggregate value as defined by a {@code seed} and {@code BiFunction}.
     *
     * @param seed the value to provide as the first argument to the {@code foldFunction}
     * @param foldFunction the function to fold by where the first argument is the {@code seed} or the value returned from subsequent calss and
     *                     the second argument is the value from the stream
     * @return the traversal with an appended {@link FoldStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#fold-step">Reference Documentation - Fold Step</a>
     */
    public default <E2> GraphTraversal<S, E2> fold(final E2 seed, final BiFunction<E2, E, E2> foldFunction) {
        this.asAdmin().getBytecode().addStep(Symbols.fold, seed, foldFunction);
        return this.asAdmin().addStep(new FoldStep<>(this.asAdmin(), new ConstantSupplier<>(seed), foldFunction)); // TODO: User should provide supplier?
    }

    /**
     * Map the traversal stream to its reduction as a sum of the {@link Traverser#bulk} values (i.e. count the number
     * of traversers up to this point).
     *
     * @return the traversal with an appended {@link CountGlobalStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#count-step">Reference Documentation - Count Step</a>
     */
    public default GraphTraversal<S, Long> count() {
        this.asAdmin().getBytecode().addStep(Symbols.count);
        return this.asAdmin().addStep(new CountGlobalStep<>(this.asAdmin()));
    }

    /**
     * Map the traversal stream to its reduction as a sum of the {@link Traverser#bulk} values given the specified
     * {@link Scope} (i.e. count the number of traversers up to this point).
     *
     * @return the traversal with an appended {@link CountGlobalStep} or {@link CountLocalStep} depending on the {@link Scope}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#count-step">Reference Documentation - Count Step</a>
     */
    public default GraphTraversal<S, Long> count(final Scope scope) {
        this.asAdmin().getBytecode().addStep(Symbols.count, scope);
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new CountGlobalStep<>(this.asAdmin()) : new CountLocalStep<>(this.asAdmin()));
    }

    /**
     * Map the traversal stream to its reduction as a sum of the {@link Traverser#get} values multiplied by their
     * {@link Traverser#bulk} (i.e. sum the traverser values up to this point).
     *
     * @return the traversal with an appended {@link SumGlobalStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#sum-step">Reference Documentation - Sum Step</a>
     */
    public default <E2 extends Number> GraphTraversal<S, E2> sum() {
        this.asAdmin().getBytecode().addStep(Symbols.sum);
        return this.asAdmin().addStep(new SumGlobalStep<>(this.asAdmin()));
    }

    /**
     * Map the traversal stream to its reduction as a sum of the {@link Traverser#get} values multiplied by their
     * {@link Traverser#bulk} given the specified {@link Scope} (i.e. sum the traverser values up to this point).
     *
     * @return the traversal with an appended {@link SumGlobalStep} or {@link SumLocalStep} depending on the {@link Scope}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#sum-step">Reference Documentation - Sum Step</a>
     */
    public default <E2 extends Number> GraphTraversal<S, E2> sum(final Scope scope) {
        this.asAdmin().getBytecode().addStep(Symbols.sum, scope);
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new SumGlobalStep<>(this.asAdmin()) : new SumLocalStep(this.asAdmin()));
    }

    /**
     * Determines the largest value in the stream.
     *
     * @return the traversal with an appended {@link MaxGlobalStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#max-step">Reference Documentation - Max Step</a>
     */
    public default <E2 extends Number> GraphTraversal<S, E2> max() {
        this.asAdmin().getBytecode().addStep(Symbols.max);
        return this.asAdmin().addStep(new MaxGlobalStep<>(this.asAdmin()));
    }

    /**
     * Determines the largest value in the stream given the {@link Scope}.
     *
     * @return the traversal with an appended {@link MaxGlobalStep} or {@link MaxLocalStep} depending on the {@link Scope}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#max-step">Reference Documentation - Max Step</a>
     */
    public default <E2 extends Number> GraphTraversal<S, E2> max(final Scope scope) {
        this.asAdmin().getBytecode().addStep(Symbols.max, scope);
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new MaxGlobalStep<>(this.asAdmin()) : new MaxLocalStep(this.asAdmin()));
    }

    /**
     * Determines the smallest value in the stream.
     *
     * @return the traversal with an appended {@link MinGlobalStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#min-step">Reference Documentation - Min Step</a>
     */
    public default <E2 extends Number> GraphTraversal<S, E2> min() {
        this.asAdmin().getBytecode().addStep(Symbols.min);
        return this.asAdmin().addStep(new MinGlobalStep<>(this.asAdmin()));
    }

    /**
     * Determines the smallest value in the stream given the {@link Scope}.
     *
     * @return the traversal with an appended {@link MinGlobalStep} or {@link MinLocalStep} depending on the {@link Scope}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#min-step">Reference Documentation - Min Step</a>
     */
    public default <E2 extends Number> GraphTraversal<S, E2> min(final Scope scope) {
        this.asAdmin().getBytecode().addStep(Symbols.min, scope);
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new MinGlobalStep<E2>(this.asAdmin()) : new MinLocalStep<>(this.asAdmin()));
    }

    /**
     * Determines the mean value in the stream.
     *
     * @return the traversal with an appended {@link MeanGlobalStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#mean-step">Reference Documentation - Mean Step</a>
     */
    public default <E2 extends Number> GraphTraversal<S, E2> mean() {
        this.asAdmin().getBytecode().addStep(Symbols.mean);
        return this.asAdmin().addStep(new MeanGlobalStep<>(this.asAdmin()));
    }

    /**
     * Determines the mean value in the stream given the {@link Scope}.
     *
     * @return the traversal with an appended {@link MeanGlobalStep} or {@link MeanLocalStep} depending on the {@link Scope}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#mean-step">Reference Documentation - Mean Step</a>
     */
    public default <E2 extends Number> GraphTraversal<S, E2> mean(final Scope scope) {
        this.asAdmin().getBytecode().addStep(Symbols.mean, scope);
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new MeanGlobalStep<>(this.asAdmin()) : new MeanLocalStep(this.asAdmin()));
    }

    /**
     * Organize objects in the stream into a {@code Map}. Calls to {@code group()} are typically accompanied with
     * {@link #by()} modulators which help specify how the grouping should occur.
     *
     * @return the traversal with an appended {@link GroupStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#group-step">Reference Documentation - Group Step</a>
     */
    public default <K, V> GraphTraversal<S, Map<K, V>> group() {
        this.asAdmin().getBytecode().addStep(Symbols.group);
        return this.asAdmin().addStep(new GroupStep<>(this.asAdmin()));
    }

    /**
     * @deprecated As of release 3.1.0, replaced by {@link #group()}
     */
    @Deprecated
    public default <K, V> GraphTraversal<S, Map<K, V>> groupV3d0() {
        this.asAdmin().getBytecode().addStep(Symbols.groupV3d0);
        return this.asAdmin().addStep(new GroupStepV3d0<>(this.asAdmin()));
    }

    /**
     * Counts the number of times a particular objects has been part of a traversal, returning a {@code Map} where the
     * object is the key and the value is the count.
     *
     * @return the traversal with an appended {@link GroupCountStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#groupcount-step">Reference Documentation - GroupCount Step</a>
     */
    public default <K> GraphTraversal<S, Map<K, Long>> groupCount() {
        this.asAdmin().getBytecode().addStep(Symbols.groupCount);
        return this.asAdmin().addStep(new GroupCountStep<>(this.asAdmin()));
    }

    /**
     * Aggregates the emanating paths into a {@link Tree} data structure.
     *
     * @return the traversal with an appended {@link TreeStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#tree-step">Reference Documentation - Tree Step</a>
     */
    public default GraphTraversal<S, Tree> tree() {
        this.asAdmin().getBytecode().addStep(Symbols.tree);
        return this.asAdmin().addStep(new TreeStep<>(this.asAdmin()));
    }

    /**
     * Adds a {@link Vertex}.
     *
     * @param vertexLabel the label of the {@link Vertex} to add
     * @return the traversal with the {@link AddVertexStep} added
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#addvertex-step">Reference Documentation - AddVertex Step</a>
     */
    public default GraphTraversal<S, Vertex> addV(final String vertexLabel) {
        this.asAdmin().getBytecode().addStep(Symbols.addV, vertexLabel);
        return this.asAdmin().addStep(new AddVertexStep<>(this.asAdmin(), vertexLabel));
    }

    /**
     * Adds a {@link Vertex} with a default vertex label.
     *
     * @return the traversal with the {@link AddVertexStep} added
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#addvertex-step">Reference Documentation - AddVertex Step</a>
     */
    public default GraphTraversal<S, Vertex> addV() {
        this.asAdmin().getBytecode().addStep(Symbols.addV);
        return this.asAdmin().addStep(new AddVertexStep<>(this.asAdmin(), null));
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
        //((AddVertexStep) this.asAdmin().getEndStep()).addPropertyMutations(propertyKeyValues);
        return (GraphTraversal<S, Vertex>) this;
    }

    /**
     * Adds an {@link Edge} with the specified edge label.
     *
     * @param edgeLabel the label of the newly added edge
     * @return the traversal with the {@link AddEdgeStep} added
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#addedge-step">Reference Documentation - AddEdge Step</a>
     */
    public default GraphTraversal<S, Edge> addE(final String edgeLabel) {
        this.asAdmin().getBytecode().addStep(Symbols.addE, edgeLabel);
        return this.asAdmin().addStep(new AddEdgeStep<>(this.asAdmin(), edgeLabel));
    }

    /**
     * When used as a modifier to {@link #addE(String)} this method specifies the step label to use for selecting the
     * incoming vertex of the newly added {@link Edge}.
     *
     * @param toStepLabel the step label of the incoming vertex
     * @return the traversal with the modified {@link AddEdgeStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#addedge-step">Reference Documentation - AddEdge Step</a>
     */
    public default GraphTraversal<S, E> to(final String toStepLabel) {
        this.asAdmin().getBytecode().addStep(Symbols.to, toStepLabel);
        ((AddEdgeStep) this.asAdmin().getEndStep()).addTo(__.select(toStepLabel));
        return this;
    }

    /**
     * When used as a modifier to {@link #addE(String)} this method specifies the step label to use for selecting the
     * outgoing vertex of the newly added {@link Edge}.
     *
     * @param fromStepLabel the step label of the outgoing vertex
     * @return the traversal with the modified {@link AddEdgeStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#addedge-step">Reference Documentation - AddEdge Step</a>
     */
    public default GraphTraversal<S, E> from(final String fromStepLabel) {
        this.asAdmin().getBytecode().addStep(Symbols.from, fromStepLabel);
        ((AddEdgeStep) this.asAdmin().getEndStep()).addFrom(__.select(fromStepLabel));
        return this;
    }

    /**
     * When used as a modifier to {@link #addE(String)} this method specifies the traversal to use for selecting the
     * incoming vertex of the newly added {@link Edge}.
     *
     * @param toVertex the traversal for selecting the incoming vertex
     * @return the traversal with the modified {@link AddEdgeStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#addedge-step">Reference Documentation - AddEdge Step</a>
     */
    public default GraphTraversal<S, E> to(final Traversal<E, Vertex> toVertex) {
        this.asAdmin().getBytecode().addStep(Symbols.to, toVertex);
        ((AddEdgeStep) this.asAdmin().getEndStep()).addTo(toVertex);
        return this;
    }

    /**
     * When used as a modifier to {@link #addE(String)} this method specifies the traversal to use for selecting the
     * outgoing vertex of the newly added {@link Edge}.
     *
     * @param fromVertex the traversal for selecting the outgoing vertex
     * @return the traversal with the modified {@link AddEdgeStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#addedge-step">Reference Documentation - AddEdge Step</a>
     */
    public default GraphTraversal<S, E> from(final Traversal<E, Vertex> fromVertex) {
        this.asAdmin().getBytecode().addStep(Symbols.from, fromVertex);
        ((AddEdgeStep) this.asAdmin().getEndStep()).addFrom(fromVertex);
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
            // addInE("a", "codeveloper", "b", "year", 2009)
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

    /**
     * Map the {@link Traverser} to either {@code true} or {@code false}, where {@code false} will not pass the
     * traverser to the next step.
     *
     * @param predicate the filter function to apply
     * @return the traversal with the {@link LambdaFilterStep} added
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#general-steps">Reference Documentation - General Steps</a>
     */
    public default GraphTraversal<S, E> filter(final Predicate<Traverser<E>> predicate) {
        this.asAdmin().getBytecode().addStep(Symbols.filter, predicate);
        return this.asAdmin().addStep(new LambdaFilterStep<>(this.asAdmin(), predicate));
    }

    /**
     * Map the {@link Traverser} to either {@code true} or {@code false}, where {@code false} will not pass the
     * traverser to the next step.
     *
     * @param filterTraversal the filter traversal to apply
     * @return the traversal with the {@link TraversalFilterStep} added
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#general-steps">Reference Documentation - General Steps</a>
     */
    public default GraphTraversal<S, E> filter(final Traversal<?, ?> filterTraversal) {
        this.asAdmin().getBytecode().addStep(Symbols.filter, filterTraversal);
        return this.asAdmin().addStep(new TraversalFilterStep<>(this.asAdmin(), (Traversal) filterTraversal));
    }

    /**
     * Ensures that at least one of the provided traversals yield a result.
     *
     * @param orTraversals filter traversals where at least one must be satisfied
     * @return the traversal with an appended {@link OrStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#or-step">Reference Documentation - Or Step</a>
     */
    public default GraphTraversal<S, E> or(final Traversal<?, ?>... orTraversals) {
        this.asAdmin().getBytecode().addStep(Symbols.or, orTraversals);
        return this.asAdmin().addStep(new OrStep(this.asAdmin(), orTraversals));
    }

    /**
     * Ensures that all of the provided traversals yield a result.
     *
     * @param andTraversals filter traversals that must be satisfied
     * @return the traversal with an appended {@link AndStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#and-step">Reference Documentation - And Step</a>
     */
    public default GraphTraversal<S, E> and(final Traversal<?, ?>... andTraversals) {
        this.asAdmin().getBytecode().addStep(Symbols.and, andTraversals);
        return this.asAdmin().addStep(new AndStep(this.asAdmin(), andTraversals));
    }

    /**
     * Provides a way to add arbitrary objects to a traversal stream.
     *
     * @param injections the objects to add to the stream
     * @return the traversal with an appended {@link InjectStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#inject-step">Reference Documentation - Inject Step</a>
     *
     */
    public default GraphTraversal<S, E> inject(final E... injections) {
        this.asAdmin().getBytecode().addStep(Symbols.inject, injections);
        return this.asAdmin().addStep(new InjectStep<>(this.asAdmin(), injections));
    }

    /**
     * Remove all duplicates in the traversal stream up to this point.
     *
     * @param scope       whether the deduplication is on the stream (global) or the current object (local).
     * @param dedupLabels if labels are provided, then the scope labels determine de-duplication. No labels implies current object.
     * @return the traversal with an appended {@link DedupGlobalStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#dedup-step">Reference Documentation - Dedup Step</a>
     */
    public default GraphTraversal<S, E> dedup(final Scope scope, final String... dedupLabels) {
        this.asAdmin().getBytecode().addStep(Symbols.dedup, scope, dedupLabels);
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new DedupGlobalStep<>(this.asAdmin(), dedupLabels) : new DedupLocalStep(this.asAdmin()));
    }

    /**
     * Remove all duplicates in the traversal stream up to this point.
     *
     * @param dedupLabels if labels are provided, then the scoped object's labels determine de-duplication. No labels implies current object.
     * @return the traversal with an appended {@link DedupGlobalStep}.
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#dedup-step">Reference Documentation - Dedup Step</a>
     */
    public default GraphTraversal<S, E> dedup(final String... dedupLabels) {
        this.asAdmin().getBytecode().addStep(Symbols.dedup, dedupLabels);
        return this.asAdmin().addStep(new DedupGlobalStep<>(this.asAdmin(), dedupLabels));
    }

    /**
     * Filters the current object based on the object itself or the path history.
     *
     * @param startKey the key containing the object to filter
     * @param predicate the filter to apply
     * @return the traversal with an appended {@link WherePredicateStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#where-step">Reference Documentation - Where Step</a>
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#using-where-with-match">Reference Documentation - Where with Match</a>
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#using-where-with-select">Reference Documentation - Where with Select</a>
     */
    public default GraphTraversal<S, E> where(final String startKey, final P<String> predicate) {
        this.asAdmin().getBytecode().addStep(Symbols.where, startKey, predicate);
        return this.asAdmin().addStep(new WherePredicateStep<>(this.asAdmin(), Optional.ofNullable(startKey), predicate));
    }

    /**
     * Filters the current object based on the object itself or the path history.
     *
     * @param predicate the filter to apply
     * @return the traversal with an appended {@link WherePredicateStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#where-step">Reference Documentation - Where Step</a>
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#using-where-with-match">Reference Documentation - Where with Match</a>
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#using-where-with-select">Reference Documentation - Where with Select</a>
     */
    public default GraphTraversal<S, E> where(final P<String> predicate) {
        this.asAdmin().getBytecode().addStep(Symbols.where, predicate);
        return this.asAdmin().addStep(new WherePredicateStep<>(this.asAdmin(), Optional.empty(), predicate));
    }

    /**
     * Filters the current object based on the object itself or the path history.
     *
     * @param whereTraversal the filter to apply
     * @return the traversal with an appended {@link WherePredicateStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#where-step">Reference Documentation - Where Step</a>
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#using-where-with-match">Reference Documentation - Where with Match</a>
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#using-where-with-select">Reference Documentation - Where with Select</a>
     */
    public default GraphTraversal<S, E> where(final Traversal<?, ?> whereTraversal) {
        this.asAdmin().getBytecode().addStep(Symbols.where, whereTraversal);
        return TraversalHelper.getVariableLocations(whereTraversal.asAdmin()).isEmpty() ?
                this.asAdmin().addStep(new TraversalFilterStep<>(this.asAdmin(), (Traversal) whereTraversal)) :
                this.asAdmin().addStep(new WhereTraversalStep<>(this.asAdmin(), whereTraversal));
    }

    /**
     * Filters vertices, edges and vertex properties based on their properties.
     *
     * @param propertyKey the key of the property to filter on
     * @param predicate the filter to apply to the key's value
     * @return the traversal with an appended {@link HasStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step">Reference Documentation - Has Step</a>
     */
    public default GraphTraversal<S, E> has(final String propertyKey, final P<?> predicate) {
        this.asAdmin().getBytecode().addStep(Symbols.has, propertyKey, predicate);
        return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(propertyKey, predicate));
    }

    /**
     * Filters vertices, edges and vertex properties based on their properties.
     *
     * @param accessor the {@link T} accessor of the property to filter on
     * @param predicate the filter to apply to the key's value
     * @return the traversal with an appended {@link HasStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step">Reference Documentation - Has Step</a>
     */
    public default GraphTraversal<S, E> has(final T accessor, final P<?> predicate) {
        this.asAdmin().getBytecode().addStep(Symbols.has, accessor, predicate);
        return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(accessor.getAccessor(), predicate));
    }

    /**
     * Filters vertices, edges and vertex properties based on their properties.
     *
     * @param propertyKey the key of the property to filter on
     * @param value the value to compare the property value to for equality
     * @return the traversal with an appended {@link HasStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step">Reference Documentation - Has Step</a>
     */
    public default GraphTraversal<S, E> has(final String propertyKey, final Object value) {
        if (value instanceof P)
            return this.has(propertyKey, (P) value);
        else if (value instanceof Traversal)
            return this.has(propertyKey, (Traversal) value);
        else {
            this.asAdmin().getBytecode().addStep(Symbols.has, propertyKey, value);
            return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(propertyKey, P.eq(value)));
        }
    }

    /**
     * Filters vertices, edges and vertex properties based on their properties.
     *
     * @param accessor the {@link T} accessor of the property to filter on
     * @param value the value to compare the accessor value to for equality
     * @return the traversal with an appended {@link HasStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step">Reference Documentation - Has Step</a>
     */
    public default GraphTraversal<S, E> has(final T accessor, final Object value) {
        if (value instanceof P)
            return this.has(accessor, (P) value);
        else if (value instanceof Traversal)
            return this.has(accessor, (Traversal) value);
        else {
            this.asAdmin().getBytecode().addStep(Symbols.has, accessor, value);
            return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(accessor.getAccessor(), P.eq(value)));
        }
    }

    /**
     * Filters vertices, edges and vertex properties based on their properties.
     *
     * @param label the label of the {@link Element}
     * @param propertyKey the key of the property to filter on
     * @param predicate the filter to apply to the key's value
     * @return the traversal with an appended {@link HasStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step">Reference Documentation - Has Step</a>
     */
    public default GraphTraversal<S, E> has(final String label, final String propertyKey, final P<?> predicate) {
        this.asAdmin().getBytecode().addStep(Symbols.has, label, propertyKey, predicate);
        TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(T.label.getAccessor(), P.eq(label)));
        return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(propertyKey, predicate));
    }

    /**
     * Filters vertices, edges and vertex properties based on their properties.
     *
     * @param label the label of the {@link Element}
     * @param propertyKey the key of the property to filter on
     * @param value the value to compare the accessor value to for equality
     * @return the traversal with an appended {@link HasStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step">Reference Documentation - Has Step</a>
     */
    public default GraphTraversal<S, E> has(final String label, final String propertyKey, final Object value) {
        this.asAdmin().getBytecode().addStep(Symbols.has, label, propertyKey, value);
        TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(T.label.getAccessor(), P.eq(label)));
        return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(propertyKey, value instanceof P ? (P) value : P.eq(value)));
    }

    /**
     * Filters vertices, edges and vertex properties based on their properties.
     *
     * @param accessor the {@link T} accessor of the property to filter on
     * @param propertyTraversal the traversal to filter the accessor value by
     * @return the traversal with an appended {@link HasStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step">Reference Documentation - Has Step</a>
     */
    public default GraphTraversal<S, E> has(final T accessor, final Traversal<?, ?> propertyTraversal) {
        this.asAdmin().getBytecode().addStep(Symbols.has, accessor, propertyTraversal);
        return this.asAdmin().addStep(
                new TraversalFilterStep<>(this.asAdmin(), propertyTraversal.asAdmin().addStep(0,
                        new PropertiesStep(propertyTraversal.asAdmin(), PropertyType.VALUE, accessor.getAccessor()))));
    }

    /**
     * Filters vertices, edges and vertex properties based on their properties.
     *
     * @param propertyKey the key of the property to filter on
     * @param propertyTraversal the traversal to filter the property value by
     * @return the traversal with an appended {@link HasStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step" target="_blank">Reference Documentation - Has Step</a>
     */
    public default GraphTraversal<S, E> has(final String propertyKey, final Traversal<?, ?> propertyTraversal) {
        this.asAdmin().getBytecode().addStep(Symbols.has, propertyKey, propertyTraversal);
        return this.asAdmin().addStep(
                new TraversalFilterStep<>(this.asAdmin(), propertyTraversal.asAdmin().addStep(0,
                        new PropertiesStep(propertyTraversal.asAdmin(), PropertyType.VALUE, propertyKey))));
    }

    /**
     * Filters vertices, edges and vertex properties based on the existence of properties.
     *
     * @param propertyKey the key of the property to filter on for existence
     * @return the traversal with an appended {@link HasStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step">Reference Documentation - Has Step</a>
     */
    public default GraphTraversal<S, E> has(final String propertyKey) {
        this.asAdmin().getBytecode().addStep(Symbols.has, propertyKey);
        return this.asAdmin().addStep(new TraversalFilterStep<>(this.asAdmin(), __.values(propertyKey)));
    }

    /**
     * Filters vertices, edges and vertex properties based on the non-existence of properties.
     *
     * @param propertyKey the key of the property to filter on for existence
     * @return the traversal with an appended {@link HasStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step">Reference Documentation - Has Step</a>
     */
    public default GraphTraversal<S, E> hasNot(final String propertyKey) {
        this.asAdmin().getBytecode().addStep(Symbols.hasNot, propertyKey);
        return this.asAdmin().addStep(new NotStep<>(this.asAdmin(), __.values(propertyKey)));
    }

    /**
     * Filters vertices, edges and vertex properties based on their label.
     *
     * @param label the label of the {@link Element}
     * @param otherLabels additional labels of the {@link Element}
     * @return the traversal with an appended {@link HasStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step">Reference Documentation - Has Step</a>
     */
    public default GraphTraversal<S, E> hasLabel(final String label, final String... otherLabels) {
        final String[] labels = new String[otherLabels.length + 1];
        labels[0] = label;
        System.arraycopy(otherLabels, 0, labels, 1, otherLabels.length);
        this.asAdmin().getBytecode().addStep(Symbols.hasLabel, labels);
        return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(T.label.getAccessor(), labels.length == 1 ? P.eq(labels[0]) : P.within(labels)));
    }

    /**
     * Filters vertices, edges and vertex properties based on their label.
     *
     * @param predicate the filter to apply to the label of the {@link Element}
     * @return the traversal with an appended {@link HasStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step">Reference Documentation - Has Step</a>
     */
    public default GraphTraversal<S, E> hasLabel(final P<String> predicate) {
        this.asAdmin().getBytecode().addStep(Symbols.hasLabel, predicate);
        return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(T.label.getAccessor(), predicate));
    }

    /**
     * Filters vertices, edges and vertex properties based on their identifier.
     *
     * @param id the identifier of the {@link Element}
     * @param otherIds additional identifiers of the {@link Element}
     * @return the traversal with an appended {@link HasStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step">Reference Documentation - Has Step</a>
     */
    public default GraphTraversal<S, E> hasId(final Object id, final Object... otherIds) {
        if (id instanceof P)
            return this.hasId((P) id);
        else {
            final List<Object> ids = new ArrayList<>();
            if (id instanceof Object[]) {
                for (final Object i : (Object[]) id) {
                    ids.add(i);
                }
            } else
                ids.add(id);
            for (final Object i : otherIds) {
                if (i.getClass().isArray()) {
                    for (final Object ii : (Object[]) i) {
                        ids.add(ii);
                    }
                } else
                    ids.add(i);
            }
            this.asAdmin().getBytecode().addStep(Symbols.hasId, ids.toArray());
            return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(T.id.getAccessor(), ids.size() == 1 ? P.eq(ids.get(0)) : P.within(ids)));
        }
    }

    /**
     * Filters vertices, edges and vertex properties based on their identifier.
     *
     * @param predicate the filter to apply to the identifier of the {@link Element}
     * @return the traversal with an appended {@link HasStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step">Reference Documentation - Has Step</a>
     */
    public default GraphTraversal<S, E> hasId(final P<Object> predicate) {
        this.asAdmin().getBytecode().addStep(Symbols.hasId, predicate);
        return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(T.id.getAccessor(), predicate));
    }

    /**
     * Filters vertices, edges and vertex properties based on their key.
     *
     * @param label the key of the {@link Element}
     * @param otherLabels additional key of the {@link Element}
     * @return the traversal with an appended {@link HasStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step">Reference Documentation - Has Step</a>
     */
    public default GraphTraversal<S, E> hasKey(final String label, final String... otherLabels) {
        final String[] labels = new String[otherLabels.length + 1];
        labels[0] = label;
        System.arraycopy(otherLabels, 0, labels, 1, otherLabels.length);
        this.asAdmin().getBytecode().addStep(Symbols.hasKey, labels);
        return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(T.key.getAccessor(), labels.length == 1 ? P.eq(labels[0]) : P.within(labels)));
    }

    /**
     * Filters vertices, edges and vertex properties based on their key.
     *
     * @param predicate the filter to apply to the key of the {@link Element}
     * @return the traversal with an appended {@link HasStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step">Reference Documentation - Has Step</a>
     */
    public default GraphTraversal<S, E> hasKey(final P<String> predicate) {
        this.asAdmin().getBytecode().addStep(Symbols.hasKey, predicate);
        return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(T.key.getAccessor(), predicate));
    }

    /**
     * Filters vertices, edges and vertex properties based on their value.
     *
     * @param value the value of the {@link Element}
     * @param otherValues additional values of the {@link Element}
     * @return the traversal with an appended {@link HasStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step">Reference Documentation - Has Step</a>
     */
    public default GraphTraversal<S, E> hasValue(final Object value, final Object... otherValues) {
        if (value instanceof P)
            return this.hasValue((P) value);
        else {
            final List<Object> values = new ArrayList<>();
            if (value instanceof Object[]) {
                for (final Object v : (Object[]) value) {
                    values.add(v);
                }
            } else
                values.add(value);
            for (final Object v : otherValues) {
                if (v instanceof Object[]) {
                    for (final Object vv : (Object[]) v) {
                        values.add(vv);
                    }
                } else
                    values.add(v);
            }
            this.asAdmin().getBytecode().addStep(Symbols.hasValue, values.toArray());
            return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(T.value.getAccessor(), values.size() == 1 ? P.eq(values.get(0)) : P.within(values)));
        }
    }

    /**
     * Filters vertices, edges and vertex properties based on their value.
     *
     * @param predicate the filter to apply to the value of the {@link Element}
     * @return the traversal with an appended {@link HasStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step">Reference Documentation - Has Step</a>
     */
    public default GraphTraversal<S, E> hasValue(final P<Object> predicate) {
        this.asAdmin().getBytecode().addStep(Symbols.hasValue, predicate);
        return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(T.value.getAccessor(), predicate));
    }

    /**
     * Filters <code>E</code> object values given the provided {@code predicate}.
     *
     * @param predicate the filter to apply
     * @return the traversal with an appended {@link IsStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#is-step" target="_blank">Reference Documentation - Is Step</a>
     */
    public default GraphTraversal<S, E> is(final P<E> predicate) {
        this.asAdmin().getBytecode().addStep(Symbols.is, predicate);
        return this.asAdmin().addStep(new IsStep<>(this.asAdmin(), predicate));
    }

    /**
     * Filter the <code>E</code> object if it is not {@link P#eq} to the provided value.
     *
     * @param value the value that the object must equal.
     * @return the traversal with an appended {@link IsStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#is-step" target="_blank">Reference Documentation - Is Step</a>
     */
    public default GraphTraversal<S, E> is(final Object value) {
        this.asAdmin().getBytecode().addStep(Symbols.is, value);
        return this.asAdmin().addStep(new IsStep<>(this.asAdmin(), value instanceof P ? (P<E>) value : P.eq((E) value)));
    }

    /**
     * Removes objects from the traversal stream when the traversal provided as an argument does not return any objects.
     *
     * @param notTraversal the traversal to filter by.
     * @return the traversal with an appended {@link NotStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#not-step" target="_blank">Reference Documentation - Not Step</a>
     */
    public default GraphTraversal<S, E> not(final Traversal<?, ?> notTraversal) {
        this.asAdmin().getBytecode().addStep(Symbols.not, notTraversal);
        return this.asAdmin().addStep(new NotStep<>(this.asAdmin(), (Traversal<E, ?>) notTraversal));
    }

    /**
     * Filter the <code>E</code> object given a biased coin toss.
     *
     * @param probability the probability that the object will pass through
     * @return the traversal with an appended {@link CoinStep}.
     */
    public default GraphTraversal<S, E> coin(final double probability) {
        this.asAdmin().getBytecode().addStep(Symbols.coin, probability);
        return this.asAdmin().addStep(new CoinStep<>(this.asAdmin(), probability));
    }

    public default GraphTraversal<S, E> range(final long low, final long high) {
        this.asAdmin().getBytecode().addStep(Symbols.range, low, high);
        return this.asAdmin().addStep(new RangeGlobalStep<>(this.asAdmin(), low, high));
    }

    public default <E2> GraphTraversal<S, E2> range(final Scope scope, final long low, final long high) {
        this.asAdmin().getBytecode().addStep(Symbols.range, scope, low, high);
        return this.asAdmin().addStep(scope.equals(Scope.global)
                ? new RangeGlobalStep<>(this.asAdmin(), low, high)
                : new RangeLocalStep<>(this.asAdmin(), low, high));
    }

    public default GraphTraversal<S, E> limit(final long limit) {
        this.asAdmin().getBytecode().addStep(Symbols.limit, limit);
        return this.asAdmin().addStep(new RangeGlobalStep<>(this.asAdmin(), 0, limit));
    }

    public default <E2> GraphTraversal<S, E2> limit(final Scope scope, final long limit) {
        this.asAdmin().getBytecode().addStep(Symbols.limit, scope, limit);
        return this.asAdmin().addStep(scope.equals(Scope.global)
                ? new RangeGlobalStep<>(this.asAdmin(), 0, limit)
                : new RangeLocalStep<>(this.asAdmin(), 0, limit));
    }

    public default GraphTraversal<S, E> tail() {
        this.asAdmin().getBytecode().addStep(Symbols.tail);
        return this.asAdmin().addStep(new TailGlobalStep<>(this.asAdmin(), 1));
    }

    public default GraphTraversal<S, E> tail(final long limit) {
        this.asAdmin().getBytecode().addStep(Symbols.tail, limit);
        return this.asAdmin().addStep(new TailGlobalStep<>(this.asAdmin(), limit));
    }

    public default <E2> GraphTraversal<S, E2> tail(final Scope scope) {
        this.asAdmin().getBytecode().addStep(Symbols.tail, scope);
        return this.asAdmin().addStep(scope.equals(Scope.global)
                ? new TailGlobalStep<>(this.asAdmin(), 1)
                : new TailLocalStep<>(this.asAdmin(), 1));
    }

    public default <E2> GraphTraversal<S, E2> tail(final Scope scope, final long limit) {
        this.asAdmin().getBytecode().addStep(Symbols.tail, scope, limit);
        return this.asAdmin().addStep(scope.equals(Scope.global)
                ? new TailGlobalStep<>(this.asAdmin(), limit)
                : new TailLocalStep<>(this.asAdmin(), limit));
    }

    /**
     * Once the first {@link Traverser} hits this step, a count down is started. Once the time limit is up, all remaining traversers are filtered out.
     *
     * @param timeLimit the count down time
     * @return the traversal with an appended {@link TimeLimitStep}
     */
    public default GraphTraversal<S, E> timeLimit(final long timeLimit) {
        this.asAdmin().getBytecode().addStep(Symbols.timeLimit, timeLimit);
        return this.asAdmin().addStep(new TimeLimitStep<E>(this.asAdmin(), timeLimit));
    }

    /**
     * Filter the <code>E</code> object if its {@link Traverser#path} is not {@link Path#isSimple}.
     *
     * @return the traversal with an appended {@link SimplePathStep}.
     */
    public default GraphTraversal<S, E> simplePath() {
        this.asAdmin().getBytecode().addStep(Symbols.simplePath);
        return this.asAdmin().addStep(new SimplePathStep<>(this.asAdmin()));
    }

    /**
     * Filter the <code>E</code> object if its {@link Traverser#path} is {@link Path#isSimple}.
     *
     * @return the traversal with an appended {@link CyclicPathStep}.
     */
    public default GraphTraversal<S, E> cyclicPath() {
        this.asAdmin().getBytecode().addStep(Symbols.cyclicPath);
        return this.asAdmin().addStep(new CyclicPathStep<>(this.asAdmin()));
    }

    public default GraphTraversal<S, E> sample(final int amountToSample) {
        this.asAdmin().getBytecode().addStep(Symbols.sample, amountToSample);
        return this.asAdmin().addStep(new SampleGlobalStep<>(this.asAdmin(), amountToSample));
    }

    public default GraphTraversal<S, E> sample(final Scope scope, final int amountToSample) {
        this.asAdmin().getBytecode().addStep(Symbols.sample, scope, amountToSample);
        return this.asAdmin().addStep(scope.equals(Scope.global)
                ? new SampleGlobalStep<>(this.asAdmin(), amountToSample)
                : new SampleLocalStep<>(this.asAdmin(), amountToSample));
    }

    /**
     * Removes elements and properties from the graph. This step is not a terminating, in the sense that it does not
     * automatically iterate the traversal. It is therefore necessary to do some form of iteration for the removal
     * to actually take place. In most cases, iteration is best accomplished with {@code g.V().drop().iterate()}.
     *
     * @return the traversal with the {@link DropStep} added
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#drop-step">Reference Documentation - Drop Step</a>
     */
    public default GraphTraversal<S, E> drop() {
        this.asAdmin().getBytecode().addStep(Symbols.drop);
        return this.asAdmin().addStep(new DropStep<>(this.asAdmin()));
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    /**
     * Perform some operation on the {@link Traverser} and pass it to the next step unmodified.
     *
     * @param consumer the operation to perform at this step in relation to the {@link Traverser}
     * @return the traversal with an appended {@link LambdaSideEffectStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#general-steps">Reference Documentation - General Steps</a>
     */
    public default GraphTraversal<S, E> sideEffect(final Consumer<Traverser<E>> consumer) {
        this.asAdmin().getBytecode().addStep(Symbols.sideEffect, consumer);
        return this.asAdmin().addStep(new LambdaSideEffectStep<>(this.asAdmin(), consumer));
    }

    /**
     * Perform some operation on the {@link Traverser} and pass it to the next step unmodified.
     *
     * @param sideEffectTraversal the operation to perform at this step in relation to the {@link Traverser}
     * @return the traversal with an appended {@link TraversalSideEffectStep}
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#general-steps">Reference Documentation - General Steps</a>
     */
    public default GraphTraversal<S, E> sideEffect(final Traversal<?, ?> sideEffectTraversal) {
        this.asAdmin().getBytecode().addStep(Symbols.sideEffect, sideEffectTraversal);
        return this.asAdmin().addStep(new TraversalSideEffectStep<>(this.asAdmin(), (Traversal) sideEffectTraversal));
    }

    public default <E2> GraphTraversal<S, E2> cap(final String sideEffectKey, final String... sideEffectKeys) {
        this.asAdmin().getBytecode().addStep(Symbols.cap, sideEffectKey, sideEffectKeys);
        return this.asAdmin().addStep(new SideEffectCapStep<>(this.asAdmin(), sideEffectKey, sideEffectKeys));
    }

    public default GraphTraversal<S, Edge> subgraph(final String sideEffectKey) {
        this.asAdmin().getBytecode().addStep(Symbols.subgraph, sideEffectKey);
        return this.asAdmin().addStep(new SubgraphStep(this.asAdmin(), sideEffectKey));
    }

    public default GraphTraversal<S, E> aggregate(final String sideEffectKey) {
        this.asAdmin().getBytecode().addStep(Symbols.aggregate, sideEffectKey);
        return this.asAdmin().addStep(new AggregateStep<>(this.asAdmin(), sideEffectKey));
    }

    public default GraphTraversal<S, E> group(final String sideEffectKey) {
        this.asAdmin().getBytecode().addStep(Symbols.group, sideEffectKey);
        return this.asAdmin().addStep(new GroupSideEffectStep<>(this.asAdmin(), sideEffectKey));
    }

    /**
     * @deprecated As of release 3.1.0, replaced by {@link #group(String)}.
     */
    public default GraphTraversal<S, E> groupV3d0(final String sideEffectKey) {
        this.asAdmin().getBytecode().addStep(Symbols.groupV3d0, sideEffectKey);
        return this.asAdmin().addStep(new GroupSideEffectStepV3d0<>(this.asAdmin(), sideEffectKey));
    }

    public default GraphTraversal<S, E> groupCount(final String sideEffectKey) {
        this.asAdmin().getBytecode().addStep(Symbols.groupCount, sideEffectKey);
        return this.asAdmin().addStep(new GroupCountSideEffectStep<>(this.asAdmin(), sideEffectKey));
    }

    public default GraphTraversal<S, E> tree(final String sideEffectKey) {
        this.asAdmin().getBytecode().addStep(Symbols.tree, sideEffectKey);
        return this.asAdmin().addStep(new TreeSideEffectStep<>(this.asAdmin(), sideEffectKey));
    }

    public default <V, U> GraphTraversal<S, E> sack(final BiFunction<V, U, V> sackOperator) {
        this.asAdmin().getBytecode().addStep(Symbols.sack, sackOperator);
        return this.asAdmin().addStep(new SackValueStep<>(this.asAdmin(), sackOperator));
    }

    /**
     * @deprecated As of release 3.1.0, replaced by {@link #sack(BiFunction)} with {@link #by(String)}.
     */
    @Deprecated
    public default <V, U> GraphTraversal<S, E> sack(final BiFunction<V, U, V> sackOperator, final String elementPropertyKey) {
        return this.sack(sackOperator).by(elementPropertyKey);
    }

    public default GraphTraversal<S, E> store(final String sideEffectKey) {
        this.asAdmin().getBytecode().addStep(Symbols.store, sideEffectKey);
        return this.asAdmin().addStep(new StoreStep<>(this.asAdmin(), sideEffectKey));
    }

    public default GraphTraversal<S, E> profile(final String sideEffectKey) {
        this.asAdmin().getBytecode().addStep(Traversal.Symbols.profile, sideEffectKey);
        return this.asAdmin().addStep(new ProfileSideEffectStep<>(this.asAdmin(), sideEffectKey));
    }

    @Override
    public default GraphTraversal<S, TraversalMetrics> profile() {
        return (GraphTraversal<S, TraversalMetrics>) Traversal.super.profile();
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
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#addproperty-step">AddProperty Step</a>
     */
    public default GraphTraversal<S, E> property(final VertexProperty.Cardinality cardinality, final Object key, final Object value, final Object... keyValues) {
        if (null == cardinality)
            this.asAdmin().getBytecode().addStep(Symbols.property, key, value, keyValues);
        else
            this.asAdmin().getBytecode().addStep(Symbols.property, cardinality, key, value, keyValues);
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
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#addproperty-step">AddProperty Step</a>
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

    /**
     * Split the {@link Traverser} to all the specified traversals.
     *
     * @param branchTraversal the traversal to branch the {@link Traverser} to
     * @return the {@link Traversal} with the {@link BranchStep} added
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#general-steps">Reference Documentation - General Steps</a>
     */
    public default <M, E2> GraphTraversal<S, E2> branch(final Traversal<?, M> branchTraversal) {
        this.asAdmin().getBytecode().addStep(Symbols.branch, branchTraversal);
        final BranchStep<E, E2, M> branchStep = new BranchStep<>(this.asAdmin());
        branchStep.setBranchTraversal((Traversal.Admin<E, M>) branchTraversal);
        return this.asAdmin().addStep(branchStep);
    }

    /**
     * Split the {@link Traverser} to all the specified functions.
     *
     * @param function the traversal to branch the {@link Traverser} to
     * @return the {@link Traversal} with the {@link BranchStep} added
     * @see <a target="_blank" href="http://tinkerpop.apache.org/docs/${project.version}/reference/#general-steps">Reference Documentation - General Steps</a>
     */
    public default <M, E2> GraphTraversal<S, E2> branch(final Function<Traverser<E>, M> function) {
        this.asAdmin().getBytecode().addStep(Symbols.branch, function);
        final BranchStep<E, E2, M> branchStep = new BranchStep<>(this.asAdmin());
        branchStep.setBranchTraversal((Traversal.Admin<E, M>) __.map(function));
        return this.asAdmin().addStep(branchStep);
    }

    public default <M, E2> GraphTraversal<S, E2> choose(final Traversal<?, M> choiceTraversal) {
        this.asAdmin().getBytecode().addStep(Symbols.choose, choiceTraversal);
        return this.asAdmin().addStep(new ChooseStep<>(this.asAdmin(), (Traversal.Admin<E, M>) choiceTraversal));
    }

    public default <E2> GraphTraversal<S, E2> choose(final Traversal<?, ?> traversalPredicate,
                                                     final Traversal<?, E2> trueChoice, final Traversal<?, E2> falseChoice) {
        this.asAdmin().getBytecode().addStep(Symbols.choose, traversalPredicate, trueChoice, falseChoice);
        return this.asAdmin().addStep(new ChooseStep<E, E2, Boolean>(this.asAdmin(), (Traversal.Admin<E, ?>) traversalPredicate, (Traversal.Admin<E, E2>) trueChoice, (Traversal.Admin<E, E2>) falseChoice));
    }

    public default <E2> GraphTraversal<S, E2> choose(final Traversal<?, ?> traversalPredicate,
                                                     final Traversal<?, E2> trueChoice) {
        this.asAdmin().getBytecode().addStep(Symbols.choose, traversalPredicate, trueChoice);
        return this.asAdmin().addStep(new ChooseStep<E, E2, Boolean>(this.asAdmin(), (Traversal.Admin<E, ?>) traversalPredicate, (Traversal.Admin<E, E2>) trueChoice, (Traversal.Admin<E, E2>) __.identity()));
    }

    public default <M, E2> GraphTraversal<S, E2> choose(final Function<E, M> choiceFunction) {
        this.asAdmin().getBytecode().addStep(Symbols.choose, choiceFunction);
        return this.asAdmin().addStep(new ChooseStep<>(this.asAdmin(), (Traversal.Admin<E, M>) __.map(new FunctionTraverser<>(choiceFunction))));
    }

    public default <E2> GraphTraversal<S, E2> choose(final Predicate<E> choosePredicate,
                                                     final Traversal<?, E2> trueChoice, final Traversal<?, E2> falseChoice) {
        this.asAdmin().getBytecode().addStep(Symbols.choose, choosePredicate, trueChoice, falseChoice);
        return this.asAdmin().addStep(new ChooseStep<E, E2, Boolean>(this.asAdmin(), (Traversal.Admin<E, ?>) __.filter(new PredicateTraverser<>(choosePredicate)), (Traversal.Admin<E, E2>) trueChoice, (Traversal.Admin<E, E2>) falseChoice));
    }

    public default <E2> GraphTraversal<S, E2> choose(final Predicate<E> choosePredicate,
                                                     final Traversal<?, E2> trueChoice) {
        this.asAdmin().getBytecode().addStep(Symbols.choose, choosePredicate, trueChoice);
        return this.asAdmin().addStep(new ChooseStep<E, E2, Boolean>(this.asAdmin(), (Traversal.Admin<E, ?>) __.filter(new PredicateTraverser<>(choosePredicate)), (Traversal.Admin<E, E2>) trueChoice, (Traversal.Admin<E, E2>) __.identity()));
    }

    public default <E2> GraphTraversal<S, E2> optional(final Traversal<?, E2> optionalTraversal) {
        this.asAdmin().getBytecode().addStep(Symbols.optional, optionalTraversal);
        return this.asAdmin().addStep(new ChooseStep<>(this.asAdmin(), (Traversal.Admin<E, ?>) optionalTraversal, (Traversal.Admin<E, E2>) optionalTraversal.asAdmin().clone(), (Traversal.Admin<E, E2>) __.<E2>identity()));
    }

    public default <E2> GraphTraversal<S, E2> union(final Traversal<?, E2>... unionTraversals) {
        this.asAdmin().getBytecode().addStep(Symbols.union, unionTraversals);
        return this.asAdmin().addStep(new UnionStep(this.asAdmin(), Arrays.copyOf(unionTraversals, unionTraversals.length, Traversal.Admin[].class)));
    }

    public default <E2> GraphTraversal<S, E2> coalesce(final Traversal<?, E2>... coalesceTraversals) {
        this.asAdmin().getBytecode().addStep(Symbols.coalesce, coalesceTraversals);
        return this.asAdmin().addStep(new CoalesceStep(this.asAdmin(), Arrays.copyOf(coalesceTraversals, coalesceTraversals.length, Traversal.Admin[].class)));
    }

    public default GraphTraversal<S, E> repeat(final Traversal<?, E> repeatTraversal) {
        this.asAdmin().getBytecode().addStep(Symbols.repeat, repeatTraversal);
        return RepeatStep.addRepeatToTraversal(this, (Traversal.Admin<E, E>) repeatTraversal);
    }

    public default GraphTraversal<S, E> emit(final Traversal<?, ?> emitTraversal) {
        this.asAdmin().getBytecode().addStep(Symbols.emit, emitTraversal);
        return RepeatStep.addEmitToTraversal(this, (Traversal.Admin<E, ?>) emitTraversal);
    }

    public default GraphTraversal<S, E> emit(final Predicate<Traverser<E>> emitPredicate) {
        this.asAdmin().getBytecode().addStep(Symbols.emit, emitPredicate);
        return RepeatStep.addEmitToTraversal(this, (Traversal.Admin<E, ?>) __.filter(emitPredicate));
    }

    public default GraphTraversal<S, E> emit() {
        this.asAdmin().getBytecode().addStep(Symbols.emit);
        return RepeatStep.addEmitToTraversal(this, TrueTraversal.instance());
    }

    public default GraphTraversal<S, E> until(final Traversal<?, ?> untilTraversal) {
        this.asAdmin().getBytecode().addStep(Symbols.until, untilTraversal);
        return RepeatStep.addUntilToTraversal(this, (Traversal.Admin<E, ?>) untilTraversal);
    }

    public default GraphTraversal<S, E> until(final Predicate<Traverser<E>> untilPredicate) {
        this.asAdmin().getBytecode().addStep(Symbols.until, untilPredicate);
        return RepeatStep.addEmitToTraversal(this, (Traversal.Admin<E, ?>) __.filter(untilPredicate));
    }

    public default GraphTraversal<S, E> times(final int maxLoops) {
        this.asAdmin().getBytecode().addStep(Symbols.times, maxLoops);
        if (this.asAdmin().getEndStep() instanceof TimesModulating) {
            ((TimesModulating) this.asAdmin().getEndStep()).modulateTimes(maxLoops);
            return this;
        } else
            return RepeatStep.addUntilToTraversal(this, new LoopTraversal<>(maxLoops));
    }

    public default <E2> GraphTraversal<S, E2> local(final Traversal<?, E2> localTraversal) {
        this.asAdmin().getBytecode().addStep(Symbols.local, localTraversal);
        return this.asAdmin().addStep(new LocalStep<>(this.asAdmin(), localTraversal.asAdmin()));
    }

    /////////////////// VERTEX PROGRAM STEPS ////////////////

    public default GraphTraversal<S, E> pageRank() {
        return this.pageRank(0.85d);
    }

    public default GraphTraversal<S, E> pageRank(final double alpha) {
        this.asAdmin().getBytecode().addStep(Symbols.pageRank, alpha);
        return this.asAdmin().addStep((Step<E, E>) new PageRankVertexProgramStep(this.asAdmin(), alpha));
    }

    public default GraphTraversal<S, E> peerPressure() {
        this.asAdmin().getBytecode().addStep(Symbols.peerPressure);
        return this.asAdmin().addStep((Step<E, E>) new PeerPressureVertexProgramStep(this.asAdmin()));
    }

    public default GraphTraversal<S, E> program(final VertexProgram<?> vertexProgram) {
        return this.asAdmin().addStep((Step<E, E>) new ProgramVertexProgramStep(this.asAdmin(), vertexProgram));
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public default GraphTraversal<S, E> as(final String stepLabel, final String... stepLabels) {
        this.asAdmin().getBytecode().addStep(Symbols.as, stepLabel, stepLabels);
        if (this.asAdmin().getSteps().size() == 0) this.asAdmin().addStep(new StartStep<>(this.asAdmin()));
        final Step<?, E> endStep = this.asAdmin().getEndStep();
        endStep.addLabel(stepLabel);
        for (final String label : stepLabels) {
            endStep.addLabel(label);
        }
        return this;
    }

    public default GraphTraversal<S, E> barrier() {
        return this.barrier(Integer.MAX_VALUE);
    }

    public default GraphTraversal<S, E> barrier(final int maxBarrierSize) {
        this.asAdmin().getBytecode().addStep(Symbols.barrier, maxBarrierSize);
        return this.asAdmin().addStep(new NoOpBarrierStep<>(this.asAdmin(), maxBarrierSize));
    }

    public default GraphTraversal<S, E> barrier(final Consumer<TraverserSet<Object>> barrierConsumer) {
        this.asAdmin().getBytecode().addStep(Symbols.barrier, barrierConsumer);
        return this.asAdmin().addStep(new LambdaCollectingBarrierStep<>(this.asAdmin(), (Consumer) barrierConsumer, Integer.MAX_VALUE));
    }


    //// BY-MODULATORS

    public default GraphTraversal<S, E> by() {
        this.asAdmin().getBytecode().addStep(Symbols.by);
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy();
        return this;
    }

    public default GraphTraversal<S, E> by(final Traversal<?, ?> traversal) {
        this.asAdmin().getBytecode().addStep(Symbols.by, traversal);
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy(traversal.asAdmin());
        return this;
    }

    public default GraphTraversal<S, E> by(final T token) {
        this.asAdmin().getBytecode().addStep(Symbols.by, token);
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy(token);
        return this;
    }

    public default GraphTraversal<S, E> by(final String key) {
        this.asAdmin().getBytecode().addStep(Symbols.by, key);
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy(key);
        return this;
    }

    public default <V> GraphTraversal<S, E> by(final Function<V, Object> function) {
        this.asAdmin().getBytecode().addStep(Symbols.by, function);
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy(function);
        return this;
    }

    //// COMPARATOR BY-MODULATORS

    public default <V> GraphTraversal<S, E> by(final Traversal<?, ?> traversal, final Comparator<V> comparator) {
        this.asAdmin().getBytecode().addStep(Symbols.by, traversal, comparator);
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy(traversal.asAdmin(), comparator);
        return this;
    }

    public default GraphTraversal<S, E> by(final Comparator<E> comparator) {
        this.asAdmin().getBytecode().addStep(Symbols.by, comparator);
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy(comparator);
        return this;
    }

    public default GraphTraversal<S, E> by(final Order order) {
        this.asAdmin().getBytecode().addStep(Symbols.by, order);
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy(order);
        return this;
    }

    public default <V> GraphTraversal<S, E> by(final String key, final Comparator<V> comparator) {
        this.asAdmin().getBytecode().addStep(Symbols.by, key, comparator);
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy(key, comparator);
        return this;
    }

    /*public default <V> GraphTraversal<S, E> by(final Column column, final Comparator<V> comparator) {
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy(column, comparator);
        return this;
    }

    public default <V> GraphTraversal<S, E> by(final T token, final Comparator<V> comparator) {
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy(token, comparator);
        return this;
    }*/

    public default <U> GraphTraversal<S, E> by(final Function<U, Object> function, final Comparator comparator) {
        this.asAdmin().getBytecode().addStep(Symbols.by, function, comparator);
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy(function, comparator);
        return this;
    }

    ////

    public default <M, E2> GraphTraversal<S, E> option(final M pickToken, final Traversal<E, E2> traversalOption) {
        this.asAdmin().getBytecode().addStep(Symbols.option, pickToken, traversalOption);
        ((TraversalOptionParent<M, E, E2>) this.asAdmin().getEndStep()).addGlobalChildOption(pickToken, traversalOption.asAdmin());
        return this;
    }

    public default <E2> GraphTraversal<S, E> option(final Traversal<E, E2> traversalOption) {
        this.asAdmin().getBytecode().addStep(Symbols.option, traversalOption);
        return this.option(TraversalOptionParent.Pick.any, traversalOption.asAdmin());
    }

    ////

    @Override
    public default GraphTraversal<S, E> iterate() {
        Traversal.super.iterate();
        return this;
    }

    ////

    public static final class Symbols {

        private Symbols() {
            // static fields only
        }

        public static final String map = "map";
        public static final String flatMap = "flatMap";
        public static final String id = "id";
        public static final String label = "label";
        public static final String identity = "identity";
        public static final String constant = "constant";
        public static final String V = "V";
        public static final String E = "E";
        public static final String to = "to";
        public static final String out = "out";
        public static final String in = "in";
        public static final String both = "both";
        public static final String toE = "toE";
        public static final String outE = "outE";
        public static final String inE = "inE";
        public static final String bothE = "bothE";
        public static final String toV = "toV";
        public static final String outV = "outV";
        public static final String inV = "inV";
        public static final String bothV = "bothV";
        public static final String otherV = "otherV";
        public static final String order = "order";
        public static final String properties = "properties";
        public static final String values = "values";
        public static final String propertyMap = "propertyMap";
        public static final String valueMap = "valueMap";
        public static final String select = "select";
        public static final String key = "key";
        public static final String value = "value";
        public static final String path = "path";
        public static final String match = "match";
        public static final String sack = "sack";
        public static final String loops = "loops";
        public static final String project = "project";
        public static final String unfold = "unfold";
        public static final String fold = "fold";
        public static final String count = "count";
        public static final String sum = "sum";
        public static final String max = "max";
        public static final String min = "min";
        public static final String mean = "mean";
        public static final String group = "group";
        @Deprecated
        public static final String groupV3d0 = "groupV3d0";
        public static final String groupCount = "groupCount";
        public static final String tree = "tree";
        public static final String addV = "addV";
        public static final String addE = "addE";
        public static final String from = "from";
        public static final String filter = "filter";
        public static final String or = "or";
        public static final String and = "and";
        public static final String inject = "inject";
        public static final String dedup = "dedup";
        public static final String where = "where";
        public static final String has = "has";
        public static final String hasNot = "hasNot";
        public static final String hasLabel = "hasLabel";
        public static final String hasId = "hasId";
        public static final String hasKey = "hasKey";
        public static final String hasValue = "hasValue";
        public static final String is = "is";
        public static final String not = "not";
        public static final String range = "range";
        public static final String limit = "limit";
        public static final String tail = "tail";
        public static final String coin = "coin";

        public static final String timeLimit = "timeLimit";
        public static final String simplePath = "simplePath";
        public static final String cyclicPath = "cyclicPath";
        public static final String sample = "sample";

        public static final String drop = "drop";

        public static final String sideEffect = "sideEffect";
        public static final String cap = "cap";
        public static final String property = "property";
        public static final String store = "store";
        public static final String aggregate = "aggregate";
        public static final String subgraph = "subgraph";
        public static final String barrier = "barrier";
        public static final String local = "local";
        public static final String emit = "emit";
        public static final String repeat = "repeat";
        public static final String until = "until";
        public static final String branch = "branch";
        public static final String union = "union";
        public static final String coalesce = "coalesce";
        public static final String choose = "choose";
        public static final String optional = "optional";


        public static final String pageRank = "pageRank";
        public static final String peerPressure = "peerPressure";
        public static final String program = "program";

        public static final String by = "by";
        public static final String times = "times";
        public static final String as = "as";
        public static final String option = "option";

    }
}