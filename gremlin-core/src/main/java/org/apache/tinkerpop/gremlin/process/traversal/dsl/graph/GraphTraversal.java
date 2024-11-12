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
import org.apache.tinkerpop.gremlin.process.computer.clustering.connected.ConnectedComponentVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ConnectedComponentVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRankVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PeerPressureVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ProgramVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ShortestPathVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.DT;
import org.apache.tinkerpop.gremlin.process.traversal.Failure;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pick;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.CardinalityValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ColumnTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.FunctionTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.LoopTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.PredicateTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TrueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.FromToModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.ReadWriting;
import org.apache.tinkerpop.gremlin.process.traversal.step.TimesModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.OptionalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AllStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AnyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.CoinStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DiscardStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.LambdaFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NoneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.PathFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TimeLimitStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AsDateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AsStringGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AsStringLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CallStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CoalesceStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CombineStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ConcatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ConjoinStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ConstantStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.DateAddStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.DateDiffStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.DedupLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.DifferenceStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.DisjunctStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ElementMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ElementStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FormatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupCountStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IndexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IntersectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LTrimGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LTrimLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LabelStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaCollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaFlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LengthGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LengthLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LoopsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MinGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MinLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProductStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyKeyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.RTrimGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.RTrimLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.RangeLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ReplaceGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ReplaceLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ReverseStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SackStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SampleLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SplitGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SplitLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SubstringGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SubstringLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TailLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ToLowerGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ToLowerLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ToUpperGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ToUpperLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalFlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalSelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TrimGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TrimLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.UnfoldStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.FailStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IoStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.LambdaSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TraversalSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
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

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.single;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
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
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#general-steps" target="_blank">Reference Documentation - General Steps</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> map(final Function<Traverser<E>, E2> function) {
        this.asAdmin().getGremlinLang().addStep(Symbols.map, function);
        return this.asAdmin().addStep(new LambdaMapStep<>(this.asAdmin(), function));
    }

    /**
     * Map a {@link Traverser} referencing an object of type <code>E</code> to an object of type <code>E2</code>.
     *
     * @param mapTraversal the traversal expression that does the functional mapping
     * @return the traversal with an appended {@link LambdaMapStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#general-steps" target="_blank">Reference Documentation - General Steps</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> map(final Traversal<?, E2> mapTraversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.map, mapTraversal);
        return this.asAdmin().addStep(new TraversalMapStep<>(this.asAdmin(), mapTraversal));
    }

    /**
     * Map a {@link Traverser} referencing an object of type <code>E</code> to an iterator of objects of type <code>E2</code>.
     * The resultant iterator is drained one-by-one before a new <code>E</code> object is pulled in for processing.
     *
     * @param function the lambda expression that does the functional mapping
     * @param <E2>     the type of the returned iterator objects
     * @return the traversal with an appended {@link LambdaFlatMapStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#general-steps" target="_blank">Reference Documentation - General Steps</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> flatMap(final Function<Traverser<E>, Iterator<E2>> function) {
        this.asAdmin().getGremlinLang().addStep(Symbols.flatMap, function);
        return this.asAdmin().addStep(new LambdaFlatMapStep<>(this.asAdmin(), function));
    }

    /**
     * Map a {@link Traverser} referencing an object of type <code>E</code> to an iterator of objects of type <code>E2</code>.
     * The internal traversal is drained one-by-one before a new <code>E</code> object is pulled in for processing.
     *
     * @param flatMapTraversal the traversal generating objects of type <code>E2</code>
     * @param <E2>             the end type of the internal traversal
     * @return the traversal with an appended {@link TraversalFlatMapStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#general-steps" target="_blank">Reference Documentation - General Steps</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> flatMap(final Traversal<?, E2> flatMapTraversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.flatMap, flatMapTraversal);
        return this.asAdmin().addStep(new TraversalFlatMapStep<>(this.asAdmin(), flatMapTraversal));
    }

    /**
     * Map the {@link Element} to its {@link Element#id}.
     *
     * @return the traversal with an appended {@link IdStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#id-step" target="_blank">Reference Documentation - Id Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, Object> id() {
        this.asAdmin().getGremlinLang().addStep(Symbols.id);
        return this.asAdmin().addStep(new IdStep<>(this.asAdmin()));
    }

    /**
     * Map the {@link Element} to its {@link Element#label}.
     *
     * @return the traversal with an appended {@link LabelStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#label-step" target="_blank">Reference Documentation - Label Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, String> label() {
        this.asAdmin().getGremlinLang().addStep(Symbols.label);
        return this.asAdmin().addStep(new LabelStep<>(this.asAdmin()));
    }

    /**
     * Map the <code>E</code> object to itself. In other words, a "no op."
     *
     * @return the traversal with an appended {@link IdentityStep}.
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> identity() {
        this.asAdmin().getGremlinLang().addStep(Symbols.identity);
        return this.asAdmin().addStep(new IdentityStep<>(this.asAdmin()));
    }

    /**
     * Map any object to a fixed <code>E</code> value.
     *
     * @return the traversal with an appended {@link ConstantStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#constant-step" target="_blank">Reference Documentation - Constant Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> constant(final E2 e) {
        this.asAdmin().getGremlinLang().addStep(Symbols.constant, e);
        return this.asAdmin().addStep(new ConstantStep<E, E2>(this.asAdmin(), e));
    }

    /**
     * A {@code V} step is usually used to start a traversal but it may also be used mid-traversal.
     *
     * @param vertexIdsOrElements vertices to inject into the traversal
     * @return the traversal with an appended {@link GraphStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#graph-step" target="_blank">Reference Documentation - Graph Step</a>
     * @since 3.1.0-incubating
     */
    public default GraphTraversal<S, Vertex> V(final Object... vertexIdsOrElements) {
        // a single null is [null]
        final Object[] ids = null == vertexIdsOrElements ? new Object[] { null } : vertexIdsOrElements;
        this.asAdmin().getGremlinLang().addStep(Symbols.V, ids);
        return this.asAdmin().addStep(new GraphStep<>(this.asAdmin(), Vertex.class, false, ids));
    }

    /**
     * A {@code E} step is usually used to start a traversal but it may also be used mid-traversal.
     *
     * @param edgeIdsOrElements edges to inject into the traversal
     * @return the traversal with an appended {@link GraphStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#e-step" target="_blank">Reference Documentation - E Step</a>
     * @since 3.7.0
     */
    public default GraphTraversal<S, Edge> E(final Object... edgeIdsOrElements) {
        // a single null is [null]
        final Object[] ids = null == edgeIdsOrElements ? new Object[] { null } : edgeIdsOrElements;
        this.asAdmin().getGremlinLang().addStep(Symbols.E, ids);
        return this.asAdmin().addStep(new GraphStep<>(this.asAdmin(), Edge.class, false, ids));
    }

    /**
     * Map the {@link Vertex} to its adjacent vertices given a direction and edge labels.
     *
     * @param direction  the direction to traverse from the current vertex
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps" target="_blank">Reference Documentation - Vertex Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, Vertex> to(final Direction direction, final String... edgeLabels) {
        this.asAdmin().getGremlinLang().addStep(Symbols.to, direction, edgeLabels);
        return this.asAdmin().addStep(new VertexStep<>(this.asAdmin(), Vertex.class, direction, edgeLabels));
    }

    /**
     * Map the {@link Vertex} to its outgoing adjacent vertices given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps" target="_blank">Reference Documentation - Vertex Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, Vertex> out(final String... edgeLabels) {
        this.asAdmin().getGremlinLang().addStep(Symbols.out, edgeLabels);
        return this.asAdmin().addStep(new VertexStep<>(this.asAdmin(), Vertex.class, Direction.OUT, edgeLabels));
    }

    /**
     * Map the {@link Vertex} to its incoming adjacent vertices given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps" target="_blank">Reference Documentation - Vertex Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, Vertex> in(final String... edgeLabels) {
        this.asAdmin().getGremlinLang().addStep(Symbols.in, edgeLabels);
        return this.asAdmin().addStep(new VertexStep<>(this.asAdmin(), Vertex.class, Direction.IN, edgeLabels));
    }

    /**
     * Map the {@link Vertex} to its adjacent vertices given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps" target="_blank">Reference Documentation - Vertex Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, Vertex> both(final String... edgeLabels) {
        this.asAdmin().getGremlinLang().addStep(Symbols.both, edgeLabels);
        return this.asAdmin().addStep(new VertexStep<>(this.asAdmin(), Vertex.class, Direction.BOTH, edgeLabels));
    }

    /**
     * Map the {@link Vertex} to its incident edges given the direction and edge labels.
     *
     * @param direction  the direction to traverse from the current vertex
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps" target="_blank">Reference Documentation - Vertex Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, Edge> toE(final Direction direction, final String... edgeLabels) {
        this.asAdmin().getGremlinLang().addStep(Symbols.toE, direction, edgeLabels);
        return this.asAdmin().addStep(new VertexStep<>(this.asAdmin(), Edge.class, direction, edgeLabels));
    }

    /**
     * Map the {@link Vertex} to its outgoing incident edges given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps" target="_blank">Reference Documentation - Vertex Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, Edge> outE(final String... edgeLabels) {
        this.asAdmin().getGremlinLang().addStep(Symbols.outE, edgeLabels);
        return this.asAdmin().addStep(new VertexStep<>(this.asAdmin(), Edge.class, Direction.OUT, edgeLabels));
    }

    /**
     * Map the {@link Vertex} to its incoming incident edges given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps" target="_blank">Reference Documentation - Vertex Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, Edge> inE(final String... edgeLabels) {
        this.asAdmin().getGremlinLang().addStep(Symbols.inE, edgeLabels);
        return this.asAdmin().addStep(new VertexStep<>(this.asAdmin(), Edge.class, Direction.IN, edgeLabels));
    }

    /**
     * Map the {@link Vertex} to its incident edges given the edge labels.
     *
     * @param edgeLabels the edge labels to traverse
     * @return the traversal with an appended {@link VertexStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps" target="_blank">Reference Documentation - Vertex Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, Edge> bothE(final String... edgeLabels) {
        this.asAdmin().getGremlinLang().addStep(Symbols.bothE, edgeLabels);
        return this.asAdmin().addStep(new VertexStep<>(this.asAdmin(), Edge.class, Direction.BOTH, edgeLabels));
    }

    /**
     * Map the {@link Edge} to its incident vertices given the direction.
     *
     * @param direction the direction to traverser from the current edge
     * @return the traversal with an appended {@link EdgeVertexStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps" target="_blank">Reference Documentation - Vertex Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, Vertex> toV(final Direction direction) {
        this.asAdmin().getGremlinLang().addStep(Symbols.toV, direction);
        return this.asAdmin().addStep(new EdgeVertexStep(this.asAdmin(), direction));
    }

    /**
     * Map the {@link Edge} to its incoming/head incident {@link Vertex}.
     *
     * @return the traversal with an appended {@link EdgeVertexStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps" target="_blank">Reference Documentation - Vertex Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, Vertex> inV() {
        this.asAdmin().getGremlinLang().addStep(Symbols.inV);
        return this.asAdmin().addStep(new EdgeVertexStep(this.asAdmin(), Direction.IN));
    }

    /**
     * Map the {@link Edge} to its outgoing/tail incident {@link Vertex}.
     *
     * @return the traversal with an appended {@link EdgeVertexStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps" target="_blank">Reference Documentation - Vertex Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, Vertex> outV() {
        this.asAdmin().getGremlinLang().addStep(Symbols.outV);
        return this.asAdmin().addStep(new EdgeVertexStep(this.asAdmin(), Direction.OUT));
    }

    /**
     * Map the {@link Edge} to its incident vertices.
     *
     * @return the traversal with an appended {@link EdgeVertexStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps" target="_blank">Reference Documentation - Vertex Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, Vertex> bothV() {
        this.asAdmin().getGremlinLang().addStep(Symbols.bothV);
        return this.asAdmin().addStep(new EdgeVertexStep(this.asAdmin(), Direction.BOTH));
    }

    /**
     * Map the {@link Edge} to the incident vertex that was not just traversed from in the path history.
     *
     * @return the traversal with an appended {@link EdgeOtherVertexStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#vertex-steps" target="_blank">Reference Documentation - Vertex Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, Vertex> otherV() {
        this.asAdmin().getGremlinLang().addStep(Symbols.otherV);
        return this.asAdmin().addStep(new EdgeOtherVertexStep(this.asAdmin()));
    }

    /**
     * Order all the objects in the traversal up to this point and then emit them one-by-one in their ordered sequence.
     *
     * @return the traversal with an appended {@link OrderGlobalStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#order-step" target="_blank">Reference Documentation - Order Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> order() {
        this.asAdmin().getGremlinLang().addStep(Symbols.order);
        return this.asAdmin().addStep(new OrderGlobalStep<>(this.asAdmin()));
    }

    /**
     * Order either the {@link Scope#local} object (e.g. a list, map, etc.) or the entire {@link Scope#global} traversal stream.
     *
     * @param scope whether the ordering is the current local object or the entire global stream.
     * @return the traversal with an appended {@link OrderGlobalStep} or {@link OrderLocalStep} depending on the {@code scope}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#order-step" target="_blank">Reference Documentation - Order Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> order(final Scope scope) {
        this.asAdmin().getGremlinLang().addStep(Symbols.order, scope);
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new OrderGlobalStep<>(this.asAdmin()) : new OrderLocalStep<>(this.asAdmin()));
    }

    /**
     * Map the {@link Element} to its associated properties given the provide property keys.
     * If no property keys are provided, then all properties are emitted.
     *
     * @param propertyKeys the properties to retrieve
     * @param <E2>         the value type of the returned properties
     * @return the traversal with an appended {@link PropertiesStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#properties-step" target="_blank">Reference Documentation - Properties Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, ? extends Property<E2>> properties(final String... propertyKeys) {
        this.asAdmin().getGremlinLang().addStep(Symbols.properties, propertyKeys);
        return this.asAdmin().addStep(new PropertiesStep<>(this.asAdmin(), PropertyType.PROPERTY, propertyKeys));
    }

    /**
     * Map the {@link Element} to the values of the associated properties given the provide property keys.
     * If no property keys are provided, then all property values are emitted.
     *
     * @param propertyKeys the properties to retrieve their value from
     * @param <E2>         the value type of the properties
     * @return the traversal with an appended {@link PropertiesStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#values-step" target="_blank">Reference Documentation - Values Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> values(final String... propertyKeys) {
        this.asAdmin().getGremlinLang().addStep(Symbols.values, propertyKeys);
        return this.asAdmin().addStep(new PropertiesStep<>(this.asAdmin(), PropertyType.VALUE, propertyKeys));
    }

    /**
     * Map the {@link Element} to a {@link Map} of the properties key'd according to their {@link Property#key}.
     * If no property keys are provided, then all properties are retrieved.
     *
     * @param propertyKeys the properties to retrieve
     * @param <E2>         the value type of the returned properties
     * @return the traversal with an appended {@link PropertyMapStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#propertymap-step" target="_blank">Reference Documentation - PropertyMap Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, Map<String, E2>> propertyMap(final String... propertyKeys) {
        this.asAdmin().getGremlinLang().addStep(Symbols.propertyMap, propertyKeys);
        return this.asAdmin().addStep(new PropertyMapStep<>(this.asAdmin(), WithOptions.none, PropertyType.PROPERTY, propertyKeys));
    }

    /**
     * Map the {@link Element} to a {@code Map} of the property values key'd according to their {@link Property#key}.
     * If no property keys are provided, then all property values are retrieved. For vertices, the {@code Map} will
     * be returned with the assumption of single property values along with {@link T#id} and {@link T#label}. Prefer
     * {@link #valueMap(String...)} if multi-property processing is required. For  edges, keys will include additional
     * related edge structure of {@link Direction#IN} and {@link Direction#OUT} which themselves are {@code Map}
     * instances of the particular {@link Vertex} represented by {@link T#id} and {@link T#label}.
     *
     * @param propertyKeys the properties to retrieve
     * @param <E2>         the value type of the returned properties
     * @return the traversal with an appended {@link ElementMapStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#elementmap-step" target="_blank">Reference Documentation - ElementMap Step</a>
     * @since 3.4.4
     */
    public default <E2> GraphTraversal<S, Map<Object, E2>> elementMap(final String... propertyKeys) {
        this.asAdmin().getGremlinLang().addStep(Symbols.elementMap, propertyKeys);
        return this.asAdmin().addStep(new ElementMapStep<>(this.asAdmin(), propertyKeys));
    }

    /**
     * Map the {@link Element} to a {@code Map} of the property values key'd according to their {@link Property#key}.
     * If no property keys are provided, then all property values are retrieved.
     *
     * @param propertyKeys the properties to retrieve
     * @param <E2>         the value type of the returned properties
     * @return the traversal with an appended {@link PropertyMapStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#valuemap-step" target="_blank">Reference Documentation - ValueMap Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, Map<Object, E2>> valueMap(final String... propertyKeys) {
        this.asAdmin().getGremlinLang().addStep(Symbols.valueMap, propertyKeys);
        return this.asAdmin().addStep(new PropertyMapStep<>(this.asAdmin(), WithOptions.none, PropertyType.VALUE, propertyKeys));
    }

    /**
     * Map the {@link Element} to a {@code Map} of the property values key'd according to their {@link Property#key}.
     * If no property keys are provided, then all property values are retrieved.
     *
     * @param includeTokens whether to include {@link T} tokens in the emitted map.
     * @param propertyKeys  the properties to retrieve
     * @param <E2>          the value type of the returned properties
     * @return the traversal with an appended {@link PropertyMapStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#valuemap-step" target="_blank">Reference Documentation - ValueMap Step</a>
     * @since 3.0.0-incubating
     * @deprecated As of release 3.4.0, deprecated in favor of {@link GraphTraversal#valueMap(String...)} in conjunction with
     *             {@link GraphTraversal#with(String, Object)} or simple prefer {@link #elementMap(String...)}.
     */
    @Deprecated
    public default <E2> GraphTraversal<S, Map<Object, E2>> valueMap(final boolean includeTokens, final String... propertyKeys) {
        this.asAdmin().getGremlinLang().addStep(Symbols.valueMap, includeTokens, propertyKeys);
        return this.asAdmin().addStep(new PropertyMapStep<>(this.asAdmin(), WithOptions.all, PropertyType.VALUE, propertyKeys));
    }

    /**
     * Map the {@link Property} to its {@link Property#key}.
     *
     * @return the traversal with an appended {@link PropertyKeyStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#key-step" target="_blank">Reference Documentation - Key Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, String> key() {
        this.asAdmin().getGremlinLang().addStep(Symbols.key);
        return this.asAdmin().addStep(new PropertyKeyStep(this.asAdmin()));
    }

    /**
     * Map the {@link Property} to its {@link Property#value}.
     *
     * @return the traversal with an appended {@link PropertyValueStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#value-step" target="_blank">Reference Documentation - Value Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> value() {
        this.asAdmin().getGremlinLang().addStep(Symbols.value);
        return this.asAdmin().addStep(new PropertyValueStep<>(this.asAdmin()));
    }

    /**
     * Map the {@link Traverser} to its {@link Path} history via {@link Traverser#path}.
     *
     * @return the traversal with an appended {@link PathStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#path-step" target="_blank">Reference Documentation - Path Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, Path> path() {
        this.asAdmin().getGremlinLang().addStep(Symbols.path);
        return this.asAdmin().addStep(new PathStep<>(this.asAdmin()));
    }

    /**
     * Map the {@link Traverser} to a {@link Map} of bindings as specified by the provided match traversals.
     *
     * @param matchTraversals the traversal that maintain variables which must hold for the life of the traverser
     * @param <E2>            the type of the objects bound in the variables
     * @return the traversal with an appended {@link MatchStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#match-step" target="_blank">Reference Documentation - Match Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, Map<String, E2>> match(final Traversal<?, ?>... matchTraversals) {
        this.asAdmin().getGremlinLang().addStep(Symbols.match, matchTraversals);
        return this.asAdmin().addStep(new MatchStep<>(this.asAdmin(), ConnectiveStep.Connective.AND, matchTraversals));
    }

    /**
     * Map the {@link Traverser} to its {@link Traverser#sack} value.
     *
     * @param <E2> the sack value type
     * @return the traversal with an appended {@link SackStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#sack-step" target="_blank">Reference Documentation - Sack Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> sack() {
        this.asAdmin().getGremlinLang().addStep(Symbols.sack);
        return this.asAdmin().addStep(new SackStep<>(this.asAdmin()));
    }

    /**
     * If the {@link Traverser} supports looping then calling this method will extract the number of loops for that
     * traverser.
     *
     * @return the traversal with an appended {@link LoopsStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#loops-step" target="_blank">Reference Documentation - Loops Step</a>
     * @since 3.1.0-incubating
     */
    public default GraphTraversal<S, Integer> loops() {
        this.asAdmin().getGremlinLang().addStep(Symbols.loops);
        return this.asAdmin().addStep(new LoopsStep<>(this.asAdmin(), null));
    }

    /**
     * If the {@link Traverser} supports looping then calling this method will extract the number of loops for that
     * traverser for the named loop.
     *
     * @return the traversal with an appended {@link LoopsStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#loops-step" target="_blank">Reference Documentation - Loops Step</a>
     * @since 3.4.0
     */
    public default GraphTraversal<S, Integer> loops(final String loopName) {
        this.asAdmin().getGremlinLang().addStep(Symbols.loops, loopName);
        return this.asAdmin().addStep(new LoopsStep<>(this.asAdmin(), loopName));
    }

    /**
     * Projects the current object in the stream into a {@code Map} that is keyed by the provided labels.
     *
     * @return the traversal with an appended {@link ProjectStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#project-step" target="_blank">Reference Documentation - Project Step</a>
     * @since 3.2.0-incubating
     */
    public default <E2> GraphTraversal<S, Map<String, E2>> project(final String projectKey, final String... otherProjectKeys) {
        final String[] projectKeys = new String[otherProjectKeys.length + 1];
        projectKeys[0] = projectKey;
        System.arraycopy(otherProjectKeys, 0, projectKeys, 1, otherProjectKeys.length);
        this.asAdmin().getGremlinLang().addStep(Symbols.project, projectKey, otherProjectKeys);
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
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#select-step" target="_blank">Reference Documentation - Select Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, Map<String, E2>> select(final Pop pop, final String selectKey1, final String selectKey2, String... otherSelectKeys) {
        final String[] selectKeys = new String[otherSelectKeys.length + 2];
        selectKeys[0] = selectKey1;
        selectKeys[1] = selectKey2;
        System.arraycopy(otherSelectKeys, 0, selectKeys, 2, otherSelectKeys.length);
        this.asAdmin().getGremlinLang().addStep(Symbols.select, pop, selectKey1, selectKey2, otherSelectKeys);
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
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#select-step" target="_blank">Reference Documentation - Select Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, Map<String, E2>> select(final String selectKey1, final String selectKey2, String... otherSelectKeys) {
        final String[] selectKeys = new String[otherSelectKeys.length + 2];
        selectKeys[0] = selectKey1;
        selectKeys[1] = selectKey2;
        System.arraycopy(otherSelectKeys, 0, selectKeys, 2, otherSelectKeys.length);
        this.asAdmin().getGremlinLang().addStep(Symbols.select, selectKey1, selectKey2, otherSelectKeys);
        return this.asAdmin().addStep(new SelectStep<>(this.asAdmin(), Pop.last, selectKeys));
    }

    /**
     * Map the {@link Traverser} to the object specified by the {@code selectKey} and apply the {@link Pop} operation
     * to it.
     *
     * @param selectKey the key to project
     * @return the traversal with an appended {@link SelectStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#select-step" target="_blank">Reference Documentation - Select Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> select(final Pop pop, final String selectKey) {
        this.asAdmin().getGremlinLang().addStep(Symbols.select, pop, selectKey);
        return this.asAdmin().addStep(new SelectOneStep<>(this.asAdmin(), pop, selectKey));
    }

    /**
     * Map the {@link Traverser} to the object specified by the {@code selectKey}. Note that unlike other uses of
     * {@code select} where there are multiple keys, this use of {@code select} with a single key does not produce a
     * {@code Map}.
     *
     * @param selectKey the key to project
     * @return the traversal with an appended {@link SelectStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#select-step" target="_blank">Reference Documentation - Select Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> select(final String selectKey) {
        this.asAdmin().getGremlinLang().addStep(Symbols.select, selectKey);
        return this.asAdmin().addStep(new SelectOneStep<>(this.asAdmin(), Pop.last, selectKey));
    }

    /**
     * Map the {@link Traverser} to the object specified by the key returned by the {@code keyTraversal} and apply the {@link Pop} operation
     * to it.
     *
     * @param keyTraversal the traversal expression that selects the key to project
     * @return the traversal with an appended {@link SelectStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#select-step" target="_blank">Reference Documentation - Select Step</a>
     * @since 3.3.3
     */
    public default <E2> GraphTraversal<S, E2> select(final Pop pop, final Traversal<S, E2> keyTraversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.select, pop, keyTraversal);
        return this.asAdmin().addStep(new TraversalSelectStep<>(this.asAdmin(), pop, keyTraversal));
    }

    /**
     * Map the {@link Traverser} to the object specified by the key returned by the {@code keyTraversal}. Note that unlike other uses of
     * {@code select} where there are multiple keys, this use of {@code select} with a traversal does not produce a
     * {@code Map}.
     *
     * @param keyTraversal the traversal expression that selects the key to project
     * @return the traversal with an appended {@link TraversalSelectStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#select-step" target="_blank">Reference Documentation - Select Step</a>
     * @since 3.3.3
     */
    public default <E2> GraphTraversal<S, E2> select(final Traversal<S, E2> keyTraversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.select, keyTraversal);
        return this.asAdmin().addStep(new TraversalSelectStep<>(this.asAdmin(), null, keyTraversal));
    }

    /**
     * A version of {@code select} that allows for the extraction of a {@link Column} from objects in the traversal.
     *
     * @param column the column to extract
     * @return the traversal with an appended {@link TraversalMapStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#select-step" target="_blank">Reference Documentation - Select Step</a>
     * @since 3.1.0-incubating
     */
    public default <E2> GraphTraversal<S, Collection<E2>> select(final Column column) {
        this.asAdmin().getGremlinLang().addStep(Symbols.select, column);
        return this.asAdmin().addStep(new TraversalMapStep<>(this.asAdmin(), new ColumnTraversal(column)));
    }

    /**
     * Unrolls a {@code Iterator}, {@code Iterable} or {@code Map} into a linear form or simply emits the object if it
     * is not one of those types.
     *
     * @return the traversal with an appended {@link UnfoldStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#unfold-step" target="_blank">Reference Documentation - Unfold Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> unfold() {
        this.asAdmin().getGremlinLang().addStep(Symbols.unfold);
        return this.asAdmin().addStep(new UnfoldStep<>(this.asAdmin()));
    }

    /**
     * Rolls up objects in the stream into an aggregate list.
     *
     * @return the traversal with an appended {@link FoldStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#fold-step" target="_blank">Reference Documentation - Fold Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, List<E>> fold() {
        this.asAdmin().getGremlinLang().addStep(Symbols.fold);
        return this.asAdmin().addStep(new FoldStep<>(this.asAdmin()));
    }

    /**
     * Rolls up objects in the stream into an aggregate value as defined by a {@code seed} and {@code BiFunction}.
     *
     * @param seed         the value to provide as the first argument to the {@code foldFunction}
     * @param foldFunction the function to fold by where the first argument is the {@code seed} or the value returned from subsequent class and
     *                     the second argument is the value from the stream
     * @return the traversal with an appended {@link FoldStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#fold-step" target="_blank">Reference Documentation - Fold Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> fold(final E2 seed, final BiFunction<E2, E, E2> foldFunction) {
        this.asAdmin().getGremlinLang().addStep(Symbols.fold, seed, foldFunction);
        return this.asAdmin().addStep(new FoldStep<>(this.asAdmin(), new ConstantSupplier<>(seed), foldFunction));
    }

    /**
     * Map the traversal stream to its reduction as a sum of the {@link Traverser#bulk} values (i.e. count the number
     * of traversers up to this point).
     *
     * @return the traversal with an appended {@link CountGlobalStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#count-step" target="_blank">Reference Documentation - Count Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, Long> count() {
        this.asAdmin().getGremlinLang().addStep(Symbols.count);
        return this.asAdmin().addStep(new CountGlobalStep<>(this.asAdmin()));
    }

    /**
     * Map the traversal stream to its reduction as a sum of the {@link Traverser#bulk} values given the specified
     * {@link Scope} (i.e. count the number of traversers up to this point).
     *
     * @return the traversal with an appended {@link CountGlobalStep} or {@link CountLocalStep} depending on the {@link Scope}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#count-step" target="_blank">Reference Documentation - Count Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, Long> count(final Scope scope) {
        this.asAdmin().getGremlinLang().addStep(Symbols.count, scope);
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new CountGlobalStep<>(this.asAdmin()) : new CountLocalStep<>(this.asAdmin()));
    }

    /**
     * Map the traversal stream to its reduction as a sum of the {@link Traverser#get} values multiplied by their
     * {@link Traverser#bulk} (i.e. sum the traverser values up to this point).
     *
     * @return the traversal with an appended {@link SumGlobalStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#sum-step" target="_blank">Reference Documentation - Sum Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2 extends Number> GraphTraversal<S, E2> sum() {
        this.asAdmin().getGremlinLang().addStep(Symbols.sum);
        return this.asAdmin().addStep(new SumGlobalStep<>(this.asAdmin()));
    }

    /**
     * Map the traversal stream to its reduction as a sum of the {@link Traverser#get} values multiplied by their
     * {@link Traverser#bulk} given the specified {@link Scope} (i.e. sum the traverser values up to this point).
     *
     * @return the traversal with an appended {@link SumGlobalStep} or {@link SumLocalStep} depending on the {@link Scope}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#sum-step" target="_blank">Reference Documentation - Sum Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2 extends Number> GraphTraversal<S, E2> sum(final Scope scope) {
        this.asAdmin().getGremlinLang().addStep(Symbols.sum, scope);
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new SumGlobalStep<>(this.asAdmin()) : new SumLocalStep<>(this.asAdmin()));
    }

    /**
     * Determines the largest value in the stream.
     *
     * @return the traversal with an appended {@link MaxGlobalStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#max-step" target="_blank">Reference Documentation - Max Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2 extends Comparable> GraphTraversal<S, E2> max() {
        this.asAdmin().getGremlinLang().addStep(Symbols.max);
        return this.asAdmin().addStep(new MaxGlobalStep<>(this.asAdmin()));
    }

    /**
     * Determines the largest value in the stream given the {@link Scope}.
     *
     * @return the traversal with an appended {@link MaxGlobalStep} or {@link MaxLocalStep} depending on the {@link Scope}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#max-step" target="_blank">Reference Documentation - Max Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2 extends Comparable> GraphTraversal<S, E2> max(final Scope scope) {
        this.asAdmin().getGremlinLang().addStep(Symbols.max, scope);
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new MaxGlobalStep<>(this.asAdmin()) : new MaxLocalStep<>(this.asAdmin()));
    }

    /**
     * Determines the smallest value in the stream.
     *
     * @return the traversal with an appended {@link MinGlobalStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#min-step" target="_blank">Reference Documentation - Min Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2 extends Comparable> GraphTraversal<S, E2> min() {
        this.asAdmin().getGremlinLang().addStep(Symbols.min);
        return this.asAdmin().addStep(new MinGlobalStep<>(this.asAdmin()));
    }

    /**
     * Determines the smallest value in the stream given the {@link Scope}.
     *
     * @return the traversal with an appended {@link MinGlobalStep} or {@link MinLocalStep} depending on the {@link Scope}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#min-step" target="_blank">Reference Documentation - Min Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2 extends Comparable> GraphTraversal<S, E2> min(final Scope scope) {
        this.asAdmin().getGremlinLang().addStep(Symbols.min, scope);
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new MinGlobalStep<E2>(this.asAdmin()) : new MinLocalStep<>(this.asAdmin()));
    }

    /**
     * Determines the mean value in the stream.
     *
     * @return the traversal with an appended {@link MeanGlobalStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#mean-step" target="_blank">Reference Documentation - Mean Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2 extends Number> GraphTraversal<S, E2> mean() {
        this.asAdmin().getGremlinLang().addStep(Symbols.mean);
        return this.asAdmin().addStep(new MeanGlobalStep<>(this.asAdmin()));
    }

    /**
     * Determines the mean value in the stream given the {@link Scope}.
     *
     * @return the traversal with an appended {@link MeanGlobalStep} or {@link MeanLocalStep} depending on the {@link Scope}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#mean-step" target="_blank">Reference Documentation - Mean Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2 extends Number> GraphTraversal<S, E2> mean(final Scope scope) {
        this.asAdmin().getGremlinLang().addStep(Symbols.mean, scope);
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new MeanGlobalStep(this.asAdmin()) : new MeanLocalStep(this.asAdmin()));
    }

    /**
     * Organize objects in the stream into a {@code Map}. Calls to {@code group()} are typically accompanied with
     * {@link #by()} modulators which help specify how the grouping should occur.
     *
     * @return the traversal with an appended {@link GroupStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#group-step" target="_blank">Reference Documentation - Group Step</a>
     * @since 3.1.0-incubating
     */
    public default <K, V> GraphTraversal<S, Map<K, V>> group() {
        this.asAdmin().getGremlinLang().addStep(Symbols.group);
        return this.asAdmin().addStep(new GroupStep<>(this.asAdmin()));
    }

    /**
     * Counts the number of times a particular objects has been part of a traversal, returning a {@code Map} where the
     * object is the key and the value is the count.
     *
     * @return the traversal with an appended {@link GroupCountStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#groupcount-step" target="_blank">Reference Documentation - GroupCount Step</a>
     * @since 3.0.0-incubating
     */
    public default <K> GraphTraversal<S, Map<K, Long>> groupCount() {
        this.asAdmin().getGremlinLang().addStep(Symbols.groupCount);
        return this.asAdmin().addStep(new GroupCountStep<>(this.asAdmin()));
    }

    /**
     * Aggregates the emanating paths into a {@link Tree} data structure.
     *
     * @return the traversal with an appended {@link TreeStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#tree-step" target="_blank">Reference Documentation - Tree Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, Tree> tree() {
        this.asAdmin().getGremlinLang().addStep(Symbols.tree);
        return this.asAdmin().addStep(new TreeStep<>(this.asAdmin()));
    }

    /**
     * Adds a {@link Vertex}.
     *
     * @param vertexLabel the label of the {@link Vertex} to add
     * @return the traversal with the {@link AddVertexStep} added
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#addvertex-step" target="_blank">Reference Documentation - AddVertex Step</a>
     * @since 3.1.0-incubating
     */
    public default GraphTraversal<S, Vertex> addV(final String vertexLabel) {
        if (null == vertexLabel) throw new IllegalArgumentException("vertexLabel cannot be null");
        this.asAdmin().getGremlinLang().addStep(Symbols.addV, vertexLabel);
        return this.asAdmin().addStep(new AddVertexStep<>(this.asAdmin(), vertexLabel));
    }

    /**
     * Adds a {@link Vertex} with a vertex label determined by a {@link Traversal}.
     *
     * @return the traversal with the {@link AddVertexStep} added
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#addvertex-step" target="_blank">Reference Documentation - AddVertex Step</a>
     * @since 3.3.1
     */
    public default GraphTraversal<S, Vertex> addV(final Traversal<?, String> vertexLabelTraversal) {
        if (null == vertexLabelTraversal) throw new IllegalArgumentException("vertexLabelTraversal cannot be null");
        this.asAdmin().getGremlinLang().addStep(Symbols.addV, vertexLabelTraversal);
        return this.asAdmin().addStep(new AddVertexStep<>(this.asAdmin(), vertexLabelTraversal.asAdmin()));
    }

    /**
     * Adds a {@link Vertex} with a default vertex label.
     *
     * @return the traversal with the {@link AddVertexStep} added
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#addvertex-step" target="_blank">Reference Documentation - AddVertex Step</a>
     * @since 3.1.0-incubating
     */
    public default GraphTraversal<S, Vertex> addV() {
        this.asAdmin().getGremlinLang().addStep(Symbols.addV);
        return this.asAdmin().addStep(new AddVertexStep<>(this.asAdmin(), (String) null));
    }

    /**
     * Performs a merge (i.e. upsert) style operation for an {@link Vertex} using the incoming {@code Map} traverser as
     * an argument. The {@code Map} represents search criteria and will match each of the supplied key/value pairs where
     * the keys may be {@code String} property values or a value of {@link T}. If a match is not made it will use that
     * search criteria to create the new {@link Vertex}.
     *
     * @since 3.6.0
     */
    public default GraphTraversal<S, Vertex> mergeV() {
        this.asAdmin().getGremlinLang().addStep(Symbols.mergeV);
        final MergeVertexStep<S> step = new MergeVertexStep<>(this.asAdmin(), false);
        return this.asAdmin().addStep(step);
    }

    /**
     * Performs a merge (i.e. upsert) style operation for an {@link Vertex} using a {@code Map} as an argument.
     * The {@code Map} represents search criteria and will match each of the supplied key/value pairs where the keys
     * may be {@code String} property values or a value of {@link T}. If a match is not made it will use that search
     * criteria to create the new {@link Vertex}.
     *
     * @param searchCreate This {@code Map} can have a key of {@link T} or a {@code String}.
     * @since 3.6.0
     */
    public default GraphTraversal<S, Vertex> mergeV(final Map<Object, Object> searchCreate) {
        this.asAdmin().getGremlinLang().addStep(Symbols.mergeV, searchCreate);
        final MergeVertexStep<S> step = new MergeVertexStep<>(this.asAdmin(), false, searchCreate);
        return this.asAdmin().addStep(step);
    }

    /**
     * Performs a merge (i.e. upsert) style operation for an {@link Vertex} using a {@code Map} as an argument.
     * The {@code Map} represents search criteria and will match each of the supplied key/value pairs where the keys
     * may be {@code String} property values or a value of {@link T}. If a match is not made it will use that search
     * criteria to create the new {@link Vertex}.
     *
     *  @param searchCreate This anonymous {@link Traversal} must produce a {@code Map} that may have a keys of
     *  {@link T} or a {@code String}.
     *  @since 3.6.0
     */
    public default GraphTraversal<S, Vertex> mergeV(final Traversal<?, Map<Object, Object>> searchCreate) {
        this.asAdmin().getGremlinLang().addStep(Symbols.mergeV, searchCreate);
        final MergeVertexStep<S> step = null == searchCreate ? new MergeVertexStep(this.asAdmin(), false, (Map) null) :
                new MergeVertexStep(this.asAdmin(), false, searchCreate.asAdmin());
        return this.asAdmin().addStep(step);
    }

    /**
     * Spawns a {@link GraphTraversal} by doing a merge (i.e. upsert) style operation for an {@link Edge} using an
     * incoming {@code Map} as an argument.
     *
     * @since 3.6.0
     */
    public default GraphTraversal<S, Edge> mergeE() {
        this.asAdmin().getGremlinLang().addStep(Symbols.mergeE);
        final MergeEdgeStep<S> step = new MergeEdgeStep(this.asAdmin(), false);
        return this.asAdmin().addStep(step);
    }

    /**
     * Spawns a {@link GraphTraversal} by doing a merge (i.e. upsert) style operation for an {@link Edge} using a
     * {@code Map} as an argument.
     *
     * @param searchCreate This {@code Map} can have a key of {@link T} {@link Direction} or a {@code String}.
     * @since 3.6.0
     */
    public default GraphTraversal<S, Edge> mergeE(final Map<Object, Object> searchCreate) {
        // get a construction time exception if the Map is bad
        this.asAdmin().getGremlinLang().addStep(Symbols.mergeE, searchCreate);
        final MergeEdgeStep<S> step = new MergeEdgeStep(this.asAdmin(), false, searchCreate);
        return this.asAdmin().addStep(step);
    }

    /**
     * Spawns a {@link GraphTraversal} by doing a merge (i.e. upsert) style operation for an {@link Edge} using a
     * {@code Map} as an argument.
     *
     * @param searchCreate This {@code Map} can have a key of {@link T} {@link Direction} or a {@code String}.
     * @since 3.6.0
     */
    public default GraphTraversal<S, Edge> mergeE(final Traversal<?, Map<Object, Object>> searchCreate) {
        this.asAdmin().getGremlinLang().addStep(Symbols.mergeE, searchCreate);

        final MergeEdgeStep<S> step = null == searchCreate ? new MergeEdgeStep(this.asAdmin(), false,  (Map) null) :
                new MergeEdgeStep(this.asAdmin(), false, searchCreate.asAdmin());
        return this.asAdmin().addStep(step);
    }

    /**
     * Adds an {@link Edge} with the specified edge label.
     *
     * @param edgeLabel the label of the newly added edge
     * @return the traversal with the {@link AddEdgeStep} added
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#addedge-step" target="_blank">Reference Documentation - AddEdge Step</a>
     * @since 3.1.0-incubating
     */
    public default GraphTraversal<S, Edge> addE(final String edgeLabel) {
        this.asAdmin().getGremlinLang().addStep(Symbols.addE, edgeLabel);
        return this.asAdmin().addStep(new AddEdgeStep<>(this.asAdmin(), edgeLabel));
    }

    /**
     * Adds a {@link Edge} with an edge label determined by a {@link Traversal}.
     *
     * @return the traversal with the {@link AddEdgeStep} added
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#addedge-step" target="_blank">Reference Documentation - AddEdge Step</a>
     * @since 3.3.1
     */
    public default GraphTraversal<S, Edge> addE(final Traversal<?, String> edgeLabelTraversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.addE, edgeLabelTraversal);
        return this.asAdmin().addStep(new AddEdgeStep<>(this.asAdmin(), null == edgeLabelTraversal ? null : edgeLabelTraversal.asAdmin()));
    }

    /**
     * Provide {@code to()}-modulation to respective steps.
     *
     * @param toStepLabel the step label to modulate to.
     * @return the traversal with the modified {@link FromToModulating} step.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#to-step" target="_blank">Reference Documentation - To Step</a>
     * @since 3.1.0-incubating
     */
    public default GraphTraversal<S, E> to(final String toStepLabel) {
        final Step<?,?> prev = this.asAdmin().getEndStep();
        if (!(prev instanceof FromToModulating))
            throw new IllegalArgumentException(String.format(
                    "The to() step cannot follow %s", prev.getClass().getSimpleName()));

        this.asAdmin().getGremlinLang().addStep(Symbols.to, toStepLabel);
        ((FromToModulating) prev).addTo(toStepLabel);
        return this;
    }

    /**
     * Provide {@code from()}-modulation to respective steps.
     *
     * @param fromStepLabel the step label to modulate to.
     * @return the traversal with the modified {@link FromToModulating} step.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#from-step" target="_blank">Reference Documentation - From Step</a>
     * @since 3.1.0-incubating
     */
    public default GraphTraversal<S, E> from(final String fromStepLabel) {
        final Step<?,?> prev = this.asAdmin().getEndStep();
        if (!(prev instanceof FromToModulating))
            throw new IllegalArgumentException(String.format(
                    "The from() step cannot follow %s", prev.getClass().getSimpleName()));

        this.asAdmin().getGremlinLang().addStep(Symbols.from, fromStepLabel);
        ((FromToModulating) prev).addFrom(fromStepLabel);
        return this;
    }

    /**
     * When used as a modifier to {@link #addE(String)} this method specifies the traversal to use for selecting the
     * incoming vertex of the newly added {@link Edge}.
     *
     * @param toVertex the traversal for selecting the incoming vertex
     * @return the traversal with the modified {@link AddEdgeStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#addedge-step" target="_blank">Reference Documentation - From Step</a>
     * @since 3.1.0-incubating
     */
    public default GraphTraversal<S, E> to(final Traversal<?, Vertex> toVertex) {
        final Step<?,?> prev = this.asAdmin().getEndStep();
        if (!(prev instanceof FromToModulating))
            throw new IllegalArgumentException(String.format(
                    "The to() step cannot follow %s", prev.getClass().getSimpleName()));

        this.asAdmin().getGremlinLang().addStep(Symbols.to, toVertex);
        ((FromToModulating) prev).addTo(toVertex.asAdmin());
        return this;
    }

    /**
     * When used as a modifier to {@link #addE(String)} this method specifies the traversal to use for selecting the
     * outgoing vertex of the newly added {@link Edge}.
     *
     * @param fromVertex the traversal for selecting the outgoing vertex
     * @return the traversal with the modified {@link AddEdgeStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#addedge-step" target="_blank">Reference Documentation - From Step</a>
     * @since 3.1.0-incubating
     */
    public default GraphTraversal<S, E> from(final Traversal<?, Vertex> fromVertex) {
        final Step<?,?> prev = this.asAdmin().getEndStep();
        if (!(prev instanceof FromToModulating))
            throw new IllegalArgumentException(String.format(
                    "The from() step cannot follow %s", prev.getClass().getSimpleName()));

        this.asAdmin().getGremlinLang().addStep(Symbols.from, fromVertex);
        ((FromToModulating) prev).addFrom(fromVertex.asAdmin());
        return this;
    }

    /**
     * When used as a modifier to {@link #addE(String)} this method specifies the traversal to use for selecting the
     * incoming vertex of the newly added {@link Edge}.
     *
     * @param toVertex the vertex for selecting the incoming vertex
     * @return the traversal with the modified {@link AddEdgeStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#addedge-step" target="_blank">Reference Documentation - From Step</a>
     * @since 3.3.0
     */
    public default GraphTraversal<S, E> to(final Vertex toVertex) {
        final Step<?,?> prev = this.asAdmin().getEndStep();
        if (!(prev instanceof FromToModulating))
            throw new IllegalArgumentException(String.format(
                    "The to() step cannot follow %s", prev.getClass().getSimpleName()));

        this.asAdmin().getGremlinLang().addStep(Symbols.to, toVertex);
        ((FromToModulating) prev).addTo(__.constant(toVertex).asAdmin());
        return this;
    }

    /**
     * When used as a modifier to {@link #addE(String)} this method specifies the traversal to use for selecting the
     * outgoing vertex of the newly added {@link Edge}.
     *
     * @param fromVertex the vertex for selecting the outgoing vertex
     * @return the traversal with the modified {@link AddEdgeStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#addedge-step" target="_blank">Reference Documentation - From Step</a>
     * @since 3.3.0
     */
    public default GraphTraversal<S, E> from(final Vertex fromVertex) {
        final Step<?,?> prev = this.asAdmin().getEndStep();
        if (!(prev instanceof FromToModulating))
            throw new IllegalArgumentException(String.format(
                    "The from() step cannot follow %s", prev.getClass().getSimpleName()));

        this.asAdmin().getGremlinLang().addStep(Symbols.from, fromVertex);
        ((FromToModulating) prev).addFrom(__.constant(fromVertex).asAdmin());
        return this;
    }

    /**
     * Map the {@link Traverser} to a {@link Double} according to the mathematical expression provided in the argument.
     *
     * @param expression the mathematical expression with variables refering to scope variables.
     * @return the traversal with the {@link MathStep} added.
     * @since 3.3.1
     */
    public default GraphTraversal<S, Double> math(final String expression) {
        this.asAdmin().getGremlinLang().addStep(Symbols.math, expression);
        return this.asAdmin().addStep(new MathStep<>(this.asAdmin(), expression));
    }

    /**
     * Map a {@link Property} to its {@link Element}.
     *
     * @return the traversal with an appended {@link ElementStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#element-step" target="_blank">Reference Documentation - Element Step</a>
     * @since 3.6.0
     */
    default GraphTraversal<S, Element> element() {
        this.asAdmin().getGremlinLang().addStep(Symbols.element);
        return this.asAdmin().addStep(new ElementStep<>(this.asAdmin()));
    }

    /**
     * Perform the specified service call with no parameters.
     *
     * @param service the name of the service call
     * @return the traversal with an appended {@link CallStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#call-step" target="_blank">Reference Documentation - Call Step</a>
     * @since 3.6.0
     */
    default <E> GraphTraversal<S, E> call(final String service) {
        this.asAdmin().getGremlinLang().addStep(Symbols.call, service);
        final CallStep<S,E> call = new CallStep<>(this.asAdmin(), false, service);
        return this.asAdmin().addStep(call);
    }

    /**
     * Perform the specified service call with the specified static parameters.
     *
     * @param service the name of the service call
     * @param params static parameter map (no nested traversals)
     * @return the traversal with an appended {@link CallStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#call-step" target="_blank">Reference Documentation - Call Step</a>
     * @since 3.6.0
     */
    default <E> GraphTraversal<S, E> call(final String service, final Map params) {
        this.asAdmin().getGremlinLang().addStep(Symbols.call, service, params);
        final CallStep<S,E> call = new CallStep<>(this.asAdmin(), false, service, params);
        return this.asAdmin().addStep(call);
    }

    /**
     * Perform the specified service call with dynamic parameters produced by the specified child traversal.
     *
     * @param service the name of the service call
     * @param childTraversal a traversal that will produce a Map of parameters for the service call when invoked.
     * @return the traversal with an appended {@link CallStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#call-step" target="_blank">Reference Documentation - Call Step</a>
     * @since 3.6.0
     */
    default <E> GraphTraversal<S, E> call(final String service, final Traversal<?, Map<?,?>> childTraversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.call, service, childTraversal);
        final CallStep<S,E> step = null == childTraversal ? new CallStep(this.asAdmin(), false, service) :
                new CallStep(this.asAdmin(), false, service, new LinkedHashMap(), childTraversal.asAdmin());
        return this.asAdmin().addStep(step);
    }

    /**
     * Perform the specified service call with both static and dynamic parameters produced by the specified child
     * traversal. These parameters will be merged at execution time per the provider implementation. Reference
     * implementation merges dynamic into static (dynamic will overwrite static).
     *
     * @param service the name of the service call
     * @param params static parameter map (no nested traversals)
     * @param childTraversal a traversal that will produce a Map of parameters for the service call when invoked.
     * @return the traversal with an appended {@link CallStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#call-step" target="_blank">Reference Documentation - Call Step</a>
     * @since 3.6.0
     */
    default <E> GraphTraversal<S, E> call(final String service, final Map params, final Traversal<?, Map<?,?>> childTraversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.call, service, params, childTraversal);
        final CallStep<S,E> step = null == childTraversal ? new CallStep(this.asAdmin(), false, service, params) :
                new CallStep(this.asAdmin(), false, service, params, childTraversal.asAdmin());
        return this.asAdmin().addStep(step);
    }

    /**
     * Concatenate values of an arbitrary number of string traversals to the incoming traverser.
     *
     * @return the traversal with an appended {@link ConcatStep}.
     * @param concatTraversal the traversal to concatenate.
     * @param otherConcatTraversals additional traversals to concatenate.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#concat-step" target="_blank">Reference Documentation - Concat Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, String> concat(final Traversal<?, String> concatTraversal, final Traversal<?, String>... otherConcatTraversals) {
        this.asAdmin().getGremlinLang().addStep(Symbols.concat, concatTraversal, otherConcatTraversals);
        return this.asAdmin().addStep(new ConcatStep<>(this.asAdmin(), concatTraversal, otherConcatTraversals));
    }

    /**
     * Concatenate an arbitrary number of strings to the incoming traverser.
     *
     * @return the traversal with an appended {@link ConcatStep}.
     * @param concatStrings the String values to concatenate.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#concat-step" target="_blank">Reference Documentation - Concat Step</a>
     * @since 3.7.0
     */
    public default GraphTraversal<S, String> concat(final String... concatStrings) {
        this.asAdmin().getGremlinLang().addStep(Symbols.concat, concatStrings);
        return this.asAdmin().addStep(new ConcatStep<>(this.asAdmin(), concatStrings));
    }

    /**
     * Returns the value of incoming traverser as strings. Null values are returned as a string value "null".
     *
     * @return the traversal with an appended {@link AsStringGlobalStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#asString-step" target="_blank">Reference Documentation - AsString Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, String> asString() {
        this.asAdmin().getGremlinLang().addStep(Symbols.asString);
        return this.asAdmin().addStep(new AsStringGlobalStep<>(this.asAdmin()));
    }

    /**
     * Returns the value of incoming traverser as strings. Null values are returned as a string value "null".
     *
     * @param scope local will operate on individual strings within incoming lists, global will operate on current traversal as a single object.
     * @return the traversal with an appended {@link AsStringGlobalStep} or {@link AsStringLocalStep} depending on the {@link Scope}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#asString-step" target="_blank">Reference Documentation - AsString Step</a>
     * @since 3.7.1
     */
    public default <E2> GraphTraversal<S, E2> asString(final Scope scope) {
        this.asAdmin().getGremlinLang().addStep(Symbols.asString, scope);
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new AsStringGlobalStep<>(this.asAdmin()) : new AsStringLocalStep<>(this.asAdmin()));
    }

    /**
     * Returns the length incoming string traverser. Null values are not processed and remain as null when returned.
     * If the incoming traverser is a non-String value then an {@code IllegalArgumentException} will be thrown.
     *
     * @return the traversal with an appended {@link LengthGlobalStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#length-step" target="_blank">Reference Documentation - Length Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, Integer> length() {
        this.asAdmin().getGremlinLang().addStep(Symbols.length);
        return this.asAdmin().addStep(new LengthGlobalStep<>(this.asAdmin()));
    }

    /**
     * Returns the length incoming string or list. Null values are not processed and remain as null when returned.
     * If the incoming traverser is a non-String value then an {@code IllegalArgumentException} will be thrown.
     *
     * @param scope local will operate on individual strings within lists, global will operate on current traversal as a single object.
     * @return the traversal with an appended {@link LengthGlobalStep} or {@link LengthLocalStep} depending on the {@link Scope}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#length-step" target="_blank">Reference Documentation - Length Step</a>
     * @since 3.7.1
     */
    public default <E2> GraphTraversal<S, E2> length(final Scope scope) {
        this.asAdmin().getGremlinLang().addStep(Symbols.length, scope);
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new LengthGlobalStep<>(this.asAdmin()) : new LengthLocalStep<>(this.asAdmin()));
    }

    /**
     * Returns the lowercase representation of incoming string traverser. Null values are not processed and remain
     * as null when returned. If the incoming traverser is a non-String value then an {@code IllegalArgumentException} will be thrown.
     *
     * @return the traversal with an appended {@link ToLowerGlobalStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#toLower-step" target="_blank">Reference Documentation - ToLower Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, String> toLower() {
        this.asAdmin().getGremlinLang().addStep(Symbols.toLower);
        return this.asAdmin().addStep(new ToLowerGlobalStep<>(this.asAdmin()));
    }

    /**
     * Returns the lowercase representation of incoming string or list of strings. Null values are not processed and remain
     * as null when returned. If the incoming traverser is a non-String value then an {@code IllegalArgumentException} will be thrown.
     *
     * @param scope local will accept lists of string and operate on individual strings within the list, global will only accept string objects.
     * @return the traversal with an appended {@link ToLowerGlobalStep} or {@link ToLowerLocalStep} depending on the {@link Scope}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#toLower-step" target="_blank">Reference Documentation - ToLower Step</a>
     * @since 3.7.1
     */
    public default <E2> GraphTraversal<S, E2> toLower(final Scope scope) {
        this.asAdmin().getGremlinLang().addStep(Symbols.toLower, scope);
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new ToLowerGlobalStep<>(this.asAdmin()) : new ToLowerLocalStep<>(this.asAdmin()));
    }

    /**
     * Returns the uppercase representation of incoming string traverser. Null values are not processed and
     * remain as null when returned. If the incoming traverser is a non-String value then an {@code IllegalArgumentException} will be thrown.
     *
     * @return the traversal with an appended {@link ToUpperGlobalStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#toUpper-step" target="_blank">Reference Documentation - ToUpper Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, String> toUpper() {
        this.asAdmin().getGremlinLang().addStep(Symbols.toUpper);
        return this.asAdmin().addStep(new ToUpperGlobalStep<>(this.asAdmin()));
    }

    /**
     * Returns the uppercase representation of incoming string or list of strings. Null values are not processed and
     * remain as null when returned. If the incoming traverser is a non-String value then an {@code IllegalArgumentException} will be thrown.
     *
     * @param scope local will accept lists of string and operate on individual strings within the list, global will only accept string objects.
     * @return the traversal with an appended {@link ToUpperGlobalStep} or {@link ToUpperLocalStep} depending on the {@link Scope}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#toUpper-step" target="_blank">Reference Documentation - ToUpper Step</a>
     * @since 3.7.1
     */
    public default <E2> GraphTraversal<S, E2> toUpper(final Scope scope) {
        this.asAdmin().getGremlinLang().addStep(Symbols.toUpper, scope);
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new ToUpperGlobalStep<>(this.asAdmin()) : new ToUpperLocalStep<>(this.asAdmin()));
    }

    /**
     * Returns a string with leading and trailing whitespace removed. Null values are not processed and
     * remain as null when returned. If the incoming traverser is a non-String value then an
     * {@code IllegalArgumentException} will be thrown.
     *
     * @return the traversal with an appended {@link TrimGlobalStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#trim-step" target="_blank">Reference Documentation - Trim Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, String> trim() {
        this.asAdmin().getGremlinLang().addStep(Symbols.trim);
        return this.asAdmin().addStep(new TrimGlobalStep<>(this.asAdmin()));
    }

    /**
     * Returns a string with leading and trailing whitespace removed. Null values are not processed and
     * remain as null when returned. If the incoming traverser is a non-String value then an
     * {@code IllegalArgumentException} will be thrown.
     *
     * @param scope local will operate on individual strings within incoming lists, global will operate on current traversal as a single object.
     * @return the traversal with an appended {@link TrimGlobalStep} or {@link TrimLocalStep} depending on the {@link Scope}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#trim-step" target="_blank">Reference Documentation - Trim Step</a>
     * @since 3.7.1
     */
    public default <E2> GraphTraversal<S, E2> trim(final Scope scope) {
        this.asAdmin().getGremlinLang().addStep(Symbols.trim, scope);
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new TrimGlobalStep<>(this.asAdmin()) : new TrimLocalStep<>(this.asAdmin()));
    }

    /**
     * Returns a string with leading whitespace removed. Null values are not processed and
     * remain as null when returned. If the incoming traverser is a non-String value then an
     * {@code IllegalArgumentException} will be thrown.
     *
     * @return the traversal with an appended {@link LTrimGlobalStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#lTrim-step" target="_blank">Reference Documentation - LTrim Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, String> lTrim() {
        this.asAdmin().getGremlinLang().addStep(Symbols.lTrim);
        return this.asAdmin().addStep(new LTrimGlobalStep<>(this.asAdmin()));
    }


    /**
     * Returns a string with leading whitespace removed. Null values are not processed and
     * remain as null when returned. If the incoming traverser is a non-String value then an
     * {@code IllegalArgumentException} will be thrown.
     *
     * @param scope local will operate on individual strings within incoming lists, global will operate on current traversal as a single object.
     * @return the traversal with an appended {@link LTrimGlobalStep} or {@link LTrimLocalStep} depending on the {@link Scope}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#lTrim-step" target="_blank">Reference Documentation - LTrim Step</a>
     * @since 3.7.1
     */
    public default <E2> GraphTraversal<S, E2> lTrim(final Scope scope) {
        this.asAdmin().getGremlinLang().addStep(Symbols.lTrim, scope);
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new LTrimGlobalStep<>(this.asAdmin()) : new LTrimLocalStep<>(this.asAdmin()));
    }

    /**
     * Returns a string with trailing whitespace removed. Null values are not processed and
     * remain as null when returned. If the incoming traverser is a non-String value then an
     * {@code IllegalArgumentException} will be thrown.
     *
     * @return the traversal with an appended {@link RTrimGlobalStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#rTrim-step" target="_blank">Reference Documentation - RTrim Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, String> rTrim() {
        this.asAdmin().getGremlinLang().addStep(Symbols.rTrim);
        return this.asAdmin().addStep(new RTrimGlobalStep<>(this.asAdmin()));
    }

    /**
     * Returns a string with trailing whitespace removed. Null values are not processed and
     * remain as null when returned. If the incoming traverser is a non-String value then an
     * {@code IllegalArgumentException} will be thrown.
     *
     * @param scope local will operate on individual strings within incoming lists, global will operate on current traversal as a single object.
     * @return the traversal with an appended {@link RTrimGlobalStep} or {@link RTrimLocalStep} depending on the {@link Scope}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#rTrim-step" target="_blank">Reference Documentation - RTrim Step</a>
     * @since 3.7.1
     */
    public default <E2> GraphTraversal<S, E2> rTrim(final Scope scope) {
        this.asAdmin().getGremlinLang().addStep(Symbols.rTrim, scope);
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new RTrimGlobalStep<>(this.asAdmin()) : new RTrimLocalStep<>(this.asAdmin()));
    }

    /**
     * Returns the reverse of the incoming traverser. Null values are not processed and remain as null when returned.
     *
     * @return the traversal with an appended {@link ReverseStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#reverse-step" target="_blank">Reference Documentation - Reverse Step</a>
     * @since 3.7.1
     */
    public default <E2> GraphTraversal<S, E2> reverse() {
        this.asAdmin().getGremlinLang().addStep(Symbols.reverse);
        return this.asAdmin().addStep(new ReverseStep<>(this.asAdmin()));
    }

    /**
     * Returns a string with the specified characters in the original string replaced with the new characters. Any null
     * arguments will be a no-op and the original string is returned. Null values from incoming traversers are not
     * processed and remain as null when returned. If the incoming traverser is a non-String value then an
     * {@code IllegalArgumentException} will be thrown.
     *
     * @param newChar the character to replace.
     * @param oldChar the character to be replaced.
     * @return the traversal with an appended {@link ReplaceGlobalStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#replace-step" target="_blank">Reference Documentation - Replace Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, String> replace(final String oldChar, final String newChar) {
        this.asAdmin().getGremlinLang().addStep(Symbols.replace, oldChar, newChar);
        return this.asAdmin().addStep(new ReplaceGlobalStep<>(this.asAdmin(), oldChar, newChar));
    }

    /**
     * Returns a string with the specified characters in the original string replaced with the new characters. Any null
     * arguments will be a no-op and the original string is returned. Null values from incoming traversers are not
     * processed and remain as null when returned. If the incoming traverser is a non-String value then an
     * {@code IllegalArgumentException} will be thrown.
     *
     * @param scope local will operate on individual strings within incoming lists, global will operate on current traversal as a single object.
     * @param newChar the character to replace.
     * @param oldChar the character to be replaced.
     * @return the traversal with an appended {@link ReplaceGlobalStep} or {@link ReplaceLocalStep} depending on the {@link Scope}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#replace-step" target="_blank">Reference Documentation - Replace Step</a>
     * @since 3.7.1
     */
    public default <E2> GraphTraversal<S, E2> replace(final Scope scope, final String oldChar, final String newChar) {
        this.asAdmin().getGremlinLang().addStep(Symbols.replace, scope, oldChar, newChar);
        return this.asAdmin().addStep(scope.equals(Scope.global) ?
                new ReplaceGlobalStep<>(this.asAdmin(), oldChar, newChar) : new ReplaceLocalStep<>(this.asAdmin(), oldChar, newChar));
    }

    /**
     * Returns a list of strings created by splitting the incoming string traverser around the matches of the given separator.
     * A null separator will split the string by whitespaces. Null values from incoming traversers are not processed
     * and remain as null when returned. If the incoming traverser is a non-String value then an
     * {@code IllegalArgumentException} will be thrown.
     *
     * @param separator the character to split the string on.
     * @return the traversal with an appended {@link SplitGlobalStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#split-step" target="_blank">Reference Documentation - Split Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, List<String>> split(final String separator) {
        this.asAdmin().getGremlinLang().addStep(Symbols.split, separator);
        return this.asAdmin().addStep(new SplitGlobalStep<>(this.asAdmin(), separator));
    }

    /**
     * Returns a list of strings created by splitting the incoming string traverser around the matches of the given separator.
     * A null separator will split the string by whitespaces. Null values from incoming traversers are not processed
     * and remain as null when returned. If the incoming traverser is a non-String value then an
     * {@code IllegalArgumentException} will be thrown.
     *
     * @param scope local will operate on individual strings within incoming lists, global will operate on current traversal as a single object.
     * @param separator the character to split the string on.
     * @return the traversal with an appended {@link SplitGlobalStep} or {@link SplitLocalStep} depending on the {@link Scope}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#split-step" target="_blank">Reference Documentation - Split Step</a>
     * @since 3.7.1
     */
    public default <E2> GraphTraversal<S, List<E2>> split(final Scope scope, final String separator) {
        this.asAdmin().getGremlinLang().addStep(Symbols.split, scope, separator);
        return this.asAdmin().addStep(scope.equals(Scope.global) ?
                new SplitGlobalStep<>(this.asAdmin(), separator) : new SplitLocalStep<>(this.asAdmin(), separator));
    }

    /**
     * Returns a substring of the incoming string traverser with a 0-based start index (inclusive) specified,
     * to the end of the string. If the start index is negative then it will begin at the specified index counted from the
     * end of the string, or 0 if exceeding the string length. Null values are not processed and remain as null when returned.
     * If the incoming traverser is a non-String value then an {@code IllegalArgumentException} will be thrown.
     *
     * @param startIndex the start index of the substring, inclusive.
     * @return the traversal with an appended {@link SubstringGlobalStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#replace-step" target="_blank">Reference Documentation - Substring Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, String> substring(final int startIndex) {
        this.asAdmin().getGremlinLang().addStep(Symbols.substring, startIndex);
        return this.asAdmin().addStep(new SubstringGlobalStep<>(this.asAdmin(), startIndex));
    }

    /**
     * Returns a substring of the incoming string traverser with a 0-based start index (inclusive) specified,
     * to the end of the string. If the start index is negative then it will begin at the specified index counted from the
     * end of the string, or 0 if exceeding the string length. Null values are not processed and remain as null when returned.
     * If the incoming traverser is a non-String value then an {@code IllegalArgumentException} will be thrown.
     *
     * @param scope local will operate on individual strings within incoming lists, global will operate on current traversal as a single object.
     * @param startIndex the start index of the substring, inclusive.
     * @return the traversal with an appended {@link SubstringGlobalStep} or {@link SubstringLocalStep} depending on the {@link Scope}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#replace-step" target="_blank">Reference Documentation - Substring Step</a>
     * @since 3.7.1
     */
    public default <E2> GraphTraversal<S, E2> substring(final Scope scope, final int startIndex) {
        this.asAdmin().getGremlinLang().addStep(Symbols.substring, scope, startIndex);
        return this.asAdmin().addStep(scope.equals(Scope.global) ?
                new SubstringGlobalStep<>(this.asAdmin(), startIndex) : new SubstringLocalStep<>(this.asAdmin(), startIndex));
    }

    /**
     * Returns a substring of the incoming string traverser with a 0-based start index (inclusive) and end index
     * (exclusive). If the start index is negative then it will begin at the specified index counted from the end of the
     * string, or 0 if exceeding the string length. If the end index is negative then it will end at the specified index
     * counted from the end, or at the end of the string if exceeding the string length. End index <= start index will
     * return the empty string. Null values are not processed and remain as null when returned. If the incoming
     * traverser is a non-String value then an {@code IllegalArgumentException} will be thrown.
     *
     * @param startIndex the start index of the substring, inclusive.
     * @param endIndex the end index of the substring, exclusive.
     * @return the traversal with an appended {@link SubstringGlobalStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#replace-step" target="_blank">Reference Documentation - Substring Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, String> substring(final int startIndex, final int endIndex) {
        this.asAdmin().getGremlinLang().addStep(Symbols.substring, startIndex, endIndex);
        return this.asAdmin().addStep(new SubstringGlobalStep<>(this.asAdmin(), startIndex, endIndex));
    }

    /**
     * Returns a substring of the incoming string traverser with a 0-based start index (inclusive) and end index
     * (exclusive). If the start index is negative then it will begin at the specified index counted from the end of the
     * string, or 0 if exceeding the string length. If the end index is negative then it will end at the specified index
     * counted from the end, or at the end of the string if exceeding the string length. End index <= start index will
     * return the empty string. Null values are not processed and remain as null when returned. If the incoming
     * traverser is a non-String value then an {@code IllegalArgumentException} will be thrown.
     *
     * @param scope local will operate on individual strings within incoming lists, global will operate on current traversal as a single object.
     * @param startIndex the start index of the substring, inclusive.
     * @param endIndex the end index of the substring, exclusive.
     * @return the traversal with an appended {@link SubstringGlobalStep} or {@link SubstringLocalStep} depending on the {@link Scope}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#replace-step" target="_blank">Reference Documentation - Substring Step</a>
     * @since 3.7.1
     */
    public default <E2> GraphTraversal<S, E2> substring(final Scope scope, final int startIndex, final int endIndex) {
        this.asAdmin().getGremlinLang().addStep(Symbols.substring, scope, startIndex, endIndex);
        return this.asAdmin().addStep(scope.equals(Scope.global) ?
                new SubstringGlobalStep<>(this.asAdmin(), startIndex, endIndex) : new SubstringLocalStep<>(this.asAdmin(), startIndex, endIndex));
    }

    /**
     * A mid-traversal step which will handle result formatting to string values.
     *
     * @return the traversal with an appended {@link FormatStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#format-step" target="_blank">Reference Documentation - format Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, String> format(final String format) {
        this.asAdmin().getGremlinLang().addStep(Symbols.format, format);
        return this.asAdmin().addStep(new FormatStep<>(this.asAdmin(), format));
    }

    /**
     * Parse value of the incoming traverser as an ISO-8601 {@link OffsetDateTime}.
     *
     * @return the traversal with an appended {@link AsDateStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#asDate-step" target="_blank">Reference Documentation - asDate Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, OffsetDateTime> asDate() {
        this.asAdmin().getGremlinLang().addStep(Symbols.asDate);
        return this.asAdmin().addStep(new AsDateStep<>(this.asAdmin()));
    }

    /**
     * Increase value of input {@link OffsetDateTime}.
     *
     * @return the traversal with an appended {@link DateAddStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#dateAdd-step" target="_blank">Reference Documentation - dateAdd Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, OffsetDateTime> dateAdd(final DT dateToken, final int value) {
        this.asAdmin().getGremlinLang().addStep(Symbols.dateAdd, dateToken, value);
        return this.asAdmin().addStep(new DateAddStep<>(this.asAdmin(), dateToken, value));
    }

    /**
     * Returns the difference between two {@link OffsetDateTime} in epoch time.
     *
     * @return the traversal with an appended {@link DateDiffStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#dateDiff-step" target="_blank">Reference Documentation - dateDiff Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, Long> dateDiff(final OffsetDateTime value) {
        this.asAdmin().getGremlinLang().addStep(Symbols.dateDiff, value);
        return this.asAdmin().addStep(new DateDiffStep<>(this.asAdmin(), value));
    }

    /**
     * Returns the difference between two {@link OffsetDateTime} in epoch time.
     *
     * @return the traversal with an appended {@link DateDiffStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#dateDiff-step" target="_blank">Reference Documentation - dateDiff Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, Long> dateDiff(final Traversal<?, OffsetDateTime> dateTraversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.dateDiff, dateTraversal);
        return this.asAdmin().addStep(new DateDiffStep<>(this.asAdmin(), dateTraversal));
    }

    /**
     * Calculates the difference between the list traverser and list argument.
     *
     * @return the traversal with an appended {@link DifferenceStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#difference-step" target="_blank">Reference Documentation - Difference Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, Set<?>> difference(final Object values) {
        this.asAdmin().getGremlinLang().addStep(Symbols.difference, values);
        return this.asAdmin().addStep(new DifferenceStep<>(this.asAdmin(), values));
    }

    /**
     * Calculates the disjunction between the list traverser and list argument.
     *
     * @return the traversal with an appended {@link DisjunctStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#disjunct-step" target="_blank">Reference Documentation - Disjunct Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, Set<?>> disjunct(final Object values) {
        this.asAdmin().getGremlinLang().addStep(Symbols.disjunct, values);
        return this.asAdmin().addStep(new DisjunctStep<>(this.asAdmin(), values));
    }

    /**
     * Calculates the intersection between the list traverser and list argument.
     *
     * @return the traversal with an appended {@link IntersectStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#intersect-step" target="_blank">Reference Documentation - Intersect Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, Set<?>> intersect(final Object values) {
        this.asAdmin().getGremlinLang().addStep(Symbols.intersect, values);
        return this.asAdmin().addStep(new IntersectStep<>(this.asAdmin(), values));
    }

    /**
     * Joins together the elements of the incoming list traverser together with the provided delimiter.
     *
     * @return the traversal with an appended {@link ConjoinStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#conjoin-step" target="_blank">Reference Documentation - Conjoin Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, String> conjoin(final String delimiter) {
        this.asAdmin().getGremlinLang().addStep(Symbols.conjoin, delimiter);
        return this.asAdmin().addStep(new ConjoinStep<>(this.asAdmin(), delimiter));
    }

    /**
     * Merges the list traverser and list argument. Also known as union.
     *
     * @return the traversal with an appended {@link MergeStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#merge-step" target="_blank">Reference Documentation - Merge Step</a>
     * @since 3.7.1
     */
    public default <E2> GraphTraversal<S, E2> merge(final Object values) {
        this.asAdmin().getGremlinLang().addStep(Symbols.merge, values);
        return this.asAdmin().addStep(new MergeStep<>(this.asAdmin(), values));
    }

    /**
     * Combines the list traverser and list argument. Also known as concatenation or append.
     *
     * @return the traversal with an appended {@link CombineStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#combine-step" target="_blank">Reference Documentation - Combine Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, List<?>> combine(final Object values) {
        this.asAdmin().getGremlinLang().addStep(Symbols.combine, values);
        return this.asAdmin().addStep(new CombineStep<>(this.asAdmin(), values));
    }

    /**
     * Calculates the cartesian product between the list traverser and list argument.
     *
     * @return the traversal with an appended {@link ProductStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#product-step" target="_blank">Reference Documentation - Product Step</a>
     * @since 3.7.1
     */
    public default GraphTraversal<S, List<List<?>>> product(final Object values) {
        this.asAdmin().getGremlinLang().addStep(Symbols.product, values);
        return this.asAdmin().addStep(new ProductStep<>(this.asAdmin(), values));
    }

    ///////////////////// FILTER STEPS /////////////////////

    /**
     * Map the {@link Traverser} to either {@code true} or {@code false}, where {@code false} will not pass the
     * traverser to the next step.
     *
     * @param predicate the filter function to apply
     * @return the traversal with the {@link LambdaFilterStep} added
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#general-steps" target="_blank">Reference Documentation - General Steps</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> filter(final Predicate<Traverser<E>> predicate) {
        this.asAdmin().getGremlinLang().addStep(Symbols.filter, predicate);
        return this.asAdmin().addStep(new LambdaFilterStep<>(this.asAdmin(), predicate));
    }

    /**
     * Map the {@link Traverser} to either {@code true} or {@code false}, where {@code false} will not pass the
     * traverser to the next step.
     *
     * @param filterTraversal the filter traversal to apply
     * @return the traversal with the {@link TraversalFilterStep} added
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#general-steps" target="_blank">Reference Documentation - General Steps</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> filter(final Traversal<?, ?> filterTraversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.filter, filterTraversal);
        return this.asAdmin().addStep(new TraversalFilterStep<>(this.asAdmin(), (Traversal) filterTraversal));
    }

    /**
     * Filter all traversers in the traversal. This step has narrow use cases and is primarily intended for use as a
     * signal to remote servers that {@link #iterate()} was called. While it may be directly used, it is often a sign
     * that a traversal should be re-written in another form.
     *
     * @return the updated traversal with respective {@link DiscardStep}.
     */
    @Override
    default GraphTraversal<S, E> discard() {
        return (GraphTraversal<S, E>) Traversal.super.discard();
    }

    /**
     * Ensures that at least one of the provided traversals yield a result.
     *
     * @param orTraversals filter traversals where at least one must be satisfied
     * @return the traversal with an appended {@link OrStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#or-step" target="_blank">Reference Documentation - Or Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> or(final Traversal<?, ?>... orTraversals) {
        this.asAdmin().getGremlinLang().addStep(Symbols.or, orTraversals);
        return this.asAdmin().addStep(new OrStep(this.asAdmin(), orTraversals));
    }

    /**
     * Ensures that all of the provided traversals yield a result.
     *
     * @param andTraversals filter traversals that must be satisfied
     * @return the traversal with an appended {@link AndStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#and-step" target="_blank">Reference Documentation - And Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> and(final Traversal<?, ?>... andTraversals) {
        this.asAdmin().getGremlinLang().addStep(Symbols.and, andTraversals);
        return this.asAdmin().addStep(new AndStep(this.asAdmin(), andTraversals));
    }

    /**
     * Provides a way to add arbitrary objects to a traversal stream.
     *
     * @param injections the objects to add to the stream
     * @return the traversal with an appended {@link InjectStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#inject-step" target="_blank">Reference Documentation - Inject Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> inject(final E... injections) {
        // a single null is [null]
        final E[] s = null == injections ? (E[]) new Object[] { null } : injections;
        this.asAdmin().getGremlinLang().addStep(Symbols.inject, s);
        return this.asAdmin().addStep(new InjectStep<>(this.asAdmin(), s));
    }

    /**
     * Remove all duplicates in the traversal stream up to this point.
     *
     * @param scope       whether the deduplication is on the stream (global) or the current object (local).
     * @param dedupLabels if labels are provided, then the scope labels determine de-duplication. No labels implies current object.
     * @return the traversal with an appended {@link DedupGlobalStep} or {@link DedupLocalStep} depending on {@code scope}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#dedup-step" target="_blank">Reference Documentation - Dedup Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> dedup(final Scope scope, final String... dedupLabels) {
        this.asAdmin().getGremlinLang().addStep(Symbols.dedup, scope, dedupLabels);
        return this.asAdmin().addStep(scope.equals(Scope.global) ? new DedupGlobalStep<>(this.asAdmin(), dedupLabels) : new DedupLocalStep(this.asAdmin()));
    }

    /**
     * Remove all duplicates in the traversal stream up to this point.
     *
     * @param dedupLabels if labels are provided, then the scoped object's labels determine de-duplication. No labels implies current object.
     * @return the traversal with an appended {@link DedupGlobalStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#dedup-step" target="_blank">Reference Documentation - Dedup Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> dedup(final String... dedupLabels) {
        this.asAdmin().getGremlinLang().addStep(Symbols.dedup, dedupLabels);
        return this.asAdmin().addStep(new DedupGlobalStep<>(this.asAdmin(), dedupLabels));
    }

    /**
     * Filters the current object based on the object itself or the path history.
     *
     * @param startKey  the key containing the object to filter
     * @param predicate the filter to apply
     * @return the traversal with an appended {@link WherePredicateStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#where-step" target="_blank">Reference Documentation - Where Step</a>
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#using-where-with-match" target="_blank">Reference Documentation - Where with Match</a>
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#using-where-with-select" target="_blank">Reference Documentation - Where with Select</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> where(final String startKey, final P<String> predicate) {
        this.asAdmin().getGremlinLang().addStep(Symbols.where, startKey, predicate);
        return this.asAdmin().addStep(new WherePredicateStep<>(this.asAdmin(), Optional.ofNullable(startKey), predicate));
    }

    /**
     * Filters the current object based on the object itself or the path history.
     *
     * @param predicate the filter to apply
     * @return the traversal with an appended {@link WherePredicateStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#where-step" target="_blank">Reference Documentation - Where Step</a>
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#using-where-with-match" target="_blank">Reference Documentation - Where with Match</a>
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#using-where-with-select" target="_blank">Reference Documentation - Where with Select</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> where(final P<String> predicate) {
        this.asAdmin().getGremlinLang().addStep(Symbols.where, predicate);
        return this.asAdmin().addStep(new WherePredicateStep<>(this.asAdmin(), Optional.empty(), predicate));
    }

    /**
     * Filters the current object based on the object itself or the path history.
     *
     * @param whereTraversal the filter to apply
     * @return the traversal with an appended {@link WherePredicateStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#where-step" target="_blank">Reference Documentation - Where Step</a>
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#using-where-with-match" target="_blank">Reference Documentation - Where with Match</a>
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#using-where-with-select" target="_blank">Reference Documentation - Where with Select</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> where(final Traversal<?, ?> whereTraversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.where, whereTraversal);
        return TraversalHelper.getVariableLocations(whereTraversal.asAdmin()).isEmpty() ?
                this.asAdmin().addStep(new TraversalFilterStep<>(this.asAdmin(), (Traversal) whereTraversal)) :
                this.asAdmin().addStep(new WhereTraversalStep<>(this.asAdmin(), whereTraversal));
    }

    /**
     * Filters vertices, edges and vertex properties based on their properties.
     *
     * @param propertyKey the key of the property to filter on
     * @param predicate   the filter to apply to the key's value
     * @return the traversal with an appended {@link HasStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step" target="_blank">Reference Documentation - Has Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> has(final String propertyKey, final P<?> predicate) {
        // Groovy can get the overload wrong for has(T, null) which should probably go at has(T,Object). users could
        // explicit cast but a redirect here makes this a bit more seamless
        if (null == predicate)
            return has(propertyKey, (Object) null);

        this.asAdmin().getGremlinLang().addStep(Symbols.has, propertyKey, predicate);
        return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(propertyKey, predicate));
    }

    /**
     * Filters vertices, edges and vertex properties based on their properties.
     *
     * @param accessor  the {@link T} accessor of the property to filter on
     * @param predicate the filter to apply to the key's value
     * @return the traversal with an appended {@link HasStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step" target="_blank">Reference Documentation - Has Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> has(final T accessor, final P<?> predicate) {
        if (null == accessor)
            throw new IllegalArgumentException("The T accessor value of has(T,Object) cannot be null");

        // Groovy can get the overload wrong for has(T, null) which should probably go at has(T,Object). users could
        // explicit cast but a redirect here makes this a bit more seamless
        if (null == predicate)
            return has(accessor, (Object) null);

        this.asAdmin().getGremlinLang().addStep(Symbols.has, accessor, predicate);
        return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(accessor.getAccessor(), predicate));
    }

    /**
     * Filters vertices, edges and vertex properties based on their properties.
     *
     * @param propertyKey the key of the property to filter on
     * @param value       the value to compare the property value to for equality
     * @return the traversal with an appended {@link HasStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step" target="_blank">Reference Documentation - Has Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> has(final String propertyKey, final Object value) {
        if (value instanceof P)
            return this.has(propertyKey, (P) value);
        else if (value instanceof Traversal)
            return this.has(propertyKey, (Traversal) value);
        else {
            this.asAdmin().getGremlinLang().addStep(Symbols.has, propertyKey, value);
            return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(propertyKey, P.eq(value)));
        }
    }

    /**
     * Filters vertices, edges and vertex properties based on their properties.
     *
     * @param accessor the {@link T} accessor of the property to filter on
     * @param value    the value to compare the accessor value to for equality
     * @return the traversal with an appended {@link HasStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step" target="_blank">Reference Documentation - Has Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> has(final T accessor, final Object value) {
        if (null == accessor)
            throw new IllegalArgumentException("The T accessor value of has(T,Object) cannot be null");

        if (value instanceof P)
            return this.has(accessor, (P) value);
        else if (value instanceof Traversal)
            return this.has(accessor, (Traversal) value);
        else {
            this.asAdmin().getGremlinLang().addStep(Symbols.has, accessor, value);
            return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(accessor.getAccessor(), P.eq(value)));
        }
    }

    /**
     * Filters vertices, edges and vertex properties based on their properties.
     *
     * @param label       the label of the {@link Element}
     * @param propertyKey the key of the property to filter on
     * @param predicate   the filter to apply to the key's value
     * @return the traversal with an appended {@link HasStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step" target="_blank">Reference Documentation - Has Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> has(final String label, final String propertyKey, final P<?> predicate) {
        this.asAdmin().getGremlinLang().addStep(Symbols.has, label, propertyKey, predicate);
        TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(T.label.getAccessor(), P.eq(label)));
        return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(propertyKey, predicate));
    }

    /**
     * Filters vertices, edges and vertex properties based on their properties.
     *
     * @param label       the label of the {@link Element}
     * @param propertyKey the key of the property to filter on
     * @param value       the value to compare the accessor value to for equality
     * @return the traversal with an appended {@link HasStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step" target="_blank">Reference Documentation - Has Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> has(final String label, final String propertyKey, final Object value) {
        this.asAdmin().getGremlinLang().addStep(Symbols.has, label, propertyKey, value);
        TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(T.label.getAccessor(), P.eq(label)));
        return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(propertyKey, value instanceof P ? (P) value : P.eq(value)));
    }

    /**
     * Filters vertices, edges and vertex properties based on their value of {@link T} where only {@link T#id} and
     * {@link T#label} are supported.
     *
     * @param accessor          the {@link T} accessor of the property to filter on
     * @param propertyTraversal the traversal to filter the accessor value by
     * @return the traversal with an appended {@link HasStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step" target="_blank">Reference Documentation - Has Step</a>
     * @since 3.1.0-incubating
     */
    public default GraphTraversal<S, E> has(final T accessor, final Traversal<?, ?> propertyTraversal) {
        if (null == accessor)
            throw new IllegalArgumentException("The T accessor value of has(T,Object) cannot be null");

        // Groovy can get the overload wrong for has(T, null) which should probably go at has(T,Object). users could
        // explicit cast but a redirect here makes this a bit more seamless
        if (null == propertyTraversal)
            return has(accessor, (Object) null);

        this.asAdmin().getGremlinLang().addStep(Symbols.has, accessor, propertyTraversal);
        switch (accessor) {
            case id:
                return this.asAdmin().addStep(
                        new TraversalFilterStep<>(this.asAdmin(), propertyTraversal.asAdmin().addStep(0,
                                new IdStep<>(propertyTraversal.asAdmin()))));
            case label:
                return this.asAdmin().addStep(
                    new TraversalFilterStep<>(this.asAdmin(), propertyTraversal.asAdmin().addStep(0,
                            new LabelStep<>(propertyTraversal.asAdmin()))));
            default:
                throw new IllegalArgumentException("has(T,Traversal) can only take id or label as its argument");
        }

    }

    /**
     * Filters vertices, edges and vertex properties based on the value of the specified property key.
     *
     * @param propertyKey       the key of the property to filter on
     * @param propertyTraversal the traversal to filter the property value by
     * @return the traversal with an appended {@link HasStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step" target="_blank">Reference Documentation - Has Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> has(final String propertyKey, final Traversal<?, ?> propertyTraversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.has, propertyKey, propertyTraversal);
        return this.asAdmin().addStep(
                new TraversalFilterStep<>(this.asAdmin(), propertyTraversal.asAdmin().addStep(0,
                        new PropertiesStep(propertyTraversal.asAdmin(), PropertyType.VALUE, propertyKey))));
    }

    /**
     * Filters vertices, edges and vertex properties based on the existence of properties.
     *
     * @param propertyKey the key of the property to filter on for existence
     * @return the traversal with an appended {@link HasStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step" target="_blank">Reference Documentation - Has Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> has(final String propertyKey) {
        this.asAdmin().getGremlinLang().addStep(Symbols.has, propertyKey);
        return this.asAdmin().addStep(new TraversalFilterStep(this.asAdmin(), __.values(propertyKey)));
    }

    /**
     * Filters vertices, edges and vertex properties based on the non-existence of properties.
     *
     * @param propertyKey the key of the property to filter on for existence
     * @return the traversal with an appended {@link HasStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step" target="_blank">Reference Documentation - Has Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> hasNot(final String propertyKey) {
        this.asAdmin().getGremlinLang().addStep(Symbols.hasNot, propertyKey);
        return this.asAdmin().addStep(new NotStep(this.asAdmin(), __.values(propertyKey)));
    }

    /**
     * Filters vertices, edges and vertex properties based on their label.
     *
     * @param label       the label of the {@link Element}
     * @param otherLabels additional labels of the {@link Element}
     * @return the traversal with an appended {@link HasStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step" target="_blank">Reference Documentation - Has Step</a>
     * @since 3.2.2
     */
    public default GraphTraversal<S, E> hasLabel(final String label, final String... otherLabels) {
        this.asAdmin().getGremlinLang().addStep(Symbols.hasLabel, label, otherLabels);

        // groovy evaluation seems to do strange things with varargs given hasLabel(null, null). odd someone would
        // do this but the failure is ugly if not handled.
        final int otherLabelsLength = null == otherLabels ? 0 : otherLabels.length;
        final String[] labels = new String[otherLabelsLength + 1];
        labels[0] = label;
        if (otherLabelsLength > 0)
            System.arraycopy(otherLabels, 0, labels, 1, otherLabelsLength);
        return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(T.label.getAccessor(), labels.length == 1 ? P.eq(labels[0]) : P.within(labels)));
    }

    /**
     * Filters vertices, edges and vertex properties based on their label. Note that calling this step with
     * {@code null} is the same as calling {@link #hasLabel(String, String...)} with a single {@code null}.
     *
     * @param predicate the filter to apply to the label of the {@link Element}
     * @return the traversal with an appended {@link HasStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step" target="_blank">Reference Documentation - Has Step</a>
     * @since 3.2.4
     */
    public default GraphTraversal<S, E> hasLabel(final P<String> predicate) {
        // if calling hasLabel(null), the likely use the caller is going for is not a "no predicate" but a eq(null)
        if (null == predicate) {
            return hasLabel((String) null);
        } else {
            this.asAdmin().getGremlinLang().addStep(Symbols.hasLabel, predicate);
            return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(T.label.getAccessor(), predicate));
        }
    }

    /**
     * Filters vertices, edges and vertex properties based on their identifier.
     *
     * @param id       the identifier of the {@link Element}
     * @param otherIds additional identifiers of the {@link Element}
     * @return the traversal with an appended {@link HasStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step" target="_blank">Reference Documentation - Has Step</a>
     * @since 3.2.2
     */
    public default GraphTraversal<S, E> hasId(final Object id, final Object... otherIds) {
        if (id instanceof P) {
            return this.hasId((P) id);
        }
        else {
            this.asAdmin().getGremlinLang().addStep(Symbols.hasId, id, otherIds);

            //using ArrayList given P.within() turns all arguments into lists
            final List<Object> ids = new ArrayList<>();
            if (id instanceof Object[]) {
                Collections.addAll(ids, (Object[]) id);
            } else if (id instanceof Collection) {
                // as ids are unrolled when it's in array, they should also be unrolled when it's a list.
                // this also aligns with behavior of hasId() when it's pushed down to g.V() (TINKERPOP-2863)
                ids.addAll((Collection<?>) id);
            } else
                ids.add(id);

            // unrolling ids from lists works cleaner with Collection too, as otherwise they will need to
            // be turned into array first
            if (otherIds != null) {
                for (final Object i : otherIds) {
                    if (id instanceof Object[]) {
                        Collections.addAll(ids, (Object[]) i);
                    } else if (i instanceof Collection) {
                        ids.addAll((Collection<?>) i);
                    } else
                        ids.add(i);
                }
            }

            return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(T.id.getAccessor(), ids.size() == 1 ? P.eq(ids.get(0)) : P.within(ids)));
        }
    }

    /**
     * Filters vertices, edges and vertex properties based on their identifier. Calling this step with a {@code null}
     * value will result in effectively calling {@link #hasId(Object, Object...)} wit a single {@code null} identifier
     * and therefore filter all results since a {@link T#id} cannot be {@code null}.
     *
     * @param predicate the filter to apply to the identifier of the {@link Element}
     * @return the traversal with an appended {@link HasStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step" target="_blank">Reference Documentation - Has Step</a>
     * @since 3.2.4
     */
    public default GraphTraversal<S, E> hasId(final P<Object> predicate) {
        if (null == predicate)
            return hasId((Object) null);

        this.asAdmin().getGremlinLang().addStep(Symbols.hasId, predicate);
        return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(T.id.getAccessor(), predicate));
    }

    /**
     * Filters {@link Property} objects based on their key. It is not meant to test key existence on an {@link Edge} or
     * a {@link Vertex}. In that case, prefer {@link #has(String)}.
     *
     * @param label       the key of the {@link Property}
     * @param otherLabels additional key of the {@link Property}
     * @return the traversal with an appended {@link HasStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step" target="_blank">Reference Documentation - Has Step</a>
     * @since 3.2.2
     */
    public default GraphTraversal<S, E> hasKey(final String label, final String... otherLabels) {
        this.asAdmin().getGremlinLang().addStep(Symbols.hasKey, label, otherLabels);

        // groovy evaluation seems to do strange things with varargs given hasLabel(null, null). odd someone would
        // do this but the failure is ugly if not handled.
        final int otherLabelsLength = null == otherLabels ? 0 : otherLabels.length;
        final String[] labels = new String[otherLabelsLength + 1];
        labels[0] = label;
        if (otherLabelsLength > 0)
            System.arraycopy(otherLabels, 0, labels, 1, otherLabelsLength);
        return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(T.key.getAccessor(), labels.length == 1 ? P.eq(labels[0]) : P.within(labels)));
    }

    /**
     * Filters {@link Property} objects based on their key. It is not meant to test key existence on an {@link Edge} or
     * a {@link Vertex}. In that case, prefer {@link #has(String)}. Note that calling this step with {@code null} is
     * the same as calling {@link #hasKey(String, String...)} with a single {@code null}.
     *
     * @param predicate the filter to apply to the key of the {@link Property}
     * @return the traversal with an appended {@link HasStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step" target="_blank">Reference Documentation - Has Step</a>
     * @since 3.2.4
     */
    public default GraphTraversal<S, E> hasKey(final P<String> predicate) {
        // if calling hasKey(null), the likely use the caller is going for is not a "no predicate" but a eq(null)
        if (null == predicate) {
            return hasKey((String) null);
        } else {
            this.asAdmin().getGremlinLang().addStep(Symbols.hasKey, predicate);
            return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(T.key.getAccessor(), predicate));
        }
    }

    /**
     * Filters {@link Property} objects based on their value.
     *
     * @param value       the value of the {@link Element}
     * @param otherValues additional values of the {@link Element}
     * @return the traversal with an appended {@link HasStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step" target="_blank">Reference Documentation - Has Step</a>
     */
    public default GraphTraversal<S, E> hasValue(final Object value, final Object... otherValues) {
        if (value instanceof P)
            return this.hasValue((P) value);
        else {
            this.asAdmin().getGremlinLang().addStep(Symbols.hasValue, value, otherValues);
            final List<Object> values = new ArrayList<>();
            if (value instanceof Object[]) {
                Collections.addAll(values, (Object[]) value);
            } else {
                values.add(value);
            }

            if (null == otherValues) {
                values.add(null);
            } else {
                for (final Object v : otherValues) {
                    if (v instanceof Object[]) {
                        Collections.addAll(values, (Object[]) v);
                    } else
                        values.add(v);
                }
            }

            return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(T.value.getAccessor(), values.size() == 1 ? P.eq(values.get(0)) : P.within(values)));
        }
    }

    /**
     * Filters {@link Property} objects based on their value.Note that calling this step with {@code null} is the same
     * as calling {@link #hasValue(Object, Object...)} with a single {@code null}.
     *
     * @param predicate the filter to apply to the value of the {@link Element}
     * @return the traversal with an appended {@link HasStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#has-step" target="_blank">Reference Documentation - Has Step</a>
     * @since 3.2.4
     */
    public default GraphTraversal<S, E> hasValue(final P<Object> predicate) {
        // if calling hasValue(null), the likely use the caller is going for is not a "no predicate" but a eq(null)
        if (null == predicate) {
            return hasValue((String) null);
        } else {
            this.asAdmin().getGremlinLang().addStep(Symbols.hasValue, predicate);
            return TraversalHelper.addHasContainer(this.asAdmin(), new HasContainer(T.value.getAccessor(), predicate));
        }
    }

    /**
     * Filters <code>E</code> object values given the provided {@code predicate}.
     *
     * @param predicate the filter to apply
     * @return the traversal with an appended {@link IsStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#is-step" target="_blank">Reference Documentation - Is Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> is(final P<E> predicate) {
        this.asAdmin().getGremlinLang().addStep(Symbols.is, predicate);
        return this.asAdmin().addStep(new IsStep<>(this.asAdmin(), predicate));
    }

    /**
     * Filter the <code>E</code> object if it is not {@link P#eq} to the provided value.
     *
     * @param value the value that the object must equal.
     * @return the traversal with an appended {@link IsStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#is-step" target="_blank">Reference Documentation - Is Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> is(final Object value) {
        this.asAdmin().getGremlinLang().addStep(Symbols.is, value);
        return this.asAdmin().addStep(new IsStep<>(this.asAdmin(), value instanceof P ? (P<E>) value : P.eq((E) value)));
    }

    /**
     * Removes objects from the traversal stream when the traversal provided as an argument returns any objects.
     *
     * @param notTraversal the traversal to filter by.
     * @return the traversal with an appended {@link NotStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#not-step" target="_blank">Reference Documentation - Not Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> not(final Traversal<?, ?> notTraversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.not, notTraversal);
        return this.asAdmin().addStep(new NotStep<>(this.asAdmin(), (Traversal<E, ?>) notTraversal));
    }

    /**
     * Filter the <code>E</code> object given a biased coin toss.
     *
     * @param probability the probability that the object will pass through
     * @return the traversal with an appended {@link CoinStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#coin-step" target="_blank">Reference Documentation - Coin Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> coin(final double probability) {
        this.asAdmin().getGremlinLang().addStep(Symbols.coin, probability);
        return this.asAdmin().addStep(new CoinStep<>(this.asAdmin(), probability));
    }

    /**
     * Filter the objects in the traversal by the number of them to pass through the stream. Those before the value
     * of {@code low} do not pass through and those that exceed the value of {@code high} will end the iteration.
     *
     * @param low  the number at which to start allowing objects through the stream
     * @param high the number at which to end the stream - use {@code -1} to emit all remaining objects
     * @return the traversal with an appended {@link RangeGlobalStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#range-step" target="_blank">Reference Documentation - Range Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> range(final long low, final long high) {
        this.asAdmin().getGremlinLang().addStep(Symbols.range, low, high);
        return this.asAdmin().addStep(new RangeGlobalStep<>(this.asAdmin(), low, high));
    }

    /**
     * Filter the objects in the traversal by the number of them to pass through the stream as constrained by the
     * {@link Scope}. Those before the value of {@code low} do not pass through and those that exceed the value of
     * {@code high} will end the iteration.
     *
     * @param scope the scope of how to apply the {@code range}
     * @param low   the number at which to start allowing objects through the stream
     * @param high  the number at which to end the stream - use {@code -1} to emit all remaining objects
     * @return the traversal with an appended {@link RangeGlobalStep} or {@link RangeLocalStep} depending on {@code scope}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#range-step" target="_blank">Reference Documentation - Range Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> range(final Scope scope, final long low, final long high) {
        this.asAdmin().getGremlinLang().addStep(Symbols.range, scope, low, high);
        return this.asAdmin().addStep(scope.equals(Scope.global)
                ? new RangeGlobalStep<>(this.asAdmin(), low, high)
                : new RangeLocalStep<>(this.asAdmin(), low, high));
    }

    /**
     * Filter the objects in the traversal by the number of them to pass through the stream, where only the first
     * {@code n} objects are allowed as defined by the {@code limit} argument.
     *
     * @param limit the number at which to end the stream
     * @return the traversal with an appended {@link RangeGlobalStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#limit-step" target="_blank">Reference Documentation - Limit Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> limit(final long limit) {
        this.asAdmin().getGremlinLang().addStep(Symbols.limit, limit);
        return this.asAdmin().addStep(new RangeGlobalStep<>(this.asAdmin(), 0, limit));
    }

    /**
     * Filter the objects in the traversal by the number of them to pass through the stream given the {@link Scope},
     * where only the first {@code n} objects are allowed as defined by the {@code limit} argument.
     *
     * @param scope the scope of how to apply the {@code limit}
     * @param limit the number at which to end the stream
     * @return the traversal with an appended {@link RangeGlobalStep} or {@link RangeLocalStep} depending on {@code scope}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#limit-step" target="_blank">Reference Documentation - Limit Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> limit(final Scope scope, final long limit) {
        this.asAdmin().getGremlinLang().addStep(Symbols.limit, scope, limit);
        return this.asAdmin().addStep(scope.equals(Scope.global)
                ? new RangeGlobalStep<>(this.asAdmin(), 0, limit)
                : new RangeLocalStep<>(this.asAdmin(), 0, limit));
    }

    /**
     * Filters the objects in the traversal emitted as being last objects in the stream. In this case, only the last
     * object will be returned.
     *
     * @return the traversal with an appended {@link TailGlobalStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#tail-step" target="_blank">Reference Documentation - Tail Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> tail() {
        this.asAdmin().getGremlinLang().addStep(Symbols.tail);
        return this.asAdmin().addStep(new TailGlobalStep<>(this.asAdmin(), 1));
    }

    /**
     * Filters the objects in the traversal emitted as being last objects in the stream. In this case, only the last
     * {@code n} objects will be returned as defined by the {@code limit}.
     *
     * @param limit the number at which to end the stream
     * @return the traversal with an appended {@link TailGlobalStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#tail-step" target="_blank">Reference Documentation - Tail Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> tail(final long limit) {
        this.asAdmin().getGremlinLang().addStep(Symbols.tail, limit);
        return this.asAdmin().addStep(new TailGlobalStep<>(this.asAdmin(), limit));
    }

    /**
     * Filters the objects in the traversal emitted as being last objects in the stream given the {@link Scope}. In
     * this case, only the last object in the stream will be returned.
     *
     * @param scope the scope of how to apply the {@code tail}
     * @return the traversal with an appended {@link TailGlobalStep} or {@link TailLocalStep} depending on {@code scope}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#tail-step" target="_blank">Reference Documentation - Tail Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> tail(final Scope scope) {
        this.asAdmin().getGremlinLang().addStep(Symbols.tail, scope);
        return this.asAdmin().addStep(scope.equals(Scope.global)
                ? new TailGlobalStep<>(this.asAdmin(), 1)
                : new TailLocalStep<>(this.asAdmin(), 1));
    }

    /**
     * Filters the objects in the traversal emitted as being last objects in the stream given the {@link Scope}. In
     * this case, only the last {@code n} objects will be returned as defined by the {@code limit}.
     *
     * @param scope the scope of how to apply the {@code tail}
     * @param limit the number at which to end the stream
     * @return the traversal with an appended {@link TailGlobalStep} or {@link TailLocalStep} depending on {@code scope}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#tail-step" target="_blank">Reference Documentation - Tail Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> tail(final Scope scope, final long limit) {
        this.asAdmin().getGremlinLang().addStep(Symbols.tail, scope, limit);
        return this.asAdmin().addStep(scope.equals(Scope.global)
                ? new TailGlobalStep<>(this.asAdmin(), limit)
                : new TailLocalStep<>(this.asAdmin(), limit));
    }

    /**
     * Filters out the first {@code n} objects in the traversal.
     *
     * @param skip the number of objects to skip
     * @return the traversal with an appended {@link RangeGlobalStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#skip-step" target="_blank">Reference Documentation - Skip Step</a>
     * @since 3.3.0
     */
    public default GraphTraversal<S, E> skip(final long skip) {
        this.asAdmin().getGremlinLang().addStep(Symbols.skip, skip);
        return this.asAdmin().addStep(new RangeGlobalStep<>(this.asAdmin(), skip, -1));
    }

    /**
     * Filters out the first {@code n} objects in the traversal.
     *
     * @param scope the scope of how to apply the {@code tail}
     * @param skip  the number of objects to skip
     * @return the traversal with an appended {@link RangeGlobalStep} or {@link RangeLocalStep} depending on {@code scope}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#skip-step" target="_blank">Reference Documentation - Skip Step</a>
     * @since 3.3.0
     */
    public default <E2> GraphTraversal<S, E2> skip(final Scope scope, final long skip) {
        this.asAdmin().getGremlinLang().addStep(Symbols.skip, scope, skip);
        return this.asAdmin().addStep(scope.equals(Scope.global)
                ? new RangeGlobalStep<>(this.asAdmin(), skip, -1)
                : new RangeLocalStep<>(this.asAdmin(), skip, -1));
    }

    /**
     * Once the first {@link Traverser} hits this step, a count down is started. Once the time limit is up, all remaining traversers are filtered out.
     *
     * @param timeLimit the count down time
     * @return the traversal with an appended {@link TimeLimitStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#timelimit-step" target="_blank">Reference Documentation - TimeLimit Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> timeLimit(final long timeLimit) {
        this.asAdmin().getGremlinLang().addStep(Symbols.timeLimit, timeLimit);
        return this.asAdmin().addStep(new TimeLimitStep<E>(this.asAdmin(), timeLimit));
    }

    /**
     * Filter the <code>E</code> object if its {@link Traverser#path} is not {@link Path#isSimple}.
     *
     * @return the traversal with an appended {@link PathFilterStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#simplepath-step" target="_blank">Reference Documentation - SimplePath Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> simplePath() {
        this.asAdmin().getGremlinLang().addStep(Symbols.simplePath);
        return this.asAdmin().addStep(new PathFilterStep<E>(this.asAdmin(), true));
    }

    /**
     * Filter the <code>E</code> object if its {@link Traverser#path} is {@link Path#isSimple}.
     *
     * @return the traversal with an appended {@link PathFilterStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#cyclicpath-step" target="_blank">Reference Documentation - CyclicPath Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> cyclicPath() {
        this.asAdmin().getGremlinLang().addStep(Symbols.cyclicPath);
        return this.asAdmin().addStep(new PathFilterStep<E>(this.asAdmin(), false));
    }

    /**
     * Allow some specified number of objects to pass through the stream.
     *
     * @param amountToSample the number of objects to allow
     * @return the traversal with an appended {@link SampleGlobalStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#sample-step" target="_blank">Reference Documentation - Sample Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> sample(final int amountToSample) {
        this.asAdmin().getGremlinLang().addStep(Symbols.sample, amountToSample);
        return this.asAdmin().addStep(new SampleGlobalStep<>(this.asAdmin(), amountToSample));
    }

    /**
     * Allow some specified number of objects to pass through the stream.
     *
     * @param scope          the scope of how to apply the {@code sample}
     * @param amountToSample the number of objects to allow
     * @return the traversal with an appended {@link SampleGlobalStep} or {@link SampleLocalStep} depending on the {@code scope}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#sample-step" target="_blank">Reference Documentation - Sample Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> sample(final Scope scope, final int amountToSample) {
        this.asAdmin().getGremlinLang().addStep(Symbols.sample, scope, amountToSample);
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
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#drop-step" target="_blank">Reference Documentation - Drop Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> drop() {
        this.asAdmin().getGremlinLang().addStep(Symbols.drop);
        return this.asAdmin().addStep(new DropStep<>(this.asAdmin()));
    }

    /**
     * Filters <code>E</code> lists given the provided {@code predicate}.
     *
     * @param predicate the filter to apply
     * @return the traversal with an appended {@link AllStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#all-step" target="_blank">Reference Documentation - All Step</a>
     * @since 3.7.1
     */
    public default <S2> GraphTraversal<S, E> all(final P<S2> predicate) {
        this.asAdmin().getGremlinLang().addStep(Symbols.all, predicate);
        return this.asAdmin().addStep(new AllStep<>(this.asAdmin(), predicate));
    }

    /**
     * Filters <code>E</code> lists given the provided {@code predicate}.
     *
     * @param predicate the filter to apply
     * @return the traversal with an appended {@link AnyStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#any-step" target="_blank">Reference Documentation - Any Step</a>
     * @since 3.7.1
     */
    public default <S2> GraphTraversal<S, E> any(final P<S2> predicate) {
        this.asAdmin().getGremlinLang().addStep(Symbols.any, predicate);
        return this.asAdmin().addStep(new AnyStep<>(this.asAdmin(), predicate));
    }

    /**
     * Filters <code>E</code> lists given the provided {@code predicate}.
     *
     * @param predicate the filter to apply
     * @return the traversal with an appended {@link NoneStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#none-step" target="_blank">Reference Documentation - None Step</a>
     * @since 4.0.0
     */
    public default <S2> GraphTraversal<S, E> none(final P<S2> predicate) {
        this.asAdmin().getGremlinLang().addStep(Symbols.none, predicate);
        return this.asAdmin().addStep(new NoneStep<>(this.asAdmin(), predicate));
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    /**
     * Perform some operation on the {@link Traverser} and pass it to the next step unmodified.
     *
     * @param consumer the operation to perform at this step in relation to the {@link Traverser}
     * @return the traversal with an appended {@link LambdaSideEffectStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#general-steps" target="_blank">Reference Documentation - General Steps</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> sideEffect(final Consumer<Traverser<E>> consumer) {
        this.asAdmin().getGremlinLang().addStep(Symbols.sideEffect, consumer);
        return this.asAdmin().addStep(new LambdaSideEffectStep<>(this.asAdmin(), consumer));
    }

    /**
     * Perform some operation on the {@link Traverser} and pass it to the next step unmodified.
     *
     * @param sideEffectTraversal the operation to perform at this step in relation to the {@link Traverser}
     * @return the traversal with an appended {@link TraversalSideEffectStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#general-steps" target="_blank">Reference Documentation - General Steps</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> sideEffect(final Traversal<?, ?> sideEffectTraversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.sideEffect, sideEffectTraversal);
        return this.asAdmin().addStep(new TraversalSideEffectStep<>(this.asAdmin(), (Traversal) sideEffectTraversal));
    }

    /**
     * Iterates the traversal up to the itself and emits the side-effect referenced by the key. If multiple keys are
     * supplied then the side-effects are emitted as a {@code Map}.
     *
     * @param sideEffectKey  the side-effect to emit
     * @param sideEffectKeys other side-effects to emit
     * @return the traversal with an appended {@link SideEffectCapStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#cap-step" target="_blank">Reference Documentation - Cap Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> cap(final String sideEffectKey, final String... sideEffectKeys) {
        this.asAdmin().getGremlinLang().addStep(Symbols.cap, sideEffectKey, sideEffectKeys);
        return this.asAdmin().addStep(new SideEffectCapStep<>(this.asAdmin(), sideEffectKey, sideEffectKeys));
    }

    /**
     * Extracts a portion of the graph being traversed into a {@link Graph} object held in the specified side-effect
     * key.
     *
     * @param sideEffectKey the name of the side-effect key that will hold the subgraph
     * @return the traversal with an appended {@link SubgraphStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#subgraph-step" target="_blank">Reference Documentation - Subgraph Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, Edge> subgraph(final String sideEffectKey) {
        this.asAdmin().getGremlinLang().addStep(Symbols.subgraph, sideEffectKey);
        return this.asAdmin().addStep(new SubgraphStep(this.asAdmin(), sideEffectKey));
    }

    /**
     * Eagerly collects objects up to this step into a side-effect. Same as calling {@link #aggregate(Scope, String)}
     * with a {@link Scope#global}.
     *
     * @param sideEffectKey the name of the side-effect key that will hold the aggregated objects
     * @return the traversal with an appended {@link AggregateGlobalStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#aggregate-step" target="_blank">Reference Documentation - Aggregate Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> aggregate(final String sideEffectKey) {
        this.asAdmin().getGremlinLang().addStep(Symbols.aggregate, sideEffectKey);
        return this.asAdmin().addStep(new AggregateGlobalStep<>(this.asAdmin(), sideEffectKey));
    }

    /**
     * Collects objects in a list using the {@link Scope} argument to determine whether it should be lazy
     * {@link Scope#local} or eager ({@link Scope#global} while gathering those objects.
     *
     * @param sideEffectKey the name of the side-effect key that will hold the aggregated objects
     * @return the traversal with an appended {@link AggregateGlobalStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#aggregate-step" target="_blank">Reference Documentation - Aggregate Step</a>
     * @since 3.4.3
     */
    public default GraphTraversal<S, E> aggregate(final Scope scope, final String sideEffectKey) {
        this.asAdmin().getGremlinLang().addStep(Symbols.aggregate, scope, sideEffectKey);
        return this.asAdmin().addStep(scope == Scope.global ?
                new AggregateGlobalStep<>(this.asAdmin(), sideEffectKey) : new AggregateLocalStep<>(this.asAdmin(), sideEffectKey));
    }

    /**
     * Organize objects in the stream into a {@code Map}. Calls to {@code group()} are typically accompanied with
     * {@link #by()} modulators which help specify how the grouping should occur.
     *
     * @param sideEffectKey the name of the side-effect key that will hold the aggregated grouping
     * @return the traversal with an appended {@link GroupStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#group-step" target="_blank">Reference Documentation - Group Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> group(final String sideEffectKey) {
        this.asAdmin().getGremlinLang().addStep(Symbols.group, sideEffectKey);
        return this.asAdmin().addStep(new GroupSideEffectStep<>(this.asAdmin(), sideEffectKey));
    }

    /**
     * Counts the number of times a particular objects has been part of a traversal, returning a {@code Map} where the
     * object is the key and the value is the count.
     *
     * @param sideEffectKey the name of the side-effect key that will hold the aggregated grouping
     * @return the traversal with an appended {@link GroupCountStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#groupcount-step" target="_blank">Reference Documentation - GroupCount Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> groupCount(final String sideEffectKey) {
        this.asAdmin().getGremlinLang().addStep(Symbols.groupCount, sideEffectKey);
        return this.asAdmin().addStep(new GroupCountSideEffectStep<>(this.asAdmin(), sideEffectKey));
    }

    /**
     * When triggered, immediately throws a {@code RuntimeException} which implements the {@link Failure} interface.
     * The traversal will be terminated as a result.
     *
     * @return the traversal with an appended {@link FailStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#fail-step" target="_blank">Reference Documentation - Fail Step</a>
     * @since 3.6.0
     */
    public default GraphTraversal<S, E> fail() {
        this.asAdmin().getGremlinLang().addStep(Symbols.fail);
        return this.asAdmin().addStep(new FailStep<>(this.asAdmin()));
    }

    /**
     * When triggered, immediately throws a {@code RuntimeException} which implements the {@link Failure} interface.
     * The traversal will be terminated as a result.
     *
     * @param message the error message to include in the exception
     * @return the traversal with an appended {@link FailStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#fail-step" target="_blank">Reference Documentation - Fail Step</a>
     * @since 3.6.0
     */
    public default GraphTraversal<S, E> fail(final String message) {
        this.asAdmin().getGremlinLang().addStep(Symbols.fail, message);
        return this.asAdmin().addStep(new FailStep<>(this.asAdmin(), message));
    }

    /**
     * Aggregates the emanating paths into a {@link Tree} data structure.
     *
     * @param sideEffectKey the name of the side-effect key that will hold the tree
     * @return the traversal with an appended {@link TreeStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#tree-step" target="_blank">Reference Documentation - Tree Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> tree(final String sideEffectKey) {
        this.asAdmin().getGremlinLang().addStep(Symbols.tree, sideEffectKey);
        return this.asAdmin().addStep(new TreeSideEffectStep<>(this.asAdmin(), sideEffectKey));
    }

    /**
     * Map the {@link Traverser} to its {@link Traverser#sack} value.
     *
     * @param sackOperator the operator to apply to the sack value
     * @return the traversal with an appended {@link SackStep}.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#sack-step" target="_blank">Reference Documentation - Sack Step</a>
     * @since 3.0.0-incubating
     */
    public default <V, U> GraphTraversal<S, E> sack(final BiFunction<V, U, V> sackOperator) {
        this.asAdmin().getGremlinLang().addStep(Symbols.sack, sackOperator);
        return this.asAdmin().addStep(new SackValueStep<>(this.asAdmin(), sackOperator));
    }

    /**
     * Lazily aggregates objects in the stream into a side-effect collection.
     *
     * @param sideEffectKey the name of the side-effect key that will hold the aggregate
     * @return the traversal with an appended {@link AggregateLocalStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#store-step" target="_blank">Reference Documentation - Store Step</a>
     * @since 3.0.0-incubating
     * @deprecated As of release 3.4.3, replaced by {@link #aggregate(Scope, String)} using {@link Scope#local}.
     */
    @Deprecated
    public default GraphTraversal<S, E> store(final String sideEffectKey) {
        this.asAdmin().getGremlinLang().addStep(Symbols.store, sideEffectKey);
        return this.asAdmin().addStep(new AggregateLocalStep<>(this.asAdmin(), sideEffectKey));
    }

    /**
     * Allows developers to examine statistical information about a traversal providing data like execution times,
     * counts, etc.
     *
     * @param sideEffectKey the name of the side-effect key within which to hold the profile object
     * @return the traversal with an appended {@link ProfileSideEffectStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#profile-step" target="_blank">Reference Documentation - Profile Step</a>
     * @since 3.2.0-incubating
     */
    public default GraphTraversal<S, E> profile(final String sideEffectKey) {
        this.asAdmin().getGremlinLang().addStep(Traversal.Symbols.profile, sideEffectKey);
        return this.asAdmin().addStep(new ProfileSideEffectStep<>(this.asAdmin(), sideEffectKey));
    }

    /**
     * Allows developers to examine statistical information about a traversal providing data like execution times,
     * counts, etc.
     *
     * @return the traversal with an appended {@link ProfileSideEffectStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#profile-step" target="_blank">Reference Documentation - Profile Step</a>
     * @since 3.0.0-incubating
     */
    @Override
    public default GraphTraversal<S, TraversalMetrics> profile() {
        return (GraphTraversal<S, TraversalMetrics>) Traversal.super.profile();
    }

    /**
     * Sets a {@link Property} value and related meta properties if supplied, if supported by the {@link Graph}
     * and if the {@link Element} is a {@link VertexProperty}.  This method is the long-hand version of
     * {@link #property(Object, Object, Object...)} with the difference that the {@link VertexProperty.Cardinality}
     * can be supplied.
     * <p/>* 
     * Generally speaking, this method will append an {@link AddPropertyStep} to the {@link Traversal} but when
     * possible, this method will attempt to fold key/value pairs into an {@link AddVertexStep}, {@link AddEdgeStep} or
     * {@link AddVertexStartStep}.  This potential optimization can only happen if cardinality is not supplied
     * and when meta-properties are not included.
     *
     * @param cardinality the specified cardinality of the property where {@code null} will allow the {@link Graph}
     *                    to use its default settings
     * @param key         the key for the property
     * @param value       the value for the property which may not be null if the {@code key} is of type {@link T}
     * @param keyValues   any meta properties to be assigned to this property
     * @return the traversal with the last step modified to add a property
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#addproperty-step" target="_blank">AddProperty Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> property(final VertexProperty.Cardinality cardinality, final Object key, final Object value, final Object... keyValues) {
        if (key instanceof T && null == value)
            throw new IllegalArgumentException("Value of T cannot be null");

        if (null == cardinality)
            this.asAdmin().getGremlinLang().addStep(Symbols.property, key, value, keyValues);
        else
            this.asAdmin().getGremlinLang().addStep(Symbols.property, cardinality, key, value, keyValues);

        // if it can be detected that this call to property() is related to an addV/E() then we can attempt to fold
        // the properties into that step to gain an optimization for those graphs that support such capabilities.
        Step endStep = this.asAdmin().getEndStep();

        // always try to fold the property() into the initial "AddElementStep" as the performance will be better
        // and as it so happens with T the value must be set by way of that approach otherwise you get an error.
        // it should be safe to execute this loop this way as we'll either hit an "AddElementStep" or an "EmptyStep".
        // if empty, it will just use the regular AddPropertyStep being tacked on to the end of the traversal as usual
        while (endStep instanceof AddPropertyStep) {
            endStep = endStep.getPreviousStep();
        }

        // edge properties can always be folded as there are no cardinality/metaproperties. of course, if the
        // cardinality is specified as something other than single or null it would be confusing to simply allow it to
        // execute and not throw an error.
        if ((endStep instanceof AddEdgeStep || endStep instanceof AddEdgeStartStep) && (null != cardinality && cardinality != single))
            throw new IllegalStateException(String.format(
                    "Multi-property cardinality of [%s] can only be set for a Vertex but is being used for addE() with key: %s",
                    cardinality.name(), key));

        // for a vertex mutation, it's possible to fold the property() into the Mutating step if there are no
        // metaproperties (i.e. keyValues) and if (1) the key is an instance of T OR OR (3) the key is a string and the
        // cardinality is not specified. Note that checking for single cardinality of the argument doesn't work well
        // because once folded we lose the cardinality argument associated to the key/value pair and then it relies on
        // the graph. that means that if you do:
        //
        // g.addV().property(single, 'k',1).property(single,'k',2)
        //
        // you could end up with whatever the cardinality is for the key which might seem "wrong" if you were explicit
        // about the specification of "single". it also isn't possible to check the Graph Features for cardinality
        // as folding seems to have different behavior based on different graphs - we clearly don't have that aspect
        // of things tested/enforced well.
        //
        // of additional note is the folding that occurs if the key is a Traversal. the key here is technically
        // unknown until traversal execution as the anonymous traversal result isn't evaluated during traversal
        // construction but during iteration. not folding to AddVertexStep creates different (breaking) traversal
        // semantics than we've had in previous versions so right/wrong could be argued, but since it's a breaking
        // change we'll just arbitrarily account for it to maintain the former behavior.
        if ((endStep instanceof AddEdgeStep || endStep instanceof AddEdgeStartStep) ||
                ((endStep instanceof AddVertexStep || endStep instanceof AddVertexStartStep) &&
                  keyValues.length == 0 &&
                  (key instanceof T || (key instanceof String && null == cardinality) || key instanceof Traversal))) {
            ((Mutating) endStep).configure(key, value);
        } else {
            final AddPropertyStep<Element> addPropertyStep = new AddPropertyStep<>(this.asAdmin(), cardinality, key, value);
            this.asAdmin().addStep(addPropertyStep);
            addPropertyStep.configure(keyValues);
        }
        return this;
    }

    /**
     * Sets the key and value of a {@link Property}. If the {@link Element} is a {@link VertexProperty} and the
     * {@link Graph} supports it, meta properties can be set.  Use of this method assumes that the
     * {@link VertexProperty.Cardinality} is defaulted to {@code null} which  means that the default cardinality for
     * the {@link Graph} will be used.
     * <p/>
     * If a {@link Map} is supplied then each of the key/value pairs in the map will
     * be added as property.  This method is the long-hand version of looping through the 
     * {@link #property(Object, Object, Object...)} method for each key/value pair supplied.
     * If a {@link CardinalityValueTraversal} is specified as a value then it will override any
     * {@link VertexProperty.Cardinality} specified for the {@code key}.
     * <p />
     * This method is effectively calls {@link #property(VertexProperty.Cardinality, Object, Object, Object...)}
     * as {@code property(null, key, value, keyValues}.
     *
     * @param key       the key for the property
     * @param value     the value for the property
     * @param keyValues any meta properties to be assigned to this property
     * @return the traversal with the last step modified to add a property
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#addproperty-step" target="_blank">AddProperty Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> property(final Object key, final Object value, final Object... keyValues) {
        if (key instanceof VertexProperty.Cardinality) {
            // expect property(Cardinality, Map) where Map has entry type of either:
            // + <String<k>,CardinalityValue<v>> = property(v.cardinality, k, v.value) - overrides the Cardinality provided by "key"
            // + <String<k>,Object<v>> = property(key, k, v) - uses Cardinality of key
            if (value instanceof Map) {
                // Handle the property(Cardinality, Map) signature
                final Map<Object, Object> map = (Map) value;
                for (Map.Entry<Object, Object> entry : map.entrySet()) {
                    final Object val = entry.getValue();
                    if (val instanceof CardinalityValueTraversal) {
                        final CardinalityValueTraversal cardVal = (CardinalityValueTraversal) val;
                        property(cardVal.getCardinality(), entry.getKey(), cardVal.getValue());
                    } else {
                        // explicitly cast to avoid a possible recursive call.
                        property((VertexProperty.Cardinality) key, entry.getKey(), entry.getValue());
                    }
                }
                return this;
            } else if (value == null) {
                // Just return the input if you pass a null
                return this;
            } else {
                return this.property((VertexProperty.Cardinality) key, value, null == keyValues ? null : keyValues[0],
                        keyValues != null && keyValues.length > 1 ?
                                Arrays.copyOfRange(keyValues, 1, keyValues.length) :
                                new Object[]{});
            }
        } else  {
            // handles if cardinality is not the first parameter
            return this.property(null, key, value, keyValues);
        }
    }

    /**
     * When a {@link Map} is supplied then each of the key/value pairs in the map will
     * be added as property.  This method is the long-hand version of looping through the 
     * {@link #property(Object, Object, Object...)} method for each key/value pair supplied.
     * <p/>
     * A value may use a {@link CardinalityValueTraversal} to allow specification of the
     * {@link VertexProperty.Cardinality} along with the property value itself.
     * <p/>
     * If a {@link Map} is not supplied then an exception is thrown.
     * <p />
     * This method is effectively calls {@link #property(VertexProperty.Cardinality, Object, Object, Object...)}
     * as {@code property(null, key, value, keyValues}.
     *
     * @param value     the value for the property
     * @return the traversal with the last step modified to add a property
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#addproperty-step" target="_blank">AddProperty Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> property(final Map<Object, Object> value) {
        if (value != null) {
            for (Map.Entry<Object, Object> entry : value.entrySet()) {
                final Object val = entry.getValue();
                if (val instanceof CardinalityValueTraversal) {
                    final CardinalityValueTraversal cardVal = (CardinalityValueTraversal) val;
                    property(cardVal.getCardinality(), entry.getKey(), cardVal.getValue());
                } else {
                    property(null, entry.getKey(), entry.getValue());
                }
            }
        }
        return this;
    }
    ///////////////////// BRANCH STEPS /////////////////////

    /**
     * Split the {@link Traverser} to all the specified traversals.
     *
     * @param branchTraversal the traversal to branch the {@link Traverser} to
     * @return the {@link Traversal} with the {@link BranchStep} added
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#general-steps" target="_blank">Reference Documentation - General Steps</a>
     * @since 3.0.0-incubating
     */
    public default <M, E2> GraphTraversal<S, E2> branch(final Traversal<?, M> branchTraversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.branch, branchTraversal);
        final BranchStep<E, E2, M> branchStep = new BranchStep<>(this.asAdmin());
        branchStep.setBranchTraversal((Traversal.Admin<E, M>) branchTraversal);
        return this.asAdmin().addStep(branchStep);
    }

    /**
     * Split the {@link Traverser} to all the specified functions.
     *
     * @param function the traversal to branch the {@link Traverser} to
     * @return the {@link Traversal} with the {@link BranchStep} added
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#general-steps" target="_blank">Reference Documentation - General Steps</a>
     * @since 3.0.0-incubating
     */
    public default <M, E2> GraphTraversal<S, E2> branch(final Function<Traverser<E>, M> function) {
        this.asAdmin().getGremlinLang().addStep(Symbols.branch, function);
        final BranchStep<E, E2, M> branchStep = new BranchStep<>(this.asAdmin());
        branchStep.setBranchTraversal((Traversal.Admin<E, M>) __.map(function));
        return this.asAdmin().addStep(branchStep);
    }

    /**
     * Routes the current traverser to a particular traversal branch option which allows the creation of if-then-else
     * like semantics within a traversal. A {@code choose} is modified by {@link #option} which provides the various
     * branch choices.
     *
     * @param choiceTraversal the traversal used to determine the value for the branch
     * @return the traversal with the appended {@link ChooseStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#choose-step" target="_blank">Reference Documentation - Choose Step</a>
     * @since 3.0.0-incubating
     */
    public default <M, E2> GraphTraversal<S, E2> choose(final Traversal<?, M> choiceTraversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.choose, choiceTraversal);
        return this.asAdmin().addStep(new ChooseStep<>(this.asAdmin(), (Traversal.Admin<E, M>) choiceTraversal));
    }

    /**
     * Routes the current traverser to a particular traversal branch option which allows the creation of if-then-else
     * like semantics within a traversal.
     *
     * @param traversalPredicate the traversal used to determine the "if" portion of the if-then-else
     * @param trueChoice         the traversal to execute in the event the {@code traversalPredicate} returns true
     * @param falseChoice        the traversal to execute in the event the {@code traversalPredicate} returns false
     * @return the traversal with the appended {@link ChooseStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#choose-step" target="_blank">Reference Documentation - Choose Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> choose(final Traversal<?, ?> traversalPredicate,
                                                     final Traversal<?, E2> trueChoice, final Traversal<?, E2> falseChoice) {
        this.asAdmin().getGremlinLang().addStep(Symbols.choose, traversalPredicate, trueChoice, falseChoice);
        return this.asAdmin().addStep(new ChooseStep<E, E2, Boolean>(this.asAdmin(), (Traversal.Admin<E, ?>) traversalPredicate, (Traversal.Admin<E, E2>) trueChoice, (Traversal.Admin<E, E2>) falseChoice));
    }

    /**
     * Routes the current traverser to a particular traversal branch option which allows the creation of if-then
     * like semantics within a traversal.
     *
     * @param traversalPredicate the traversal used to determine the "if" portion of the if-then-else
     * @param trueChoice         the traversal to execute in the event the {@code traversalPredicate} returns true
     * @return the traversal with the appended {@link ChooseStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#choose-step" target="_blank">Reference Documentation - Choose Step</a>
     * @since 3.2.4
     */
    public default <E2> GraphTraversal<S, E2> choose(final Traversal<?, ?> traversalPredicate,
                                                     final Traversal<?, E2> trueChoice) {
        this.asAdmin().getGremlinLang().addStep(Symbols.choose, traversalPredicate, trueChoice);
        return this.asAdmin().addStep(new ChooseStep<E, E2, Boolean>(this.asAdmin(), (Traversal.Admin<E, ?>) traversalPredicate, (Traversal.Admin<E, E2>) trueChoice, (Traversal.Admin<E, E2>) __.identity()));
    }

    /**
     * Routes the current traverser to a particular traversal branch option which allows the creation of if-then-else
     * like semantics within a traversal. A {@code choose} is modified by {@link #option} which provides the various
     * branch choices.
     *
     * @param choiceFunction the function used to determine the value for the branch
     * @return the traversal with the appended {@link ChooseStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#choose-step" target="_blank">Reference Documentation - Choose Step</a>
     * @since 3.0.0-incubating
     */
    public default <M, E2> GraphTraversal<S, E2> choose(final Function<E, M> choiceFunction) {
        this.asAdmin().getGremlinLang().addStep(Symbols.choose, choiceFunction);
        return this.asAdmin().addStep(new ChooseStep<>(this.asAdmin(), (Traversal.Admin<E, M>) __.map(new FunctionTraverser<>(choiceFunction))));
    }

    /**
     * Routes the current traverser to a particular traversal branch option which allows the creation of if-then-else
     * like semantics within a traversal.
     *
     * @param choosePredicate the function used to determine the "if" portion of the if-then-else
     * @param trueChoice      the traversal to execute in the event the {@code traversalPredicate} returns true
     * @param falseChoice     the traversal to execute in the event the {@code traversalPredicate} returns false
     * @return the traversal with the appended {@link ChooseStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#choose-step" target="_blank">Reference Documentation - Choose Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> choose(final Predicate<E> choosePredicate,
                                                     final Traversal<?, E2> trueChoice, final Traversal<?, E2> falseChoice) {
        this.asAdmin().getGremlinLang().addStep(Symbols.choose, choosePredicate, trueChoice, falseChoice);
        return this.asAdmin().addStep(new ChooseStep<E, E2, Boolean>(this.asAdmin(), (Traversal.Admin<E, ?>) __.filter(new PredicateTraverser<>(choosePredicate)), (Traversal.Admin<E, E2>) trueChoice, (Traversal.Admin<E, E2>) falseChoice));
    }

    /**
     * Routes the current traverser to a particular traversal branch option which allows the creation of if-then
     * like semantics within a traversal.
     *
     * @param choosePredicate the function used to determine the "if" portion of the if-then-else
     * @param trueChoice      the traversal to execute in the event the {@code traversalPredicate} returns true
     * @return the traversal with the appended {@link ChooseStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#choose-step" target="_blank">Reference Documentation - Choose Step</a>
     * @since 3.2.4
     */
    public default <E2> GraphTraversal<S, E2> choose(final Predicate<E> choosePredicate,
                                                     final Traversal<?, E2> trueChoice) {
        this.asAdmin().getGremlinLang().addStep(Symbols.choose, choosePredicate, trueChoice);
        return this.asAdmin().addStep(new ChooseStep<E, E2, Boolean>(this.asAdmin(), (Traversal.Admin<E, ?>) __.filter(new PredicateTraverser<>(choosePredicate)), (Traversal.Admin<E, E2>) trueChoice, (Traversal.Admin<E, E2>) __.identity()));
    }

    /**
     * Returns the result of the specified traversal if it yields a result, otherwise it returns the calling element.
     *
     * @param optionalTraversal the traversal to execute for a potential result
     * @return the traversal with the appended {@link ChooseStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#optional-step" target="_blank">Reference Documentation - Optional Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> optional(final Traversal<?, E2> optionalTraversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.optional, optionalTraversal);
        return this.asAdmin().addStep(new OptionalStep<>(this.asAdmin(), (Traversal.Admin<E2, E2>) optionalTraversal));
    }

    /**
     * Merges the results of an arbitrary number of traversals.
     *
     * @param unionTraversals the traversals to merge
     * @return the traversal with the appended {@link UnionStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#union-step" target="_blank">Reference Documentation - Union Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> union(final Traversal<?, E2>... unionTraversals) {
        this.asAdmin().getGremlinLang().addStep(Symbols.union, unionTraversals);
        return this.asAdmin().addStep(new UnionStep<>(this.asAdmin(), Arrays.copyOf(unionTraversals, unionTraversals.length, Traversal.Admin[].class)));
    }

    /**
     * Evaluates the provided traversals and returns the result of the first traversal to emit at least one object.
     *
     * @param coalesceTraversals the traversals to coalesce
     * @return the traversal with the appended {@link CoalesceStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#coalesce-step" target="_blank">Reference Documentation - Coalesce Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> coalesce(final Traversal<?, E2>... coalesceTraversals) {
        this.asAdmin().getGremlinLang().addStep(Symbols.coalesce, coalesceTraversals);
        return this.asAdmin().addStep(new CoalesceStep<>(this.asAdmin(), Arrays.copyOf(coalesceTraversals, coalesceTraversals.length, Traversal.Admin[].class)));
    }

    /**
     * This step is used for looping over a traversal given some break predicate.
     *
     * @param repeatTraversal the traversal to repeat over
     * @return the traversal with the appended {@link RepeatStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#repeat-step" target="_blank">Reference Documentation - Repeat Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> repeat(final Traversal<?, E> repeatTraversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.repeat, repeatTraversal);
        return RepeatStep.addRepeatToTraversal(this, (Traversal.Admin<E, E>) repeatTraversal);
    }

    /**
     * This step is used for looping over a traversal given some break predicate and with a specified loop name.
     *
     * @param repeatTraversal the traversal to repeat over
     * @param loopName The name given to the loop
     * @return the traversal with the appended {@link RepeatStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#repeat-step" target="_blank">Reference Documentation - Repeat Step</a>
     * @since 3.4.0
     */
    public default GraphTraversal<S, E> repeat(final String loopName, final Traversal<?, E> repeatTraversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.repeat, loopName, repeatTraversal);
        return RepeatStep.addRepeatToTraversal(this, loopName, (Traversal.Admin<E, E>) repeatTraversal);
    }


    /**
     * Emit is used in conjunction with {@link #repeat(Traversal)} to determine what objects get emit from the loop.
     *
     * @param emitTraversal the emit predicate defined as a traversal
     * @return the traversal with the appended {@link RepeatStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#repeat-step" target="_blank">Reference Documentation - Repeat Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> emit(final Traversal<?, ?> emitTraversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.emit, emitTraversal);
        return RepeatStep.addEmitToTraversal(this, (Traversal.Admin<E, ?>) emitTraversal);
    }

    /**
     * Emit is used in conjunction with {@link #repeat(Traversal)} to determine what objects get emit from the loop.
     *
     * @param emitPredicate the emit predicate
     * @return the traversal with the appended {@link RepeatStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#repeat-step" target="_blank">Reference Documentation - Repeat Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> emit(final Predicate<Traverser<E>> emitPredicate) {
        this.asAdmin().getGremlinLang().addStep(Symbols.emit, emitPredicate);
        return RepeatStep.addEmitToTraversal(this, (Traversal.Admin<E, ?>) __.filter(emitPredicate));
    }

    /**
     * Emit is used in conjunction with {@link #repeat(Traversal)} to emit all objects from the loop.
     *
     * @return the traversal with the appended {@link RepeatStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#repeat-step" target="_blank">Reference Documentation - Repeat Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> emit() {
        this.asAdmin().getGremlinLang().addStep(Symbols.emit);
        return RepeatStep.addEmitToTraversal(this, TrueTraversal.instance());
    }

    /**
     * Modifies a {@link #repeat(Traversal)} to determine when the loop should exit.
     *
     * @param untilTraversal the traversal that determines when the loop exits
     * @return the traversal with the appended {@link RepeatStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#repeat-step" target="_blank">Reference Documentation - Repeat Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> until(final Traversal<?, ?> untilTraversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.until, untilTraversal);
        return RepeatStep.addUntilToTraversal(this, (Traversal.Admin<E, ?>) untilTraversal);
    }

    /**
     * Modifies a {@link #repeat(Traversal)} to determine when the loop should exit.
     *
     * @param untilPredicate the predicate that determines when the loop exits
     * @return the traversal with the appended {@link RepeatStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#repeat-step" target="_blank">Reference Documentation - Repeat Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> until(final Predicate<Traverser<E>> untilPredicate) {
        this.asAdmin().getGremlinLang().addStep(Symbols.until, untilPredicate);
        return RepeatStep.addUntilToTraversal(this, (Traversal.Admin<E, ?>) __.filter(untilPredicate));
    }

    /**
     * Modifies a {@link #repeat(Traversal)} to specify how many loops should occur before exiting.
     *
     * @param maxLoops the number of loops to execute prior to exiting
     * @return the traversal with the appended {@link RepeatStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#repeat-step" target="_blank">Reference Documentation - Repeat Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> times(final int maxLoops) {
        this.asAdmin().getGremlinLang().addStep(Symbols.times, maxLoops);
        if (this.asAdmin().getEndStep() instanceof TimesModulating) {
            ((TimesModulating) this.asAdmin().getEndStep()).modulateTimes(maxLoops);
            return this;
        } else
            return RepeatStep.addUntilToTraversal(this, new LoopTraversal<>(maxLoops));
    }

    /**
     * Provides a execute a specified traversal on a single element within a stream.
     *
     * @param localTraversal the traversal to execute locally
     * @return the traversal with the appended {@link LocalStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#local-step" target="_blank">Reference Documentation - Local Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E2> local(final Traversal<?, E2> localTraversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.local, localTraversal);
        return this.asAdmin().addStep(new LocalStep<>(this.asAdmin(), localTraversal.asAdmin()));
    }

    /////////////////// VERTEX PROGRAM STEPS ////////////////

    /**
     * Calculates a PageRank over the graph using a 0.85 for the {@code alpha} value.
     *
     * @return the traversal with the appended {@link PageRankVertexProgramStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#pagerank-step" target="_blank">Reference Documentation - PageRank Step</a>
     * @since 3.2.0-incubating
     */
    public default GraphTraversal<S, E> pageRank() {
        this.asAdmin().getGremlinLang().addStep(Symbols.pageRank);
        return this.asAdmin().addStep((Step<E, E>) new PageRankVertexProgramStep(this.asAdmin(), 0.85d));
    }

    /**
     * Calculates a PageRank over the graph.
     *
     * @return the traversal with the appended {@link PageRankVertexProgramStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#pagerank-step" target="_blank">Reference Documentation - PageRank Step</a>
     * @since 3.2.0-incubating
     */
    public default GraphTraversal<S, E> pageRank(final double alpha) {
        this.asAdmin().getGremlinLang().addStep(Symbols.pageRank, alpha);
        return this.asAdmin().addStep((Step<E, E>) new PageRankVertexProgramStep(this.asAdmin(), alpha));
    }

    /**
     * Executes a Peer Pressure community detection algorithm over the graph.
     *
     * @return the traversal with the appended {@link PeerPressureVertexProgramStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#peerpressure-step" target="_blank">Reference Documentation - PeerPressure Step</a>
     * @since 3.2.0-incubating
     */
    public default GraphTraversal<S, E> peerPressure() {
        this.asAdmin().getGremlinLang().addStep(Symbols.peerPressure);
        return this.asAdmin().addStep((Step<E, E>) new PeerPressureVertexProgramStep(this.asAdmin()));
    }

    /**
     * Executes a Connected Component algorithm over the graph.
     *
     * @return the traversal with the appended {@link ConnectedComponentVertexProgram}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#connectedcomponent-step" target="_blank">Reference Documentation - ConnectedComponent Step</a>
     * @since 3.4.0
     */
    public default GraphTraversal<S, E> connectedComponent() {
        this.asAdmin().getGremlinLang().addStep(Symbols.connectedComponent);
        return this.asAdmin().addStep((Step<E, E>) new ConnectedComponentVertexProgramStep(this.asAdmin()));
    }


    /**
     * Executes a Shortest Path algorithm over the graph.
     *
     * @return the traversal with the appended {@link ShortestPathVertexProgramStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#shortestpath-step" target="_blank">Reference Documentation - ShortestPath Step</a>
     */
    public default GraphTraversal<S, Path> shortestPath() {
        if (this.asAdmin().getEndStep() instanceof GraphStep) {
            // This is very unfortunate, but I couldn't find another way to make it work. Without the additional
            // IdentityStep, TraversalVertexProgram doesn't handle halted traversers as expected (it ignores both:
            // HALTED_TRAVERSER stored in memory and stored as vertex properties); instead it just emits all vertices.
            this.identity();
        }
        this.asAdmin().getGremlinLang().addStep(Symbols.shortestPath);
        return (GraphTraversal<S, Path>) ((Traversal.Admin) this.asAdmin())
                .addStep(new ShortestPathVertexProgramStep(this.asAdmin()));
    }

    /**
     * Executes an arbitrary {@link VertexProgram} over the graph.
     *
     * @return the traversal with the appended {@link ProgramVertexProgramStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#program-step" target="_blank">Reference Documentation - Program Step</a>
     * @since 3.2.0-incubating
     */
    public default GraphTraversal<S, E> program(final VertexProgram<?> vertexProgram) {
        return this.asAdmin().addStep((Step<E, E>) new ProgramVertexProgramStep(this.asAdmin(), vertexProgram));
    }

    ///////////////////// UTILITY STEPS /////////////////////

    /**
     * A step modulator that provides a label to the step that can be accessed later in the traversal by other steps.
     *
     * @param stepLabel  the name of the step
     * @param stepLabels additional names for the label
     * @return the traversal with the modified end step
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#as-step" target="_blank">Reference Documentation - As Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> as(final String stepLabel, final String... stepLabels) {
        this.asAdmin().getGremlinLang().addStep(Symbols.as, stepLabel, stepLabels);
        if (this.asAdmin().getSteps().size() == 0) this.asAdmin().addStep(new StartStep<>(this.asAdmin()));
        final Step<?, E> endStep = this.asAdmin().getEndStep();
        endStep.addLabel(stepLabel);
        for (final String label : stepLabels) {
            endStep.addLabel(label);
        }
        return this;
    }

    /**
     * Turns the lazy traversal pipeline into a bulk-synchronous pipeline which basically iterates that traversal to
     * the size of the barrier. In this case, it iterates the entire thing as the default barrier size is set to
     * {@code Integer.MAX_VALUE}.
     *
     * @return the traversal with an appended {@link NoOpBarrierStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#barrier-step" target="_blank">Reference Documentation - Barrier Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> barrier() {
        this.asAdmin().getGremlinLang().addStep(Symbols.barrier);
        return this.asAdmin().addStep(new NoOpBarrierStep<>(this.asAdmin(), Integer.MAX_VALUE));
    }

    /**
     * Turns the lazy traversal pipeline into a bulk-synchronous pipeline which basically iterates that traversal to
     * the size of the barrier.
     *
     * @param maxBarrierSize the size of the barrier
     * @return the traversal with an appended {@link NoOpBarrierStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#barrier-step" target="_blank">Reference Documentation - Barrier Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> barrier(final int maxBarrierSize) {
        this.asAdmin().getGremlinLang().addStep(Symbols.barrier, maxBarrierSize);
        return this.asAdmin().addStep(new NoOpBarrierStep<>(this.asAdmin(), maxBarrierSize));
    }

    /**
     * Indexes all items of the current collection. The indexing format can be configured using the {@link GraphTraversal#with(String, Object)}
     * and {@link org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions#indexer}.
     *
     * Indexed as list: ["a","b","c"] =&gt; [["a",0],["b",1],["c",2]]
     * Indexed as map:  ["a","b","c"] =&gt; {0:"a",1:"b",2:"c"}
     *
     * If the current object is not a collection, this step will map the object to a single item collection/map:
     *
     * Indexed as list: "a" =&gt; ["a",0]
     * Indexed as map:  "a" =&gt; {0:"a"}
     *
     * @return the traversal with an appended {@link IndexStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#index-step" target="_blank">Reference Documentation - Index Step</a>
     * @since 3.4.0
     */
    public default <E2> GraphTraversal<S, E2> index() {
        this.asAdmin().getGremlinLang().addStep(Symbols.index);
        return this.asAdmin().addStep(new IndexStep<>(this.asAdmin()));
    }

    /**
     * Turns the lazy traversal pipeline into a bulk-synchronous pipeline which basically iterates that traversal to
     * the size of the barrier. In this case, it iterates the entire thing as the default barrier size is set to
     * {@code Integer.MAX_VALUE}.
     *
     * @param barrierConsumer a consumer function that is applied to the objects aggregated to the barrier
     * @return the traversal with an appended {@link NoOpBarrierStep}
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#barrier-step" target="_blank">Reference Documentation - Barrier Step</a>
     * @since 3.2.0-incubating
     */
    public default GraphTraversal<S, E> barrier(final Consumer<TraverserSet<Object>> barrierConsumer) {
        this.asAdmin().getGremlinLang().addStep(Symbols.barrier, barrierConsumer);
        return this.asAdmin().addStep(new LambdaCollectingBarrierStep<>(this.asAdmin(), (Consumer) barrierConsumer, Integer.MAX_VALUE));
    }

    //// WITH-MODULATOR

    /**
     * Provides a configuration to a step in the form of a key which is the same as {@code with(key, true)}. The key
     * of the configuration must be step specific and therefore a configuration could be supplied that is not known to
     * be valid until execution.
     *
     * @param key the key of the configuration to apply to a step
     * @return the traversal with a modulated step
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#with-step" target="_blank">Reference Documentation - With Step</a>
     * @since 3.4.0
     */
    public default GraphTraversal<S,E> with(final String key) {
        this.asAdmin().getGremlinLang().addStep(Symbols.with, key);
        final Object[] configPair = { key, true };
        ((Configuring) this.asAdmin().getEndStep()).configure(configPair);
        return this;
    }

    /**
     * Provides a configuration to a step in the form of a key and value pair. The key of the configuration must be
     * step specific and therefore a configuration could be supplied that is not known to be valid until execution.
     *
     * @param key the key of the configuration to apply to a step
     * @param value the value of the configuration to apply to a step
     * @return the traversal with a modulated step
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#with-step" target="_blank">Reference Documentation - With Step</a>
     * @since 3.4.0
     */
    public default GraphTraversal<S,E> with(final String key, final Object value) {
        this.asAdmin().getGremlinLang().addStep(Symbols.with, key, value);
        final Object[] configPair = { key, value };
        ((Configuring) this.asAdmin().getEndStep()).configure(configPair);
        return this;
    }

    //// BY-MODULATORS

    /**
     * The {@code by()} can be applied to a number of different step to alter their behaviors. This form is essentially
     * an {@link #identity()} modulation.
     *
     * @return the traversal with a modulated step.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#by-step" target="_blank">Reference Documentation - By Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> by() {
        this.asAdmin().getGremlinLang().addStep(Symbols.by);
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy();
        return this;
    }

    /**
     * The {@code by()} can be applied to a number of different step to alter their behaviors. Modifies the previous
     * step with the specified traversal.
     *
     * @param traversal the traversal to apply
     * @return the traversal with a modulated step.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#by-step" target="_blank">Reference Documentation - By Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> by(final Traversal<?, ?> traversal) {
        this.asAdmin().getGremlinLang().addStep(Symbols.by, traversal);
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy(traversal.asAdmin());
        return this;
    }

    /**
     * The {@code by()} can be applied to a number of different step to alter their behaviors. Modifies the previous
     * step with the specified token of {@link T}.
     *
     * @param token the token to apply
     * @return the traversal with a modulated step.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#by-step" target="_blank">Reference Documentation - By Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> by(final T token) {
        this.asAdmin().getGremlinLang().addStep(Symbols.by, token);
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy(token);
        return this;
    }

    /**
     * The {@code by()} can be applied to a number of different step to alter their behaviors. Modifies the previous
     * step with the specified key.
     *
     * @param key the key to apply
     * @return the traversal with a modulated step.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#by-step" target="_blank">Reference Documentation - By Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> by(final String key) {
        this.asAdmin().getGremlinLang().addStep(Symbols.by, key);
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy(key);
        return this;
    }

    /**
     * The {@code by()} can be applied to a number of different step to alter their behaviors. Modifies the previous
     * step with the specified function.
     *
     * @param function the function to apply
     * @return the traversal with a modulated step.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#by-step" target="_blank">Reference Documentation - By Step</a>
     * @since 3.0.0-incubating
     */
    public default <V> GraphTraversal<S, E> by(final Function<V, Object> function) {
        this.asAdmin().getGremlinLang().addStep(Symbols.by, function);
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy(function);
        return this;
    }

    //// COMPARATOR BY-MODULATORS

    /**
     * The {@code by()} can be applied to a number of different step to alter their behaviors. Modifies the previous
     * step with the specified function.
     *
     * @param traversal  the traversal to apply
     * @param comparator the comparator to apply typically for some {@link #order()}
     * @return the traversal with a modulated step.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#by-step" target="_blank">Reference Documentation - By Step</a>
     * @since 3.0.0-incubating
     */
    public default <V> GraphTraversal<S, E> by(final Traversal<?, ?> traversal, final Comparator<V> comparator) {
        this.asAdmin().getGremlinLang().addStep(Symbols.by, traversal, comparator);
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy(traversal.asAdmin(), comparator);
        return this;
    }

    /**
     * The {@code by()} can be applied to a number of different step to alter their behaviors. Modifies the previous
     * step with the specified function.
     *
     * @param comparator the comparator to apply typically for some {@link #order()}
     * @return the traversal with a modulated step.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#by-step" target="_blank">Reference Documentation - By Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> by(final Comparator<E> comparator) {
        this.asAdmin().getGremlinLang().addStep(Symbols.by, comparator);
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy(comparator);
        return this;
    }

    /**
     * The {@code by()} can be applied to a number of different step to alter their behaviors. Modifies the previous
     * step with the specified function.
     *
     * @param order the comparator to apply typically for some {@link #order()}
     * @return the traversal with a modulated step.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#by-step" target="_blank">Reference Documentation - By Step</a>
     * @since 3.0.0-incubating
     */
    public default GraphTraversal<S, E> by(final Order order) {
        this.asAdmin().getGremlinLang().addStep(Symbols.by, order);
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy(order);
        return this;
    }

    /**
     * The {@code by()} can be applied to a number of different step to alter their behaviors. Modifies the previous
     * step with the specified function.
     *
     * @param key        the key to apply                                                                                                     traversal
     * @param comparator the comparator to apply typically for some {@link #order()}
     * @return the traversal with a modulated step.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#by-step" target="_blank">Reference Documentation - By Step</a>
     * @since 3.0.0-incubating
     */
    public default <V> GraphTraversal<S, E> by(final String key, final Comparator<V> comparator) {
        this.asAdmin().getGremlinLang().addStep(Symbols.by, key, comparator);
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy(key, comparator);
        return this;
    }

    /**
     * The {@code by()} can be applied to a number of different step to alter their behaviors. Modifies the previous
     * step with the specified function.
     *
     * @param function   the function to apply
     * @param comparator the comparator to apply typically for some {@link #order()}
     * @return the traversal with a modulated step.
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#by-step" target="_blank">Reference Documentation - By Step</a>
     * @since 3.0.0-incubating
     */
    public default <U> GraphTraversal<S, E> by(final Function<U, Object> function, final Comparator comparator) {
        this.asAdmin().getGremlinLang().addStep(Symbols.by, function, comparator);
        ((ByModulating) this.asAdmin().getEndStep()).modulateBy(function, comparator);
        return this;
    }

    ////

    /**
     * This is a step modulator to a {@link TraversalOptionParent} like {@code choose()} or {@code mergeV()} where the
     * provided argument associated to the {@code token} is applied according to the semantics of the step. Please see
     * the documentation of such steps to understand the usage context.
     *
     * @param token       the token that would trigger this option which may be a {@link Pick}, {@link Merge},
     *                    a {@link Traversal}, {@link Predicate}, or object depending on the step being modulated.
     * @param traversalOption the option as a traversal
     * @return the traversal with the modulated step
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#choose-step" target="_blank">Reference Documentation - Choose Step</a>
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#mergev-step" target="_blank">Reference Documentation - MergeV Step</a>
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#mergee-step" target="_blank">Reference Documentation - MergeE Step</a>
     * @since 3.0.0-incubating
     */
    public default <M, E2> GraphTraversal<S, E> option(final M token, final Traversal<?, E2> traversalOption) {
        this.asAdmin().getGremlinLang().addStep(Symbols.option, token, traversalOption);

        // handle null similar to how option() with Map handles it, otherwise we get a NPE if this one gets used
        final Traversal.Admin<E,E2> t = null == traversalOption ?
                new ConstantTraversal<>(null) : (Traversal.Admin<E, E2>) traversalOption.asAdmin();
        ((TraversalOptionParent<M, E, E2>) this.asAdmin().getEndStep()).addChildOption(token, t);
        return this;
    }

    /**
     * This is a step modulator to a {@link TraversalOptionParent} like {@code choose()} or {@code mergeV()} where the
     * provided argument associated to the {@code token} is applied according to the semantics of the step. Please see
     * the documentation of such steps to understand the usage context.
     *
     * @param m Provides a {@code Map} as the option which is the same as doing {@code constant(m)}.
     * @return the traversal with the modulated step
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#mergev-step" target="_blank">Reference Documentation - MergeV Step</a>
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#mergee-step" target="_blank">Reference Documentation - MergeE Step</a>
     * @since 3.6.0
     */
    public default <M, E2> GraphTraversal<S, E> option(final M token, final Map<Object, Object> m) {
        final Step<?, ?> lastStep = this.asAdmin().getEndStep();

        // CardinalityValueTraversal doesn't make sense for any prior step other than mergeV()
        if (!(lastStep instanceof MergeVertexStep) && m != null) {
            for (Object k : m.keySet()) {
                final Object o = m.get(k);
                if (o instanceof CardinalityValueTraversal)
                    throw new IllegalStateException("option() with the Cardinality argument can only be used following mergeV()");
            }
        }

        this.asAdmin().getGremlinLang().addStep(Symbols.option, token, m);
        ((TraversalOptionParent<M, E, E2>) this.asAdmin().getEndStep()).addChildOption(token, (Traversal.Admin<E, E2>) new ConstantTraversal<>(m).asAdmin());
        return this;
    }

    /**
     * This is a step modulator to {@link #mergeV()} where the provided argument associated to the {@code token} is
     * applied according to the semantics of the step. Please see the documentation of such steps to understand the
     * usage context.
     *
     * @param m Provides a {@code Map} as the option which is the same as doing {@code constant(m)}.
     * @return the traversal with the modulated step
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#mergev-step" target="_blank">Reference Documentation - MergeV Step</a>
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#mergee-step" target="_blank">Reference Documentation - MergeE Step</a>
     * @since 3.7.0
     */
    public default <M, E2> GraphTraversal<S, E> option(final Merge merge, final Map<Object, Object> m, final VertexProperty.Cardinality cardinality) {
        final Step<?, ?> lastStep = this.asAdmin().getEndStep();

        // CardinalityValueTraversal doesn't make sense for any prior step other than mergeV()
        if (!(lastStep instanceof MergeVertexStep)) {
            throw new IllegalStateException("option() with the Cardinality argument can only be used following mergeV()");
        }

        this.asAdmin().getGremlinLang().addStep(Symbols.option, merge, m, cardinality);
        // do explicit cardinality for every single pair in the map
        for (Object k : m.keySet()) {
            final Object o = m.get(k);
            if (!(o instanceof CardinalityValueTraversal))
                m.put(k, new CardinalityValueTraversal(cardinality, o));
        }
        ((TraversalOptionParent<M, E, E2>) lastStep).addChildOption((M) merge, (Traversal.Admin<E, E2>) new ConstantTraversal<>(m).asAdmin());
        return this;
    }

    /**
     * This step modifies {@link #choose(Function)} to specifies the available choices that might be executed.
     *
     * @param traversalOption the option as a traversal
     * @return the traversal with the modulated step
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#choose-step" target="_blank">Reference Documentation - Choose Step</a>
     * @since 3.0.0-incubating
     */
    public default <E2> GraphTraversal<S, E> option(final Traversal<?, E2> traversalOption) {
        this.asAdmin().getGremlinLang().addStep(Symbols.option, traversalOption);
        ((TraversalOptionParent<Object, E, E2>) this.asAdmin().getEndStep()).addChildOption(Pick.any, (Traversal.Admin<E, E2>) traversalOption.asAdmin());
        return this;
    }

    ////

    ///////////////////// IO STEPS /////////////////////

    /**
     * This step is technically a step modulator for the the {@link GraphTraversalSource#io(String)} step which
     * instructs the step to perform a read with its given configuration.
     *
     * @return the traversal with the {@link IoStep} modulated to read
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#io-step" target="_blank">Reference Documentation - IO Step</a>
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#read-step" target="_blank">Reference Documentation - Read Step</a>
     * @since 3.4.0
     */
    public default GraphTraversal<S,E> read() {
        this.asAdmin().getGremlinLang().addStep(Symbols.read);
        ((ReadWriting) this.asAdmin().getEndStep()).setMode(ReadWriting.Mode.READING);
        return this;
    }

    /**
     * This step is technically a step modulator for the the {@link GraphTraversalSource#io(String)} step which
     * instructs the step to perform a write with its given configuration.
     *
     * @return the traversal with the {@link IoStep} modulated to write
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#io-step" target="_blank">Reference Documentation - IO Step</a>
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#write-step" target="_blank">Reference Documentation - Write Step</a>
     * @since 3.4.0
     */
    public default GraphTraversal<S,E> write() {
        this.asAdmin().getGremlinLang().addStep(Symbols.write);
        ((ReadWriting) this.asAdmin().getEndStep()).setMode(ReadWriting.Mode.WRITING);
        return this;
    }

    /**
     * Iterates the traversal presumably for the generation of side-effects.
     */
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
        public static final String elementMap = "elementMap";
        public static final String select = "select";
        public static final String key = "key";
        public static final String value = "value";
        public static final String path = "path";
        public static final String match = "match";
        public static final String math = "math";
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
        public static final String groupCount = "groupCount";
        public static final String tree = "tree";
        public static final String addV = "addV";
        public static final String addE = "addE";
        public static final String mergeV = "mergeV";
        public static final String mergeE = "mergeE";
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
        public static final String conjoin = "conjoin";
        public static final String not = "not";
        public static final String range = "range";
        public static final String limit = "limit";
        public static final String skip = "skip";
        public static final String tail = "tail";
        public static final String coin = "coin";
        public static final String io = "io";
        public static final String read = "read";
        public static final String write = "write";
        public static final String call = "call";
        public static final String element = "element";
        public static final String concat = "concat";
        public static final String asString = "asString";
        public static final String toUpper = "toUpper";
        public static final String toLower = "toLower";
        public static final String length = "length";
        public static final String trim = "trim";
        public static final String lTrim = "lTrim";
        public static final String rTrim = "rTrim";
        public static final String reverse = "reverse";
        public static final String replace = "replace";
        public static final String substring = "substring";
        public static final String split = "split";
        public static final String format = "format";
        public static final String asDate = "asDate";
        public static final String dateAdd = "dateAdd";
        public static final String dateDiff = "dateDiff";
        public static final String all = "all";
        public static final String any = "any";
        public static final String none = "none";
        public static final String merge = "merge";
        public static final String product = "product";
        public static final String combine = "combine";
        public static final String difference = "difference";
        public static final String disjunct = "disjunct";
        public static final String intersect = "intersect";

        public static final String timeLimit = "timeLimit";
        public static final String simplePath = "simplePath";
        public static final String cyclicPath = "cyclicPath";
        public static final String sample = "sample";

        public static final String drop = "drop";

        public static final String sideEffect = "sideEffect";
        public static final String cap = "cap";
        public static final String property = "property";

        /**
         * @deprecated As of release 3.4.3, replaced by {@link Symbols#aggregate} with a {@link Scope#local}.
         */
        @Deprecated
        public static final String store = "store";
        public static final String aggregate = "aggregate";
        public static final String fail = "fail";
        public static final String subgraph = "subgraph";
        public static final String barrier = "barrier";
        public static final String index = "index";
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
        public static final String connectedComponent = "connectedComponent";
        public static final String shortestPath = "shortestPath";
        public static final String program = "program";

        public static final String by = "by";
        public static final String with = "with";
        public static final String times = "times";
        public static final String as = "as";
        public static final String option = "option";

    }
}