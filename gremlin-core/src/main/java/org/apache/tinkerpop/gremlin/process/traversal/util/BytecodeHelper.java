/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ConnectedComponentVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRankVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PeerPressureVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ProgramVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ShortestPathVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.GraphOp;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
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
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.LambdaFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DiscardStep;
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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AsBoolStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AsDateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AsNumberStep;
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
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.FailStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IoStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.LambdaSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TraversalSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Utility class for parsing {@link Bytecode}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class BytecodeHelper {
    private static final Map<String, List<Class<? extends Step>>> byteCodeSymbolStepMap;

    static {
        final Map<String, List<Class<? extends Step>>> operationStepMap = new HashMap<String, List<Class<? extends Step>>>() {{
            put(GraphTraversal.Symbols.map, Arrays.asList(LambdaMapStep.class, TraversalMapStep.class));
            put(GraphTraversal.Symbols.flatMap, Arrays.asList(LambdaFlatMapStep.class, TraversalFlatMapStep.class));
            put(GraphTraversal.Symbols.id, Collections.singletonList(IdStep.class));
            put(GraphTraversal.Symbols.label, Collections.singletonList(LabelStep.class));
            put(GraphTraversal.Symbols.identity, Collections.singletonList(IdentityStep.class));
            put(GraphTraversal.Symbols.constant, Collections.singletonList(ConstantStep.class));
            put(GraphTraversal.Symbols.V, Collections.singletonList(GraphStep.class));
            put(GraphTraversal.Symbols.E, Collections.singletonList(GraphStep.class));
            put(GraphTraversal.Symbols.to, Collections.emptyList());
            put(GraphTraversal.Symbols.out, Collections.singletonList(VertexStep.class));
            put(GraphTraversal.Symbols.in, Collections.singletonList(VertexStep.class));
            put(GraphTraversal.Symbols.both, Collections.singletonList(VertexStep.class));
            put(GraphTraversal.Symbols.toE, Collections.singletonList(VertexStep.class));
            put(GraphTraversal.Symbols.outE, Collections.singletonList(VertexStep.class));
            put(GraphTraversal.Symbols.inE, Collections.singletonList(VertexStep.class));
            put(GraphTraversal.Symbols.bothE, Collections.singletonList(VertexStep.class));
            put(GraphTraversal.Symbols.toV, Collections.singletonList(EdgeVertexStep.class));
            put(GraphTraversal.Symbols.outV, Collections.singletonList(EdgeVertexStep.class));
            put(GraphTraversal.Symbols.inV, Collections.singletonList(EdgeVertexStep.class));
            put(GraphTraversal.Symbols.bothV, Collections.singletonList(EdgeVertexStep.class));
            put(GraphTraversal.Symbols.otherV, Collections.singletonList(EdgeVertexStep.class));
            put(GraphTraversal.Symbols.order, Arrays.asList(OrderGlobalStep.class, OrderLocalStep.class));
            put(GraphTraversal.Symbols.properties, Collections.singletonList(PropertiesStep.class));
            put(GraphTraversal.Symbols.values, Collections.singletonList(PropertiesStep.class));
            put(GraphTraversal.Symbols.propertyMap, Collections.singletonList(PropertyMapStep.class));
            put(GraphTraversal.Symbols.valueMap, Collections.singletonList(PropertyMapStep.class));
            put(GraphTraversal.Symbols.elementMap, Collections.singletonList(ElementMapStep.class));
            put(GraphTraversal.Symbols.select, Arrays.asList(SelectStep.class, SelectOneStep.class,
                    TraversalSelectStep.class, TraversalMapStep.class));
            put(GraphTraversal.Symbols.key, Collections.singletonList(PropertyKeyStep.class));
            put(GraphTraversal.Symbols.value, Collections.singletonList(PropertyValueStep.class));
            put(GraphTraversal.Symbols.path, Collections.singletonList(PathStep.class));
            put(GraphTraversal.Symbols.match, Collections.singletonList(MatchStep.class));
            put(GraphTraversal.Symbols.math, Collections.singletonList(MathStep.class));
            put(GraphTraversal.Symbols.sack, Arrays.asList(SackStep.class, SackValueStep.class));
            put(GraphTraversal.Symbols.loops, Collections.singletonList(LoopsStep.class));
            put(GraphTraversal.Symbols.project, Collections.singletonList(ProjectStep.class));
            put(GraphTraversal.Symbols.unfold, Collections.singletonList(UnfoldStep.class));
            put(GraphTraversal.Symbols.fold, Collections.singletonList(FoldStep.class));
            put(GraphTraversal.Symbols.count, Arrays.asList(CountGlobalStep.class, CountLocalStep.class));
            put(GraphTraversal.Symbols.sum, Arrays.asList(SumGlobalStep.class, SumLocalStep.class));
            put(GraphTraversal.Symbols.max, Arrays.asList(MaxGlobalStep.class, MaxLocalStep.class));
            put(GraphTraversal.Symbols.min, Arrays.asList(MinGlobalStep.class, MinGlobalStep.class));
            put(GraphTraversal.Symbols.mean, Arrays.asList(MeanGlobalStep.class, MeanLocalStep.class));
            put(GraphTraversal.Symbols.concat, Collections.singletonList(ConcatStep.class));
            put(GraphTraversal.Symbols.format, Collections.singletonList(FormatStep.class));
            put(GraphTraversal.Symbols.asString, Arrays.asList(AsStringGlobalStep.class, AsStringLocalStep.class));
            put(GraphTraversal.Symbols.length, Arrays.asList(LengthGlobalStep.class, LengthLocalStep.class));
            put(GraphTraversal.Symbols.toLower, Arrays.asList(ToLowerGlobalStep.class, ToLowerLocalStep.class));
            put(GraphTraversal.Symbols.toUpper, Arrays.asList(ToUpperGlobalStep.class, ToUpperLocalStep.class));
            put(GraphTraversal.Symbols.trim, Arrays.asList(TrimGlobalStep.class, TrimLocalStep.class));
            put(GraphTraversal.Symbols.lTrim, Arrays.asList(LTrimGlobalStep.class, LTrimLocalStep.class));
            put(GraphTraversal.Symbols.rTrim, Arrays.asList(RTrimGlobalStep.class, RTrimLocalStep.class));
            put(GraphTraversal.Symbols.reverse, Collections.singletonList(ReverseStep.class));
            put(GraphTraversal.Symbols.replace, Arrays.asList(ReplaceGlobalStep.class, ReplaceLocalStep.class));
            put(GraphTraversal.Symbols.substring, Arrays.asList(SubstringGlobalStep.class, ReplaceLocalStep.class));
            put(GraphTraversal.Symbols.split, Arrays.asList(SplitGlobalStep.class, SplitLocalStep.class));
            put(GraphTraversal.Symbols.asBool, Collections.singletonList(AsBoolStep.class));
            put(GraphTraversal.Symbols.asDate, Collections.singletonList(AsDateStep.class));
            put(GraphTraversal.Symbols.dateAdd, Collections.singletonList(DateAddStep.class));
            put(GraphTraversal.Symbols.dateDiff, Collections.singletonList(DateDiffStep.class));
            put(GraphTraversal.Symbols.asNumber, Collections.singletonList(AsNumberStep.class));
            put(GraphTraversal.Symbols.all, Collections.singletonList(AllStep.class));
            put(GraphTraversal.Symbols.any, Collections.singletonList(AnyStep.class));
            put(GraphTraversal.Symbols.none, Collections.singletonList(NoneStep.class));
            put(GraphTraversal.Symbols.combine, Collections.singletonList(CombineStep.class));
            put(GraphTraversal.Symbols.difference, Collections.singletonList(DifferenceStep.class));
            put(GraphTraversal.Symbols.disjunct, Collections.singletonList(DisjunctStep.class));
            put(GraphTraversal.Symbols.merge, Collections.singletonList(MergeStep.class));
            put(GraphTraversal.Symbols.conjoin, Collections.singletonList(ConjoinStep.class));
            put(GraphTraversal.Symbols.product, Collections.singletonList(ProductStep.class));
            put(GraphTraversal.Symbols.intersect, Collections.singletonList(IntersectStep.class));
            put(GraphTraversal.Symbols.group, Arrays.asList(GroupStep.class, GroupSideEffectStep.class));
            put(GraphTraversal.Symbols.groupCount, Arrays.asList(GroupCountStep.class, GroupCountSideEffectStep.class));
            put(GraphTraversal.Symbols.tree, Arrays.asList(TreeStep.class, TreeSideEffectStep.class));
            put(GraphTraversal.Symbols.addV, Collections.singletonList(AddVertexStep.class));
            put(GraphTraversal.Symbols.addE, Collections.singletonList(AddEdgeStep.class));
            put(GraphTraversal.Symbols.mergeV, Collections.singletonList(MergeVertexStep.class));
            put(GraphTraversal.Symbols.mergeE, Collections.singletonList(MergeEdgeStep.class));
            put(GraphTraversal.Symbols.from, Collections.emptyList());
            put(GraphTraversal.Symbols.filter, Arrays.asList(LambdaFilterStep.class, TraversalFilterStep.class));
            put(GraphTraversal.Symbols.or, Collections.singletonList(OrStep.class));
            put(GraphTraversal.Symbols.and, Collections.singletonList(AndStep.class));
            put(GraphTraversal.Symbols.inject, Collections.singletonList(InjectStep.class));
            put(GraphTraversal.Symbols.dedup, Arrays.asList(DedupGlobalStep.class, DedupLocalStep.class));
            put(GraphTraversal.Symbols.where, Arrays.asList(WherePredicateStep.class, WhereTraversalStep.class,
                    TraversalFilterStep.class));
            put(GraphTraversal.Symbols.has, Collections.singletonList(HasStep.class));
            put(GraphTraversal.Symbols.hasNot, Collections.singletonList(NotStep.class));
            put(GraphTraversal.Symbols.hasLabel, Collections.singletonList(HasStep.class));
            put(GraphTraversal.Symbols.hasId, Collections.singletonList(HasStep.class));
            put(GraphTraversal.Symbols.hasKey, Collections.singletonList(HasStep.class));
            put(GraphTraversal.Symbols.hasValue, Collections.singletonList(HasStep.class));
            put(GraphTraversal.Symbols.is, Collections.singletonList(IsStep.class));
            put(GraphTraversal.Symbols.not, Collections.singletonList(NotStep.class));
            put(GraphTraversal.Symbols.range, Arrays.asList(RangeGlobalStep.class, RangeLocalStep.class));
            put(GraphTraversal.Symbols.limit, Arrays.asList(RangeGlobalStep.class, RangeLocalStep.class));
            put(GraphTraversal.Symbols.skip, Arrays.asList(RangeGlobalStep.class, RangeLocalStep.class));
            put(GraphTraversal.Symbols.tail, Arrays.asList(TailGlobalStep.class, TailLocalStep.class));
            put(GraphTraversal.Symbols.coin, Collections.singletonList(CoinStep.class));
            put(GraphTraversal.Symbols.io, Collections.singletonList(IoStep.class));
            put(GraphTraversal.Symbols.read, Collections.emptyList());
            put(GraphTraversal.Symbols.write, Collections.emptyList());
            put(GraphTraversal.Symbols.call, Collections.singletonList(CallStep.class));
            put(GraphTraversal.Symbols.element, Collections.singletonList(ElementStep.class));
            put(GraphTraversal.Symbols.timeLimit, Collections.singletonList(TimeLimitStep.class));
            put(GraphTraversal.Symbols.simplePath, Collections.singletonList(PathFilterStep.class));
            put(GraphTraversal.Symbols.cyclicPath, Collections.singletonList(PathFilterStep.class));
            put(GraphTraversal.Symbols.sample, Arrays.asList(SampleGlobalStep.class, SampleLocalStep.class));
            put(GraphTraversal.Symbols.drop, Collections.singletonList(DropStep.class));
            put(GraphTraversal.Symbols.sideEffect, Arrays.asList(LambdaSideEffectStep.class, TraversalSideEffectStep.class));
            put(GraphTraversal.Symbols.cap, Collections.singletonList(SideEffectCapStep.class));
            put(GraphTraversal.Symbols.property, Collections.singletonList(AddPropertyStep.class));
            put(GraphTraversal.Symbols.aggregate, Collections.singletonList(AggregateStep.class));
            put(GraphTraversal.Symbols.fail, Collections.singletonList(FailStep.class));
            put(GraphTraversal.Symbols.subgraph, Collections.singletonList(SubgraphStep.class));
            put(GraphTraversal.Symbols.barrier, Arrays.asList(NoOpBarrierStep.class, LambdaCollectingBarrierStep.class));
            put(GraphTraversal.Symbols.index, Collections.singletonList(IndexStep.class));
            put(GraphTraversal.Symbols.local, Collections.singletonList(LocalStep.class));
            put(GraphTraversal.Symbols.emit, Collections.emptyList());
            put(GraphTraversal.Symbols.repeat, Collections.singletonList(RepeatStep.class));
            put(GraphTraversal.Symbols.until, Collections.emptyList());
            put(GraphTraversal.Symbols.branch, Collections.singletonList(BranchStep.class));
            put(GraphTraversal.Symbols.union, Collections.singletonList(UnionStep.class));
            put(GraphTraversal.Symbols.coalesce, Collections.singletonList(CoalesceStep.class));
            put(GraphTraversal.Symbols.choose, Collections.singletonList(ChooseStep.class));
            put(GraphTraversal.Symbols.optional, Collections.singletonList(OptionalStep.class));
            put(GraphTraversal.Symbols.pageRank, Collections.singletonList(PageRankVertexProgramStep.class));
            put(GraphTraversal.Symbols.peerPressure, Collections.singletonList(PeerPressureVertexProgramStep.class));
            put(GraphTraversal.Symbols.connectedComponent, Collections.singletonList(ConnectedComponentVertexProgramStep.class));
            put(GraphTraversal.Symbols.shortestPath, Collections.singletonList(ShortestPathVertexProgramStep.class));
            put(GraphTraversal.Symbols.program, Collections.singletonList(ProgramVertexProgramStep.class));
            put(GraphTraversal.Symbols.by, Collections.emptyList());
            put(GraphTraversal.Symbols.with, Collections.emptyList());
            put(GraphTraversal.Symbols.times, Collections.emptyList());
            put(GraphTraversal.Symbols.as, Collections.emptyList());
            put(GraphTraversal.Symbols.option, Collections.emptyList());
            put(Traversal.Symbols.profile, Collections.singletonList(ProfileStep.class));
            put(Traversal.Symbols.discard, Collections.singletonList(DiscardStep.class));
            put(TraversalSource.Symbols.withSack, Collections.emptyList());
            put(TraversalSource.Symbols.withoutStrategies, Collections.emptyList());
            put(TraversalSource.Symbols.withStrategies, Collections.emptyList());
            put(TraversalSource.Symbols.withSideEffect, Collections.emptyList());
            put(TraversalSource.Symbols.withRemote, Collections.emptyList());
            put(TraversalSource.Symbols.withComputer, Collections.emptyList());
            put(GraphTraversalSource.Symbols.withBulk, Collections.emptyList());
            put(GraphTraversalSource.Symbols.withPath, Collections.emptyList());
            put(GraphTraversalSource.Symbols.tx, Collections.emptyList());
        }};
        byteCodeSymbolStepMap = Collections.unmodifiableMap(operationStepMap);
    }

    private BytecodeHelper() {
        // public static methods only
    }

    /**
     * Parses {@link Bytecode} to find {@link TraversalStrategy} objects added in the source instructions.
     */
    public static <A extends TraversalStrategy> Iterator<A> findStrategies(final Bytecode bytecode, final Class<A> clazz) {
        return IteratorUtils.map(
                IteratorUtils.filter(bytecode.getSourceInstructions().iterator(),
                        s -> s.getOperator().equals(TraversalSource.Symbols.withStrategies) && clazz.isAssignableFrom(s.getArguments()[0].getClass())),
                os -> (A) os.getArguments()[0]);
    }

    public static boolean removeStrategies(final Bytecode bytecode, final String operator, final Class<TraversalStrategy>[] clazzes) {
        return bytecode.getSourceInstructions().removeIf(
                s -> {
                    if (!operator.equals(s.getOperator()) || clazzes.length != s.getArguments().length) {
                        return false;
                    }
                    for (int i = 0; i < clazzes.length; i++) {
                        final Class<?> sClass =
                                s.getArguments()[i] instanceof TraversalStrategyProxy ?
                                        ((TraversalStrategyProxy) s.getArguments()[i]).getStrategyClass() :
                                        ((s.getArguments()[i] instanceof Class) ? (Class<?>) s.getArguments()[i] : s.getArguments()[i].getClass());
                        if (!(clazzes[i].isAssignableFrom(sClass))) {
                            return false;
                        }
                    }
                    return true;
                }
        );
    }

    public static Bytecode filterInstructions(final Bytecode bytecode, final Predicate<Bytecode.Instruction> predicate) {
        final Bytecode clone = new Bytecode();
        for (final Bytecode.Instruction instruction : bytecode.getSourceInstructions()) {
            if (predicate.test(instruction))
                clone.addSource(instruction.getOperator(), instruction.getArguments());
        }
        for (final Bytecode.Instruction instruction : bytecode.getStepInstructions()) {
            if (predicate.test(instruction))
                clone.addStep(instruction.getOperator(), instruction.getArguments());
        }
        return clone;
    }

    /**
     * Checks if the bytecode is one of the standard {@link GraphOp} options.
     */
    public static boolean isGraphOperation(final Bytecode bytecode) {
        return Stream.of(GraphOp.values()).anyMatch(op -> op.equals(bytecode));
    }

    public static Optional<String> getLambdaLanguage(final Bytecode bytecode) {
        for (final Bytecode.Instruction instruction : bytecode.getInstructions()) {
            for (Object object : instruction.getArguments()) {
                if (object instanceof Lambda)
                    return Optional.of(((Lambda) object).getLambdaLanguage());
                else if (object instanceof Bytecode) {
                    final Optional<String> temp = BytecodeHelper.getLambdaLanguage((Bytecode) object);
                    if (temp.isPresent())
                        return temp;
                }
            }
        }
        return Optional.empty();
    }

    public static void removeBindings(final Bytecode bytecode) {
        for (final Bytecode.Instruction instruction : bytecode.getInstructions()) {
            final Object[] arguments = instruction.getArguments();
            for (int i = 0; i < arguments.length; i++) {
                if (arguments[i] instanceof Bytecode.Binding)
                    arguments[i] = ((Bytecode.Binding) arguments[i]).value();
                else if (arguments[i] instanceof Bytecode)
                    removeBindings((Bytecode) arguments[i]);
            }
        }
    }

    public static void detachElements(final Bytecode bytecode) {
        for (final Bytecode.Instruction instruction : bytecode.getInstructions()) {
            final Object[] arguments = instruction.getArguments();
            for (int i = 0; i < arguments.length; i++) {
                if (arguments[i] instanceof Bytecode)
                    detachElements((Bytecode) arguments[i]);
                else if(arguments[i] instanceof List) {
                    final List<Object> list = new ArrayList<>();
                    for(final Object object : (List)arguments[i]) {
                        list.add(DetachedFactory.detach(object, false));
                    }
                    arguments[i] = list;
                }
                else
                    arguments[i] = DetachedFactory.detach(arguments[i], false);
            }
        }
    }

    /**
     * Returns a list of {@link Step} which can be added to the traversal for the provided operator.
     * <p>
     * Graph Traversal may or may not add the returned list of steps into the traversal. List only represents
     * possible steps added to traversal given an operator.
     * </p>
     *
     * @param operator Graph operator
     * @return List of possible {@link Step}(s)
     */
    public static List<Class<? extends Step>> findPossibleTraversalSteps(final String operator) {
        if (!byteCodeSymbolStepMap.containsKey(operator)) {
            throw new IllegalArgumentException("Unable to find Traversal steps for the graph operator: " + operator);
        }
        return byteCodeSymbolStepMap.get(operator);
    }
}
