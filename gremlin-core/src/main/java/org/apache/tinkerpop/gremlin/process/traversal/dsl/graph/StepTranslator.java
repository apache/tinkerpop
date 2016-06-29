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

package org.apache.tinkerpop.gremlin.process.traversal.dsl.graph;

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRankVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PeerPressureVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ProgramVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.TraversalSourceSymbols;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ColumnTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.FunctionTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.LoopTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.PredicateTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TrueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
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
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.RequirementsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SackStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SideEffectStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.function.ConstantSupplier;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class StepTranslator implements Translator<GraphTraversal.Admin<?, ?>, GraphTraversalSource> {

    @Override
    public String getAlias() {
        return null;
    }

    @Override
    public GraphTraversalSource addSource(final GraphTraversalSource traversalSource, final String sourceName, final Object... arguments) {
        final GraphTraversalSource clone = traversalSource.clone();
        switch (sourceName) {
            case TraversalSourceSymbols.withComputer:
                Class<? extends GraphComputer> graphComputerClass;
                try {
                    graphComputerClass = ((Computer) arguments[0]).apply(clone.getGraph()).getClass();
                } catch (final Exception e) {
                    graphComputerClass = ((Computer) arguments[0]).getGraphComputerClass();
                }
                final List<TraversalStrategy<?>> graphComputerStrategies = TraversalStrategies.GlobalCache.getStrategies(graphComputerClass).toList();
                final TraversalStrategy[] traversalStrategies = new TraversalStrategy[graphComputerStrategies.size() + 1];
                traversalStrategies[0] = new VertexProgramStrategy(((Computer) arguments[0]));
                for (int i = 0; i < graphComputerStrategies.size(); i++) {
                    traversalStrategies[i + 1] = graphComputerStrategies.get(i);
                }
                clone.getStrategies().addStrategies(traversalStrategies);
                return clone;
            case Symbols.withPath:
                RequirementsStrategy.addRequirements(clone.strategies, TraverserRequirement.PATH);
                return clone;
            case Symbols.withBulk:
                if (1 == arguments.length && !((boolean) arguments[0]))
                    RequirementsStrategy.addRequirements(clone.strategies, TraverserRequirement.ONE_BULK);
                return clone;
            case TraversalSourceSymbols.withSideEffect:
                SideEffectStrategy.addSideEffect(clone.getStrategies(), (String) arguments[0],
                        arguments[1] instanceof Supplier ? (Supplier) arguments[1] : new ConstantSupplier<>(arguments[1]), 2 == arguments.length ? null : (BinaryOperator) arguments[2]);
                return clone;
            case TraversalSourceSymbols.withSack:
                if (1 == arguments.length) {
                    clone.getStrategies().addStrategies(SackStrategy.build().initialValue(arguments[0] instanceof Supplier ?
                            (Supplier) arguments[0] :
                            new ConstantSupplier<>(arguments[0])).create());
                } else if (2 == arguments.length) {
                    if (arguments[1] instanceof UnaryOperator)
                        clone.getStrategies().addStrategies(SackStrategy.build().initialValue(arguments[0] instanceof Supplier ?
                                (Supplier) arguments[0] :
                                new ConstantSupplier<>(arguments[0])).splitOperator((UnaryOperator) arguments[1]).create());
                    else
                        clone.getStrategies().addStrategies((SackStrategy.build().initialValue(arguments[0] instanceof Supplier ?
                                (Supplier) arguments[0] :
                                new ConstantSupplier<>(arguments[0])).mergeOperator((BinaryOperator) arguments[1]).create()));
                } else {
                    clone.getStrategies().addStrategies(SackStrategy.build().initialValue(arguments[0] instanceof Supplier ?
                            (Supplier) arguments[0] :
                            new ConstantSupplier<>(arguments[0])).splitOperator((UnaryOperator) arguments[1]).mergeOperator((BinaryOperator) arguments[2]).create());
                }
                return clone;
            default:
                throw new IllegalArgumentException("The provided step name is not supported by " + StepTranslator.class.getSimpleName() + ": " + sourceName);
        }

    }

    public GraphTraversal.Admin<?, ?> addSpawnStep(final GraphTraversalSource traversalSource, final String stepName, final Object... arguments) {
        final GraphTraversal.Admin<?, ?> traversal = new DefaultGraphTraversal<>(traversalSource.getGraph());
        traversal.setStrategies(traversalSource.getStrategies().clone());
        ///
        switch (stepName) {
            case Symbols.addV:
                return traversal.addStep(new AddVertexStartStep(traversal, 0 == arguments.length ? null : (String) arguments[0]));
            case Symbols.inject:
                return traversal.addStep(new InjectStep<>(traversal, arguments));
            case Symbols.V:
                return traversal.addStep(new GraphStep<>(traversal, Vertex.class, true, arguments));
            case Symbols.E:
                return traversal.addStep(new GraphStep<>(traversal, Edge.class, true, arguments));
            default:
                throw new IllegalArgumentException("The provided step name is not supported by " + StepTranslator.class.getSimpleName() + ": " + stepName);
        }
    }

    @Override
    public GraphTraversal.Admin<?, ?> addStep(final GraphTraversal.Admin<?, ?> traversal, final String stepName, final Object... arguments) {
        switch (stepName) {
            case Symbols.map:
                traversal.addStep(arguments[0] instanceof Traversal ?
                        new TraversalMapStep<>(traversal, (Traversal) arguments[0]) :
                        new LambdaMapStep<>(traversal, (Function) arguments[0]));
                return traversal;
            case Symbols.flatMap:
                traversal.addStep(arguments[0] instanceof Traversal ?
                        new TraversalFlatMapStep<>(traversal, (Traversal) arguments[0]) :
                        new LambdaFlatMapStep<>(traversal, (Function) arguments[0]));
                return traversal;
            case Symbols.id:
                traversal.addStep(new IdStep<>(traversal));
                return traversal;
            case Symbols.label:
                traversal.addStep(new LabelStep<>(traversal));
                return traversal;
            case Symbols.identity:
                traversal.addStep(new IdentityStep<>(traversal));
                return traversal;
            case Symbols.constant:
                traversal.addStep(new ConstantStep<>(traversal, arguments[0]));
                return traversal;
            case Symbols.V:
                traversal.addStep(new GraphStep<>(traversal, Vertex.class, false, arguments));
                return traversal;
            case Symbols.to:
                if (1 == arguments.length)
                    ((AddEdgeStep) traversal.getEndStep()).addTo(arguments[0] instanceof Vertex ?
                            (Vertex) arguments[0] :
                            __.select((String) arguments[0]));
                else
                    traversal.addStep(new VertexStep<>(traversal, Vertex.class, (Direction) arguments[0], (String[]) arguments[1]));
                return traversal;
            case Symbols.out:
                traversal.addStep(new VertexStep<>(traversal, Vertex.class, Direction.OUT, (String[]) arguments));
                return traversal;
            case Symbols.in:
                traversal.addStep(new VertexStep<>(traversal, Vertex.class, Direction.IN, (String[]) arguments));
                return traversal;
            case Symbols.both:
                traversal.addStep(new VertexStep<>(traversal, Vertex.class, Direction.BOTH, (String[]) arguments));
                return traversal;
            case Symbols.toE:
                traversal.addStep(new VertexStep<>(traversal, Edge.class, (Direction) arguments[0], (String[]) arguments[1]));
                return traversal;
            case Symbols.outE:
                traversal.addStep(new VertexStep<>(traversal, Edge.class, Direction.OUT, (String[]) arguments));
                return traversal;
            case Symbols.inE:
                traversal.addStep(new VertexStep<>(traversal, Edge.class, Direction.IN, (String[]) arguments));
                return traversal;
            case Symbols.bothE:
                traversal.addStep(new VertexStep<>(traversal, Edge.class, Direction.BOTH, (String[]) arguments));
                return traversal;
            case Symbols.toV:
                traversal.addStep(new EdgeVertexStep(traversal, (Direction) arguments[0]));
                return traversal;
            case Symbols.outV:
                traversal.addStep(new EdgeVertexStep(traversal, Direction.OUT));
                return traversal;
            case Symbols.inV:
                traversal.addStep(new EdgeVertexStep(traversal, Direction.IN));
                return traversal;
            case Symbols.bothV:
                traversal.addStep(new EdgeVertexStep(traversal, Direction.BOTH));
                return traversal;
            case Symbols.otherV:
                traversal.addStep(new EdgeOtherVertexStep(traversal));
                return traversal;
            case Symbols.order:
                traversal.addStep(arguments.length == 0 || Scope.global == arguments[0] ?
                        new OrderGlobalStep<>(traversal) :
                        new OrderLocalStep<>(traversal));
                return traversal;
            case Symbols.properties:
                traversal.addStep(new PropertiesStep<>(traversal, PropertyType.PROPERTY, (String[]) arguments));
                return traversal;
            case Symbols.values:
                traversal.addStep(new PropertiesStep<>(traversal, PropertyType.VALUE, (String[]) arguments));
                return traversal;
            case Symbols.propertyMap:
                traversal.addStep(0 != arguments.length && arguments[0] instanceof Boolean ?
                        new PropertyMapStep<>(traversal, (boolean) arguments[0], PropertyType.PROPERTY, (String[]) arguments[1]) :
                        new PropertyMapStep<>(traversal, false, PropertyType.PROPERTY, (String[]) arguments));
                return traversal;
            case Symbols.valueMap:
                traversal.addStep(0 != arguments.length && arguments[0] instanceof Boolean ?
                        new PropertyMapStep<>(traversal, (boolean) arguments[0], PropertyType.VALUE, (String[]) arguments[1]) :
                        new PropertyMapStep<>(traversal, false, PropertyType.VALUE, (String[]) arguments));
                return traversal;
            case Symbols.select:
                if (arguments[0] instanceof Column)
                    traversal.addStep(new TraversalMapStep<>(traversal, new ColumnTraversal((Column) arguments[0])));
                else if (arguments[0] instanceof Pop) {
                    if (arguments[1] instanceof String)
                        traversal.addStep(new SelectOneStep<>(traversal, (Pop) arguments[0], (String) arguments[1]));
                    else {
                        final String[] selectKeys = new String[((String[]) arguments[3]).length + 2];
                        selectKeys[0] = (String) arguments[1];
                        selectKeys[1] = (String) arguments[2];
                        System.arraycopy(arguments[3], 0, selectKeys, 2, ((String[]) arguments[3]).length);
                        traversal.addStep(new SelectStep<>(traversal, (Pop) arguments[0], selectKeys));
                    }
                } else {
                    if (1 == arguments.length && arguments[0] instanceof String)
                        traversal.addStep(new SelectOneStep<>(traversal, null, (String) arguments[0]));
                    else {
                        final String[] selectKeys = new String[((String[]) arguments[2]).length + 2];
                        selectKeys[0] = (String) arguments[0];
                        selectKeys[1] = (String) arguments[1];
                        System.arraycopy(arguments[2], 0, selectKeys, 2, ((String[]) arguments[2]).length);
                        traversal.addStep(new SelectStep<>(traversal, null, selectKeys));
                    }
                }
                return traversal;
            case Symbols.key:
                traversal.addStep(new PropertyKeyStep(traversal));
                return traversal;
            case Symbols.value:
                traversal.addStep(new PropertyValueStep<>(traversal));
                return traversal;
            case Symbols.path:
                traversal.addStep(new PathStep<>(traversal));
                return traversal;
            case Symbols.match:
                traversal.addStep(new MatchStep<>(traversal, ConnectiveStep.Connective.AND, (Traversal[]) arguments));
                return traversal;
            case Symbols.sack:
                if (0 == arguments.length)
                    traversal.addStep(new SackStep<>(traversal));
                else {
                    final SackValueStep<?, ?, ?> sackValueStep = new SackValueStep<>(traversal, (BiFunction) arguments[0]);
                    if (2 == arguments.length)
                        sackValueStep.modulateBy((String) arguments[1]);
                    traversal.addStep(sackValueStep);
                }
                return traversal;
            case Symbols.loops:
                traversal.addStep(new LoopsStep<>(traversal));
                return traversal;
            case Symbols.project:
                final String[] projectKeys = new String[((String[]) arguments[1]).length + 1];
                projectKeys[0] = (String) arguments[0];
                System.arraycopy((arguments[1]), 0, projectKeys, 1, ((String[]) arguments[1]).length);
                traversal.addStep(new ProjectStep<>(traversal, projectKeys));
                return traversal;
            case Symbols.unfold:
                traversal.addStep(new UnfoldStep<>(traversal));
                return traversal;
            case Symbols.fold:
                traversal.addStep(0 == arguments.length ?
                        new FoldStep<>(traversal) :
                        new FoldStep<>(traversal, arguments[0] instanceof Supplier ?
                                (Supplier) arguments[0] :
                                new ConstantSupplier(arguments[0]), (BiFunction) arguments[1]));
                return traversal;
            case Symbols.count:
                traversal.addStep(0 == arguments.length || Scope.global == arguments[0] ?
                        new CountGlobalStep<>(traversal) :
                        new CountLocalStep<>(traversal));
                return traversal;
            case Symbols.sum:
                traversal.addStep(0 == arguments.length || Scope.global == arguments[0] ?
                        new SumGlobalStep<>(traversal) :
                        new SumLocalStep<>(traversal));
                return traversal;
            case Symbols.max:
                traversal.addStep(0 == arguments.length || Scope.global == arguments[0] ?
                        new MaxGlobalStep<>(traversal) :
                        new MaxLocalStep<>(traversal));
                return traversal;
            case Symbols.min:
                traversal.addStep(0 == arguments.length || Scope.global == arguments[0] ?
                        new MinGlobalStep<>(traversal) :
                        new MinLocalStep<>(traversal));
                return traversal;
            case Symbols.mean:
                traversal.addStep(0 == arguments.length || Scope.global == arguments[0] ?
                        new MeanGlobalStep<>(traversal) :
                        new MeanLocalStep<>(traversal));
                return traversal;
            case Symbols.group:
                traversal.addStep(0 == arguments.length ?
                        new GroupStep<>(traversal) :
                        new GroupSideEffectStep<>(traversal, (String) arguments[0]));
                return traversal;
            case Symbols.groupV3d0:
                traversal.addStep(0 == arguments.length ?
                        new GroupStepV3d0<>(traversal) :
                        new GroupSideEffectStepV3d0<>(traversal, (String) arguments[0]));
                return traversal;
            case Symbols.groupCount:
                traversal.addStep(0 == arguments.length ?
                        new GroupCountStep<>(traversal) :
                        new GroupCountSideEffectStep<>(traversal, (String) arguments[0]));
                return traversal;
            case Symbols.tree:
                traversal.addStep(0 == arguments.length ?
                        new TreeStep<>(traversal) :
                        new TreeSideEffectStep<>(traversal, (String) arguments[0]));
                return traversal;
            case Symbols.addV:
                traversal.addStep(1 == arguments.length ?
                        new AddVertexStep<>(traversal, (String) arguments[0]) :
                        new AddVertexStep<>(traversal, null));
                if (arguments.length > 1) {
                    for (int i = 0; i < arguments.length; i = i + 2) {
                        traversal.addStep(new AddPropertyStep<>(traversal, null, arguments[i], arguments[i + 1]));
                    }
                }
                return traversal;
            case Symbols.addE:
                if (1 == arguments.length)
                    traversal.addStep(new AddEdgeStep<>(traversal, (String) arguments[0]));
                return traversal;
            case Symbols.from:
                ((AddEdgeStep) traversal.getEndStep()).addFrom(arguments[0] instanceof Vertex ?
                        (Vertex) arguments[0] :
                        __.select((String) arguments[0]));
                return traversal;
            case Symbols.filter:
                traversal.addStep(arguments[0] instanceof Traversal ?
                        new TraversalFilterStep<>(traversal, (Traversal) arguments[0]) :
                        new LambdaFilterStep<>(traversal, (Predicate) arguments[0]));
                return traversal;
            case Symbols.or:
                traversal.addStep(new OrStep<>(traversal, (Traversal[]) arguments));
                return traversal;
            case Symbols.and:
                traversal.addStep(new AndStep<>(traversal, (Traversal[]) arguments));
                return traversal;
            case Symbols.inject:
                traversal.addStep(new InjectStep<>(traversal, arguments));
                return traversal;
            case Symbols.dedup:
                if (0 == arguments.length)
                    traversal.addStep(new DedupGlobalStep<>(traversal));
                else {
                    traversal.addStep(arguments instanceof String[] ?
                            new DedupGlobalStep<>(traversal, (String[]) arguments) :
                            arguments[0] == Scope.global ?
                                    new DedupGlobalStep<>(traversal, (String[]) arguments[1]) :
                                    new DedupLocalStep<>(traversal));
                }
                return traversal;
            case Symbols.where:
                traversal.addStep(arguments[0] instanceof Traversal.Admin ?
                        TraversalHelper.getVariableLocations((Traversal.Admin) arguments[0]).isEmpty() ?
                                new TraversalFilterStep<>(traversal, (Traversal.Admin) arguments[0]) :
                                new WhereTraversalStep<>(traversal, (Traversal.Admin) arguments[0]) :
                        arguments[0] instanceof String ?
                                new WherePredicateStep<>(traversal, Optional.of((String) arguments[0]), (P) arguments[1]) :
                                new WherePredicateStep<>(traversal, Optional.empty(), (P) arguments[0]));
                return traversal;
            case Symbols.has:
                if (1 == arguments.length) {
                    traversal.addStep(new TraversalFilterStep<>(traversal, __.values((String) arguments[0])));
                } else if (2 == arguments.length) {
                    final String propertyKey = arguments[0] instanceof T ? ((T) arguments[0]).getAccessor() : (String) arguments[0];
                    if (arguments[1] instanceof Traversal.Admin) {
                        traversal.addStep(
                                new TraversalFilterStep<>(traversal, ((Traversal.Admin) arguments[1]).addStep(0,
                                        new PropertiesStep((Traversal.Admin) arguments[1], PropertyType.VALUE, propertyKey))));
                    } else {
                        final P predicate = arguments[1] instanceof P ? (P) arguments[1] : P.eq(arguments[1]);
                        traversal.addStep(new HasStep<>(traversal, HasContainer.makeHasContainers(propertyKey, predicate)));
                    }
                } else if (3 == arguments.length) {
                    traversal.addStep(new HasStep<>(traversal, HasContainer.makeHasContainers(T.label.getAccessor(), P.eq(arguments[0]))));
                    traversal.addStep(new HasStep<>(traversal, HasContainer.makeHasContainers(arguments[1] instanceof T ?
                                    ((T) arguments[1]).getAccessor() :
                                    (String) arguments[1],
                            arguments[2] instanceof P ?
                                    (P) arguments[2] :
                                    P.eq(arguments[2]))));
                }
                return traversal;
            case Symbols.hasNot:
                traversal.addStep(new NotStep<>(traversal, __.values((String) arguments[0])));
                return traversal;
            case Symbols.hasLabel:
                traversal.addStep(new HasStep<>(traversal, 0 == arguments.length ?
                        HasContainer.makeHasContainers(T.label.getAccessor(), P.eq(arguments[0])) :
                        HasContainer.makeHasContainers(T.label.getAccessor(), P.within(arguments))));
                return traversal;
            case Symbols.hasId:
                traversal.addStep(new HasStep<>(traversal, 0 == arguments.length ?
                        HasContainer.makeHasContainers(T.id.getAccessor(), P.eq(arguments[0])) :
                        HasContainer.makeHasContainers(T.id.getAccessor(), P.within(arguments))));
                return traversal;
            case Symbols.hasKey:
                traversal.addStep(new HasStep<>(traversal, 0 == arguments.length ?
                        HasContainer.makeHasContainers(T.key.getAccessor(), P.eq(arguments[0])) :
                        HasContainer.makeHasContainers(T.key.getAccessor(), P.within(arguments))));
                return traversal;
            case Symbols.hasValue:
                traversal.addStep(new HasStep<>(traversal, 0 == arguments.length ?
                        HasContainer.makeHasContainers(T.value.getAccessor(), P.eq(arguments[0])) :
                        HasContainer.makeHasContainers(T.value.getAccessor(), P.within(arguments))));
                return traversal;
            case Symbols.is:
                traversal.addStep(new IsStep<>(traversal, arguments[0] instanceof P ?
                        (P) arguments[0] :
                        P.eq(arguments[0])));
                return traversal;
            case Symbols.not:
                traversal.addStep(new NotStep<>(traversal, (Traversal) arguments[0]));
                return traversal;
            case Symbols.range:
                if (2 == arguments.length)
                    traversal.addStep(new RangeGlobalStep<>(traversal, (long) arguments[0], (long) arguments[1]));
                else
                    traversal.addStep(Scope.global == arguments[0] ?
                            new RangeGlobalStep<>(traversal, (long) arguments[1], (long) arguments[2]) :
                            new RangeLocalStep<>(traversal, (long) arguments[1], (long) arguments[2]));
                return traversal;
            case Symbols.limit:
                if (1 == arguments.length)
                    traversal.addStep(new RangeGlobalStep<>(traversal, 0, (long) arguments[0]));
                else
                    traversal.addStep(Scope.global == arguments[0] ?
                            new RangeGlobalStep<>(traversal, 0, (long) arguments[1]) :
                            new RangeLocalStep<>(traversal, 0, (long) arguments[1]));
                return traversal;
            case Symbols.tail:
                if (0 == arguments.length)
                    traversal.addStep(new TailGlobalStep<>(traversal, 1L));
                else if (arguments[0] instanceof Long)
                    traversal.addStep(new TailGlobalStep<>(traversal, (long) arguments[0]));
                else if (1 == arguments.length)
                    traversal.addStep(Scope.global == arguments[0] ?
                            new TailGlobalStep<>(traversal, 1L) :
                            new TailLocalStep<>(traversal, 1L));
                else
                    traversal.addStep(Scope.global == arguments[0] ?
                            new TailGlobalStep<>(traversal, (long) arguments[1]) :
                            new TailLocalStep<>(traversal, (long) arguments[1]));
                return traversal;
            case Symbols.coin:
                traversal.addStep(new CoinStep<>(traversal, (double) arguments[0]));
                return traversal;
            case Symbols.timeLimit:
                traversal.addStep(new TimeLimitStep<>(traversal, (long) arguments[0]));
                return traversal;
            case Symbols.simplePath:
                traversal.addStep(new SimplePathStep<>(traversal));
                return traversal;
            case Symbols.cyclicPath:
                traversal.addStep(new CyclicPathStep<>(traversal));
                return traversal;
            case Symbols.sample:
                if (1 == arguments.length)
                    traversal.addStep(new SampleGlobalStep<>(traversal, (int) arguments[0]));
                else
                    traversal.addStep(Scope.global == arguments[0] ?
                            new SampleGlobalStep<>(traversal, (int) arguments[1]) :
                            new SampleLocalStep<>(traversal, (int) arguments[1]));
                return traversal;
            case Symbols.drop:
                traversal.addStep(new DropStep<>(traversal));
                return traversal;
            case Symbols.sideEffect:
                traversal.addStep(arguments[0] instanceof Traversal ?
                        new TraversalSideEffectStep<>(traversal, (Traversal) arguments[0]) :
                        new LambdaSideEffectStep<>(traversal, (Consumer) arguments[0]));
                return traversal;
            case Symbols.cap:
                traversal.addStep(1 == arguments.length ?
                        new SideEffectCapStep<>(traversal, (String) arguments[0]) :
                        new SideEffectCapStep<>(traversal, (String) arguments[0], (String[]) arguments[1]));
                return traversal;
            case Symbols.aggregate:
                traversal.addStep(new AggregateStep<>(traversal, (String) arguments[0]));
                return traversal;
            case Symbols.store:
                traversal.addStep(new StoreStep<>(traversal, (String) arguments[0]));
                return traversal;
            case Symbols.subgraph:
                traversal.addStep(new SubgraphStep(traversal, (String) arguments[0]));
                return traversal;
            case Symbols.profile:
                traversal.addStep(new ProfileSideEffectStep<>(traversal, 0 == arguments.length ?
                        ProfileSideEffectStep.DEFAULT_METRICS_KEY :
                        (String) arguments[0]));
                if (0 == arguments.length)
                    traversal.addStep(new SideEffectCapStep<>(traversal, ProfileSideEffectStep.DEFAULT_METRICS_KEY));
                return traversal;
            case Symbols.branch:
                final BranchStep branchStep = new BranchStep<>(traversal);
                branchStep.setBranchTraversal(arguments[0] instanceof Traversal.Admin ?
                        (Traversal.Admin) arguments[0] :
                        __.map((Function) arguments[0]).asAdmin());
                traversal.addStep(branchStep);
                return traversal;
            case Symbols.choose:
                if (1 == arguments.length)
                    traversal.addStep(new ChooseStep<>(traversal, arguments[0] instanceof Traversal.Admin ?
                            (Traversal.Admin) arguments[0] :
                            __.map(new FunctionTraverser<>((Function) arguments[0])).asAdmin()));
                else
                    traversal.addStep(new ChooseStep<>(traversal,
                            arguments[0] instanceof Traversal.Admin ?
                                    (Traversal.Admin) arguments[0] :
                                    __.filter(new PredicateTraverser<>((Predicate) arguments[0])).asAdmin(),
                            (Traversal.Admin) arguments[1], (Traversal.Admin) arguments[2]));
                return traversal;
            case Symbols.optional:
                traversal.addStep(new ChooseStep<>(traversal, (Traversal.Admin) arguments[0], ((Traversal.Admin) arguments[0]).clone(), __.identity().asAdmin()));
                return traversal;
            case Symbols.union:
                traversal.addStep(new UnionStep<>(traversal, Arrays.copyOf(arguments, arguments.length, Traversal.Admin[].class)));
                return traversal;
            case Symbols.coalesce:
                traversal.addStep(new CoalesceStep<>(traversal, Arrays.copyOf(arguments, arguments.length, Traversal.Admin[].class)));
                return traversal;
            case Symbols.repeat:
                RepeatStep.addRepeatToTraversal(traversal, (Traversal.Admin) arguments[0]);
                return traversal;
            case Symbols.emit:
                if (0 == arguments.length)
                    RepeatStep.addEmitToTraversal(traversal, TrueTraversal.instance());
                else if (arguments[0] instanceof Traversal.Admin)
                    RepeatStep.addEmitToTraversal(traversal, (Traversal.Admin) arguments[0]);
                else
                    RepeatStep.addEmitToTraversal(traversal, __.filter((Predicate) arguments[0]).asAdmin());
                return traversal;
            case Symbols.until:
                if (arguments[0] instanceof Traversal.Admin)
                    RepeatStep.addUntilToTraversal(traversal, (Traversal.Admin) arguments[0]);
                else
                    RepeatStep.addUntilToTraversal(traversal, __.filter((Predicate) arguments[0]).asAdmin());
                return traversal;
            case Symbols.times:
                if (traversal.getEndStep() instanceof TimesModulating)
                    ((TimesModulating) traversal.getEndStep()).modulateTimes((int) arguments[0]);
                else
                    RepeatStep.addUntilToTraversal(traversal, new LoopTraversal<>((int) arguments[0]));
                return traversal;
            case Symbols.barrier:
                traversal.addStep(0 == arguments.length ?
                        new NoOpBarrierStep<>(traversal) :
                        arguments[0] instanceof Consumer ?
                                new LambdaCollectingBarrierStep<>(traversal, (Consumer) arguments[0], Integer.MAX_VALUE) :
                                new NoOpBarrierStep<>(traversal, (int) arguments[0]));
                return traversal;
            case Symbols.local:
                traversal.addStep(new LocalStep<>(traversal, (Traversal.Admin) arguments[0]));
                return traversal;
            case Symbols.pageRank:
                traversal.addStep(new PageRankVertexProgramStep(traversal, 0 == arguments.length ? 0.85d : (double) arguments[0]));
                return traversal;
            case Symbols.peerPressure:
                traversal.addStep(new PeerPressureVertexProgramStep(traversal));
                return traversal;
            case Symbols.program:
                traversal.addStep(new ProgramVertexProgramStep(traversal, (VertexProgram) arguments[0]));
                return traversal;
            case Symbols.by:
                if (0 == arguments.length)
                    ((ByModulating) traversal.getEndStep()).modulateBy();
                else if (1 == arguments.length) {
                    if (arguments[0] instanceof String)
                        ((ByModulating) traversal.getEndStep()).modulateBy((String) arguments[0]);
                    else if (arguments[0] instanceof T)
                        ((ByModulating) traversal.getEndStep()).modulateBy((T) arguments[0]);
                    else if (arguments[0] instanceof Traversal.Admin)
                        ((ByModulating) traversal.getEndStep()).modulateBy((Traversal.Admin) arguments[0]);
                    else if (arguments[0] instanceof Function)
                        ((ByModulating) traversal.getEndStep()).modulateBy((Function) arguments[0]);
                    else if (arguments[0] instanceof Order)
                        ((ByModulating) traversal.getEndStep()).modulateBy((Order) arguments[0]);
                    else if (arguments[0] instanceof Comparator)
                        ((ByModulating) traversal.getEndStep()).modulateBy((Comparator) arguments[0]);
                } else {
                    if (arguments[0] instanceof String)
                        ((ByModulating) traversal.getEndStep()).modulateBy((String) arguments[0], (Comparator) arguments[1]);
                    else if (arguments[0] instanceof T)
                        ((ByModulating) traversal.getEndStep()).modulateBy((T) arguments[0], (Comparator) arguments[1]);
                    else if (arguments[0] instanceof Traversal.Admin)
                        ((ByModulating) traversal.getEndStep()).modulateBy((Traversal.Admin) arguments[0], (Comparator) arguments[1]);
                    else

                        ((ByModulating) traversal.getEndStep()).modulateBy((Function) arguments[0], (Comparator) arguments[1]);
                }
                return traversal;
            case Symbols.as:
                if (traversal.getSteps().isEmpty())
                    traversal.addStep(new StartStep<>(traversal));
                traversal.getEndStep().addLabel((String) arguments[0]);
                if (arguments.length > 1) {
                    final String[] otherLabels = (String[]) arguments[1];
                    for (int i = 0; i < otherLabels.length; i++) {
                        traversal.getEndStep().addLabel(otherLabels[i]);
                    }
                }
                return traversal;
            case Symbols.option:
                if (1 == arguments.length)
                    ((TraversalOptionParent) traversal.getEndStep()).addGlobalChildOption(TraversalOptionParent.Pick.any, (Traversal.Admin) arguments[0]);
                else
                    ((TraversalOptionParent) traversal.getEndStep()).addGlobalChildOption(arguments[0], (Traversal.Admin) arguments[1]);
                return traversal;
            default:
                throw new IllegalArgumentException("The provided step name is not supported by " + StepTranslator.class.getSimpleName() + ": " + stepName);
        }
    }

    @Override
    public Translator getAnonymousTraversalTranslator() {
        return null;
    }

    @Override
    public String getTraversalScript() {
        return null;
    }

    @Override
    public Translator clone() {
        return this;
    }

    @Override
    public String getSourceLanguage() {
        return "gremlin-java";
    }

    @Override
    public String getTargetLanguage() {
        return "java";
    }
}
