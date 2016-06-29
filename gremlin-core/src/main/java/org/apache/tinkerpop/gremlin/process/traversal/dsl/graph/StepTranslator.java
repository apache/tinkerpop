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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ColumnTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.CoinStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.LambdaFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyKeyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SackStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalFlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.UnfoldStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class StepTranslator implements Translator {
    @Override
    public String getAlias() {
        return null;
    }

    @Override
    public void addStep(final Traversal.Admin<?, ?> traversal, final String stepName, final Object... arguments) {
        switch (stepName) {
            case Symbols.map:
                traversal.addStep(arguments[0] instanceof Traversal ?
                        new TraversalMapStep<>(traversal, (Traversal) arguments[0]) :
                        new LambdaMapStep<>(traversal, (Function) arguments[0]));
                return;
            case Symbols.flatMap:
                traversal.addStep(arguments[0] instanceof Traversal ?
                        new TraversalFlatMapStep<>(traversal, (Traversal) arguments[0]) :
                        new LambdaFlatMapStep<>(traversal, (Function) arguments[0]));
                return;
            case Symbols.id:
                traversal.addStep(new IdStep<>(traversal));
                return;
            case Symbols.label:
                traversal.addStep(new LabelStep<>(traversal));
                return;
            case Symbols.identity:
                traversal.addStep(new IdentityStep<>(traversal));
                return;
            case Symbols.constant:
                traversal.addStep(new ConstantStep<>(traversal, arguments[0]));
                return;
            case Symbols.V:
                traversal.addStep(new GraphStep<>(traversal, Vertex.class, false, arguments));
                return;
            case Symbols.to:
                if (1 == arguments.length)
                    ((AddEdgeStep) traversal.getEndStep()).addTo(arguments[0] instanceof Vertex ?
                            (Vertex) arguments[0] :
                            __.select((String) arguments[0]));
                else
                    traversal.addStep(new VertexStep<>(traversal, Vertex.class, (Direction) arguments[0], (String[]) arguments[1]));
                return;
            case Symbols.out:
                traversal.addStep(new VertexStep<>(traversal, Vertex.class, Direction.OUT, (String[]) arguments[0]));
                return;
            case Symbols.in:
                traversal.addStep(new VertexStep<>(traversal, Vertex.class, Direction.IN, (String[]) arguments[0]));
                return;
            case Symbols.both:
                traversal.addStep(new VertexStep<>(traversal, Vertex.class, Direction.BOTH, (String[]) arguments[0]));
                return;
            case Symbols.toE:
                traversal.addStep(new VertexStep<>(traversal, Edge.class, (Direction) arguments[0], (String[]) arguments[1]));
                return;
            case Symbols.outE:
                traversal.addStep(new VertexStep<>(traversal, Edge.class, Direction.OUT, (String[]) arguments[0]));
                return;
            case Symbols.inE:
                traversal.addStep(new VertexStep<>(traversal, Edge.class, Direction.IN, (String[]) arguments[0]));
                return;
            case Symbols.bothE:
                traversal.addStep(new VertexStep<>(traversal, Edge.class, Direction.BOTH, (String[]) arguments[0]));
                return;
            case Symbols.toV:
                traversal.addStep(new EdgeVertexStep(traversal, (Direction) arguments[0]));
                return;
            case Symbols.outV:
                traversal.addStep(new EdgeVertexStep(traversal, Direction.OUT));
                return;
            case Symbols.inV:
                traversal.addStep(new EdgeVertexStep(traversal, Direction.IN));
                return;
            case Symbols.bothV:
                traversal.addStep(new EdgeVertexStep(traversal, Direction.BOTH));
                return;
            case Symbols.otherV:
                traversal.addStep(new EdgeOtherVertexStep(traversal));
                return;
            case Symbols.order:
                traversal.addStep(arguments.length == 0 || Scope.global == arguments[0] ?
                        new OrderGlobalStep<>(traversal) :
                        new OrderLocalStep<>(traversal));
                return;
            case Symbols.properties:
                traversal.addStep(new PropertiesStep<>(traversal, PropertyType.PROPERTY, (String[]) arguments));
                return;
            case Symbols.values:
                traversal.addStep(new PropertiesStep<>(traversal, PropertyType.VALUE, (String[]) arguments));
                return;
            case Symbols.propertyMap:
                traversal.addStep(arguments[0] instanceof Boolean ?
                        new PropertyMapStep<>(traversal, (boolean) arguments[0], PropertyType.PROPERTY, (String[]) arguments[1]) :
                        new PropertyMapStep<>(traversal, false, PropertyType.PROPERTY, (String[]) arguments[0]));
                return;
            case Symbols.valueMap:
                traversal.addStep(arguments[0] instanceof Boolean ?
                        new PropertyMapStep<>(traversal, (boolean) arguments[0], PropertyType.VALUE, (String[]) arguments[1]) :
                        new PropertyMapStep<>(traversal, false, PropertyType.VALUE, (String[]) arguments[0]));
                return;
            case Symbols.select:
                if (arguments[0] instanceof Column)
                    traversal.addStep(new TraversalMapStep<>(traversal, new ColumnTraversal((Column) arguments[0])));
                else if (arguments[0] instanceof Pop)
                    if (arguments[1] instanceof String)
                        traversal.addStep(new SelectOneStep<>(traversal, (Pop) arguments[0], (String) arguments[1]));
                    else {
                        final String[] selectKeys = new String[((String[]) arguments[3]).length + 2];
                        selectKeys[0] = (String) arguments[1];
                        selectKeys[1] = (String) arguments[2];
                        System.arraycopy(arguments[3], 0, selectKeys, 2, ((String[]) arguments[3]).length);
                        traversal.addStep(new SelectStep<>(traversal, (Pop) arguments[0], selectKeys));
                    }
                else {
                    if (arguments[0] instanceof String)
                        traversal.addStep(new SelectOneStep<>(traversal, null, (String) arguments[0]));
                    else {
                        final String[] selectKeys = new String[((String[]) arguments[2]).length + 2];
                        selectKeys[0] = (String) arguments[0];
                        selectKeys[1] = (String) arguments[1];
                        System.arraycopy(arguments[2], 0, selectKeys, 2, ((String[]) arguments[2]).length);
                        traversal.addStep(new SelectStep<>(traversal, null, selectKeys));
                    }
                }
                return;
            case Symbols.key:
                traversal.addStep(new PropertyKeyStep(traversal));
                return;
            case Symbols.value:
                traversal.addStep(new PropertyValueStep<>(traversal));
                return;
            case Symbols.path:
                traversal.addStep(new PathStep<>(traversal));
                return;
            case Symbols.match:
                traversal.addStep(new MatchStep<>(traversal, ConnectiveStep.Connective.AND, (Traversal[]) arguments[0]));
                return;
            case Symbols.sack:
                traversal.addStep(new SackStep<>(traversal));
                return;
            case Symbols.loops:
                traversal.addStep(new LoopsStep<>(traversal));
                return;
            case Symbols.project:
                final String[] projectKeys = new String[((String[]) arguments[1]).length + 1];
                projectKeys[0] = (String) arguments[0];
                System.arraycopy((arguments[1]), 0, projectKeys, 1, ((String[]) arguments[1]).length);
                traversal.addStep(new ProjectStep<>(traversal, projectKeys));
                return;
            case Symbols.unfold:
                traversal.addStep(new UnfoldStep<>(traversal));
                return;
            case Symbols.fold:
                traversal.addStep(0 == arguments.length ?
                        new FoldStep<>(traversal) :
                        new FoldStep<>(traversal, (Supplier) arguments[0], (BiFunction) arguments[1]));
                return;
            case Symbols.count:
                traversal.addStep(0 == arguments.length || Scope.global == arguments[0] ?
                        new CountGlobalStep<>(traversal) :
                        new CountLocalStep<>(traversal));
                return;
            case Symbols.sum:
                traversal.addStep(0 == arguments.length || Scope.global == arguments[0] ?
                        new SumGlobalStep<>(traversal) :
                        new SumLocalStep<>(traversal));
                return;
            case Symbols.max:
                traversal.addStep(0 == arguments.length || Scope.global == arguments[0] ?
                        new MaxGlobalStep<>(traversal) :
                        new MaxLocalStep<>(traversal));
                return;
            case Symbols.min:
                traversal.addStep(0 == arguments.length || Scope.global == arguments[0] ?
                        new MinGlobalStep<>(traversal) :
                        new MinLocalStep<>(traversal));
                return;
            case Symbols.mean:
                traversal.addStep(0 == arguments.length || Scope.global == arguments[0] ?
                        new MeanGlobalStep<>(traversal) :
                        new MeanLocalStep<>(traversal));
                return;
            case Symbols.group:
                traversal.addStep(new GroupStep<>(traversal));
                return;
            case Symbols.groupV3d0:
                traversal.addStep(new GroupStepV3d0<>(traversal));
                return;
            case Symbols.groupCount:
                traversal.addStep(new GroupCountStep<>(traversal));
                return;
            case Symbols.tree:
                traversal.addStep(new TreeStep<>(traversal));
                return;
            case Symbols.addV:
                traversal.addStep(1 == arguments.length ?
                        new AddVertexStep<>(traversal, (String) arguments[0]) :
                        new AddVertexStep<>(traversal, null));
                if (arguments.length > 1) {
                    for (int i = 0; i < arguments.length; i = i + 2) {
                        traversal.addStep(new AddPropertyStep<>(traversal, null, arguments[i], arguments[i + 1]));
                    }
                }
                return;
            case Symbols.addE:
                if (1 == arguments.length)
                    traversal.addStep(new AddEdgeStep<>(traversal, (String) arguments[0]));
                return;
            case Symbols.from:
                ((AddEdgeStep) traversal.getEndStep()).addFrom(arguments[0] instanceof Vertex ?
                        (Vertex) arguments[0] :
                        __.select((String) arguments[0]));
                return;
            case Symbols.filter:
                traversal.addStep(arguments[0] instanceof Traversal ?
                        new TraversalFilterStep<>(traversal, (Traversal) arguments[0]) :
                        new LambdaFilterStep<>(traversal, (Predicate) arguments[0]));
                return;
            case Symbols.or:
                traversal.addStep(new OrStep<>(traversal, (Traversal[]) arguments));
                return;
            case Symbols.and:
                traversal.addStep(new AndStep<>(traversal, (Traversal[]) arguments));
                return;
            case Symbols.inject:
                traversal.addStep(new InjectStep<>(traversal, arguments));
                return;
            case Symbols.dedup:
                traversal.addStep(arguments[0] instanceof String[] ?
                        new DedupGlobalStep<>(traversal, (String[]) arguments[0]) :
                        arguments[0] == Scope.global ?
                                new DedupGlobalStep<>(traversal, (String[]) arguments[1]) :
                                new DedupLocalStep<>(traversal));
                return;
            case Symbols.where:
                traversal.addStep(arguments[0] instanceof Traversal ?
                        new WhereTraversalStep<>(traversal, (Traversal) arguments[0]) :
                        arguments[0] instanceof String ?
                                new WherePredicateStep<>(traversal, Optional.of((String) arguments[0]), (P) arguments[1]) :
                                new WherePredicateStep<>(traversal, Optional.empty(), (P) arguments[0]));
                return;
            case Symbols.is:
                traversal.addStep(new IsStep<>(traversal, arguments[0] instanceof P ? (P) arguments[0] : P.eq(arguments[0])));
                return;
            case Symbols.not:
                traversal.addStep(new NotStep<>(traversal, (Traversal) arguments[0]));
                return;
            case Symbols.coin:
                traversal.addStep(new CoinStep<>(traversal, (Double) arguments[0]));
                return;
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
