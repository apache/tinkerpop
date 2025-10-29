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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Pick;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.AbstractLambdaTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.LoopTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.PredicateTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TrueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CallStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CoalesceStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CombineStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ConcatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.DateDiffStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.DifferenceStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.DisjunctStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FormatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupCountStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IntersectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.OptionalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.PathFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProductStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalSelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalFlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TraversalSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.GValueReductionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.service.ServiceRegistry;

import org.apache.tinkerpop.gremlin.structure.util.GraphFactoryTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@RunWith(Parameterized.class)
public class TraversalParentTest {

    private static ServiceRegistry mockedRegistry= mock(ServiceRegistry.class);
    private static Service<?, ?> mockedService = mock(Service.class);
    private static GraphTraversalSource g = traversal().with(GraphFactoryTest.MockGraph.open(new MapConfiguration(Map.of("service-registry", mockedRegistry))));

    @BeforeClass
    public static void setupClass() {
        when(mockedRegistry.get(any(), anyBoolean(), any())).thenReturn(mockedService);
        when(mockedService.getRequirements()).thenReturn(Set.of());
    }

    @Parameterized.Parameter(value = 0)
    public Class<Step<?,?>> stepClass;

    @Parameterized.Parameter(value = 1)
    public Traversal.Admin<?,?> traversal;

    @Parameterized.Parameter(value = 2)
    public List<Traversal.Admin<?,?>> expectedGlobalChildren;

    @Parameterized.Parameter(value = 3)
    public List<Traversal.Admin<?,?>> expectedLocalChildren;

    /**
     * Overrides expectedGlobalChildren for assertions following strategy execution. If left null, expectedGlobalChildren is used instead.
     */
    @Parameterized.Parameter(value = 4)
    public List<Traversal.Admin<?,?>> postStrategyExpectedGlobalChildren;

    /**
     * Overrides expectedLocalChildren for assertions following strategy execution. If left null, expectedLocalChildren is used instead.
     */
    @Parameterized.Parameter(value = 5)
    public List<Traversal.Admin<?,?>> postStrategyExpectedLocalChildren;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {AddVertexStepContract.class,
                        g.addV("label").property("name", __.constant("cole")),
                        List.of(),
                        List.of(__.constant("cole")),
                        null, null
                },
                {AddVertexStepContract.class,
                        g.addV(__.constant("label")).property("name", __.constant("cole")),
                        List.of(),
                        List.of(__.constant("cole"), __.constant("label")),
                        null, null
                },
                {AddVertexStepContract.class,
                        g.addV().property("name", __.constant("cole")).property(T.label, __.constant("label")).property(T.id, __.constant(1)),
                        List.of(),
                        List.of(__.constant("cole"), __.constant("label"),  __.constant(1)),
                        null, null
                },
                {AddVertexStepContract.class,
                        g.addV(GValue.of("l", "label")).property("name", GValue.of("name", "cole")),
                        List.of(),
                        List.of(), // Property is not stored as a child traversal in this case
                        null, null
                },
                {AddVertexStepContract.class,
                        g.addV("label").property(__.constant("name"), __.constant("cole")),
                        List.of(),
                        List.of(__.constant("name"), __.constant("cole")),
                        null, null
                },
                {AddVertexStepContract.class,
                        g.inject(1).addV("label").property("name", __.constant("cole")),
                        List.of(),
                        List.of(__.constant("cole")),
                        null, null
                },
                {AddVertexStepContract.class,
                        g.inject(1).addV(__.constant("label")).property("name", __.constant("cole")),
                        List.of(),
                        List.of(__.constant("cole"), __.constant("label")),
                        null, null
                },
                {AddVertexStepContract.class,
                        g.inject(1).addV(GValue.of("l", "label")).property("name", GValue.of("name", "cole")),
                        List.of(),
                        List.of(), // Property is not stored as a child traversal in this case
                        null, null
                },
                {AddVertexStepContract.class,
                        g.inject(1).addV("label").property(__.constant("name"), __.constant("cole")),
                        List.of(),
                        List.of(__.constant("name"), __.constant("cole")),
                        null, null
                },
                {AddEdgeStepContract.class,
                        g.addE("label").from(__.V(1)).to(__.V(2))
                                .property("name", __.constant("cole"))
                                .property(T.id, __.constant(5)),
                        List.of(),
                        List.of(__.constant("cole"), __.V(1), __.V(2), __.constant(5)),
                        null, null
                },
                {AddEdgeStepContract.class,
                        g.addE("label").from(__.V(1)).to(__.V(2)).property("name", __.constant("cole")),
                        List.of(),
                        List.of(__.constant("cole"), __.V(1), __.V(2)),
                        null, null
                },
                {AddEdgeStepContract.class,
                        g.addE(GValue.of("l", "label")).from(GValue.of("from", 1)).to(GValue.of("to", 2)).property("name", GValue.of("name", "cole")),
                        List.of(),
                        List.of(new ConstantTraversal<>(1), new ConstantTraversal<>(2)),
                        null, null
                },
                {AddEdgeStepContract.class,
                        g.addE("label").from(__.V(1)).to(__.V(2)).property(__.constant("name"), __.constant("cole")),
                        List.of(),
                        List.of(__.constant("name"), __.constant("cole"), __.V(1), __.V(2)),
                        null, null
                },
                {AddEdgeStepContract.class,
                        g.inject(1).addE("label").from(__.V(1)).to(__.V(2)).property("name", __.constant("cole")),
                        List.of(),
                        List.of(__.constant("cole"), __.V(1), __.V(2)),
                        null, null
                },
                {AddEdgeStepContract.class,
                        g.inject(1).addE(GValue.of("l", "label")).from(GValue.of("from", 1)).to(GValue.of("to", 2)).property("name", GValue.of("name", "cole")),
                        List.of(),
                        List.of(new ConstantTraversal<>(1), new ConstantTraversal<>(2)),
                        null, null
                },
                {AddEdgeStepContract.class,
                        g.inject(1).addE("label").from(__.V(1)).to(__.V(2)).property(__.constant("name"), __.constant("cole")),
                        List.of(),
                        List.of(__.constant("name"), __.constant("cole"), __.V(1), __.V(2)),
                        null, null
                },
                {CallStepContract.class,
                        g.call("service"),
                        List.of(),
                        List.of(),
                        null, null
                },
                {CallStepContract.class,
                        g.call("service").with("key", __.constant("value")),
                        List.of(),
                        List.of(__.constant("value")),
                        null, null
                },
                {CallStepContract.class,
                        g.call("service", __.constant(Map.of("key", "value"))),
                        List.of(),
                        List.of(__.constant(Map.of("key", "value"))),
                        null, null
                },
                {CallStepContract.class,
                        g.call("service", __.constant(Map.of("key", "value"))).with("key", __.constant("value")),
                        List.of(),
                        List.of(__.constant(Map.of("key", "value")), __.constant("value")),
                        null, null
                },
                {AddPropertyStepContract.class,
                        g.inject(1).property("key", "value"),
                        List.of(),
                        List.of(),
                        null, null
                },
                {AddPropertyStepContract.class,
                        g.inject(1).property("key", __.constant("value")),
                        List.of(),
                        List.of(__.constant("value")),
                        null, null
                },
                {AddPropertyStepContract.class,
                        g.inject(1).property(__.constant("key"), __.constant("value")),
                        List.of(),
                        List.of(__.constant("key"), __.constant("value")),
                        null, null
                },
                {AddPropertyStepContract.class,
                        g.addV().property("name", "value", "metaKey", "metaValue"),
                        List.of(),
                        List.of(),
                        null, null
                },
                {AddPropertyStepContract.class,
                        g.addV().property("name", __.constant("value"), "metaKey", __.constant("metaValue")),
                        List.of(),
                        List.of(__.constant("value"), __.constant("metaValue")),
                        null, null
                },
                {AddPropertyStepContract.class,
                        g.addV().property(__.constant("name"), __.constant("value"), __.constant("metaKey"), __.constant("metaValue")),
                        List.of(),
                        List.of(__.constant("name"), __.constant("value"), __.constant("metaKey"), __.constant("metaValue")),
                        null, null
                },
                {AddPropertyStepContract.class,
                        g.addV().property("name", "value", "metaKey1", "metaValue1", "metaKey2", "metaValue2"),
                        List.of(),
                        List.of(),
                        null, null
                },
                {MergeStepContract.class,
                        g.mergeV(Map.of("name", "marko")),
                        List.of(),
                        List.of(new ConstantTraversal<>(Map.of("name", "marko"))), // constant MergeMap's are internally boxed in Traversals
                        null, null
                },
                {MergeStepContract.class,
                        g.mergeV(__.constant(Map.of("name", "marko"))),
                        List.of(),
                        List.of(__.constant(Map.of("name", "marko"))),
                        null, null
                },
                {MergeStepContract.class,
                        g.mergeV(Map.of("name", "marko")).option(Merge.onCreate, __.constant(Map.of("age", 29))),
                        List.of(),
                        List.of(new ConstantTraversal<>(Map.of("name", "marko")), __.constant(Map.of("age", 29))), // constant MergeMap's are internally boxed in Traversals
                        null, null
                },
                {MergeStepContract.class,
                        g.mergeV(__.constant(Map.of("name", "marko"))).option(Merge.onCreate, __.constant(Map.of("age", 29))).option(Merge.onMatch, __.constant(Map.of("updated", true))),
                        List.of(),
                        List.of(__.constant(Map.of("name", "marko")), __.constant(Map.of("age", 29)), __.constant(Map.of("updated", true))),
                        null, null
                },
                {MergeStepContract.class,
                        g.mergeE(Map.of(T.label, "knows")),
                        List.of(),
                        List.of(new ConstantTraversal<>(Map.of(T.label, "knows"))), // constant MergeMap's are internally boxed in Traversals
                        null, null
                },
                {MergeStepContract.class,
                        g.mergeE(__.constant(Map.of(T.label, "knows"))),
                        List.of(),
                        List.of(__.constant(Map.of(T.label, "knows"))),
                        null, null
                },
                {MergeStepContract.class,
                        g.mergeE(Map.of(T.label, "knows")).option(Merge.onCreate, __.constant(Map.of("weight", 0.5))),
                        List.of(),
                        List.of(new ConstantTraversal<>(Map.of(T.label, "knows")), __.constant(Map.of("weight", 0.5))), // constant MergeMap's are internally boxed in Traversals
                        null, null
                },
                {MergeStepContract.class,
                        g.mergeE(__.constant(Map.of(T.label, "knows"))).option(Merge.onCreate, __.constant(Map.of("weight", 0.5))).option(Merge.onMatch, __.constant(Map.of("updated", true))),
                        List.of(),
                        List.of(__.constant(Map.of(T.label, "knows")), __.constant(Map.of("weight", 0.5)), __.constant(Map.of("updated", true))),
                        null, null
                },
                {AggregateStep.class,
                        g.V().aggregate("x"),
                        List.of(),
                        List.of(),
                        null, null
                },
                {AggregateStep.class,
                        g.V().aggregate("x").by("name"),
                        List.of(),
                        List.of(new ValueTraversal<>("name")),
                        null, null
                },
                {AggregateStep.class,
                        g.V().aggregate("x").by(__.constant("value")),
                        List.of(),
                        List.of(__.constant("value")),
                        null, null
                },
                {AndStep.class,
                        g.V().and(__.outE(), __.has("age", P.gte(32))),
                        List.of(),
                        List.of(__.outE(), __.has("age", P.gte(32))),
                        null, null
                },
                {OrStep.class,
                        g.V().or(__.outE(), __.has("age", P.gte(32))),
                        List.of(),
                        List.of(__.outE(), __.has("age", P.gte(32))),
                        null, null
                },
                {BranchStep.class,
                        g.V().branch(__.label()).option("person", __.values("name")).option("software", __.values("lang")),
                        List.of(appendComputerAwareEndStep(__.values("name")), appendComputerAwareEndStep(__.values("lang"))),
                        List.of(__.label(), new PredicateTraversal<>(P.eq("person")), new PredicateTraversal<>(P.eq("software"))), // option keys are wrapped in PredicateTraversals internally
                        null, null
                },
                {ChooseStep.class,
                        g.V().choose(__.out().count()).option(2L, __.values("name")).option(3L, __.values("age")),
                        List.of(appendComputerAwareEndStep(__.values("name")), appendComputerAwareEndStep(__.values("age")), appendComputerAwareEndStep(__.identity()), appendComputerAwareEndStep(__.identity())),
                        List.of(__.out().count(), new PredicateTraversal<>(P.eq(2L)), new PredicateTraversal<>(P.eq(3L))), // option keys are wrapped in PredicateTraversals internally
                        null, List.of(__.outE().count(), new PredicateTraversal<>(P.eq(2L)), new PredicateTraversal<>(P.eq(3L)))
                },
                {CoalesceStep.class,
                        g.V().coalesce(__.out("knows"), __.out("created")),
                        List.of(),
                        List.of(__.out("knows"), __.out("created")),
                        null, null
                },
                {CombineStep.class,
                        g.V().fold().combine(__.constant(List.of(1, 2, 3))),
                        List.of(),
                        List.of(__.constant(List.of(1, 2, 3))),
                        null, null
                },
                {ConcatStep.class,
                        g.inject("a", "b").concat(__.constant("c"), __.constant("d")),
                        List.of(),
                        List.of(__.constant("c"), __.constant("d")),
                        null, null
                },
                {DateDiffStep.class,
                        g.inject(OffsetDateTime.parse("1970-01-01T00:00:00.000Z")).dateDiff(__.constant(OffsetDateTime.parse("1970-02-01T00:00:00.000Z"))),
                        List.of(),
                        List.of(__.constant(OffsetDateTime.parse("1970-02-01T00:00:00.000Z"))),
                        null, null
                },
                {DedupGlobalStep.class,
                        g.V().dedup(),
                        List.of(),
                        List.of(),
                        null, null
                },
                {DedupGlobalStep.class,
                        g.V().dedup().by("name"),
                        List.of(),
                        List.of(new ValueTraversal<>("name")),
                        null, null
                },
                {DedupGlobalStep.class,
                        g.V().dedup().by(__.constant("value")),
                        List.of(),
                        List.of(__.constant("value")),
                        null, null
                },
                {DifferenceStep.class,
                        g.V().fold().difference(__.constant(List.of(1, 2, 3))),
                        List.of(),
                        List.of(__.constant(List.of(1, 2, 3))),
                        null, null
                },
                {DisjunctStep.class,
                        g.V().fold().disjunct(__.constant(List.of(1, 2, 3))),
                        List.of(),
                        List.of(__.constant(List.of(1, 2, 3))),
                        null, null
                },
                {FormatStep.class,
                        g.V().format("Hello %{name}"),
                        List.of(),
                        List.of(),
                        null, null
                },
                {FormatStep.class,
                        g.V().format("Hello %{name}").by("name"),
                        List.of(),
                        List.of(new ValueTraversal<>("name")),
                        null, null
                },
                {FormatStep.class,
                        g.V().format("Hello %{name}").by(__.constant("value")),
                        List.of(),
                        List.of(__.constant("value")),
                        null, null
                },
                {GroupCountStep.class,
                        g.V().groupCount(),
                        List.of(),
                        List.of(),
                        null, null
                },
                {GroupCountStep.class,
                        g.V().groupCount().by("name"),
                        List.of(),
                        List.of(new ValueTraversal<>("name")),
                        null, null
                },
                {GroupCountStep.class,
                        g.V().groupCount().by(__.constant("value")),
                        List.of(),
                        List.of(__.constant("value")),
                        null, null
                },
                {GroupCountSideEffectStep.class,
                        g.V().groupCount("x"),
                        List.of(),
                        List.of(),
                        null, null
                },
                {GroupCountSideEffectStep.class,
                        g.V().groupCount("x").by("name"),
                        List.of(),
                        List.of(new ValueTraversal<>("name")),
                        null, null
                },
                {GroupCountSideEffectStep.class,
                        g.V().groupCount("x").by(__.constant("value")),
                        List.of(),
                        List.of(__.constant("value")),
                        null, null
                },
                {GroupStep.class,
                        g.V().group(),
                        List.of(),
                        List.of(__.fold()),
                        null, null
                },
                {GroupStep.class,
                        g.V().group().by("name"),
                        List.of(),
                        List.of(new ValueTraversal<>("name"), __.fold()),
                        null, null
                },
                {GroupStep.class,
                        g.V().group().by("name").by("age"),
                        List.of(),
                        List.of(new ValueTraversal<>("name"), __.map(new ValueTraversal<>("age")).fold()),
                        null, null
                },
                {GroupStep.class,
                        g.V().group().by(__.constant("key")).by(__.constant("value")),
                        List.of(),
                        List.of(__.constant("key"), __.constant("value")),
                        null, null
                },
                {IntersectStep.class,
                        g.V().fold().intersect(__.constant(List.of(1, 2, 3))),
                        List.of(),
                        List.of(__.constant(List.of(1, 2, 3))),
                        null, null
                },
                {LocalStep.class,
                        g.V().local(__.outE().count()),
                        List.of(),
                        List.of(__.outE().count()),
                        null, null
                },
                {MatchStep.class,
                        g.V().match(__.as("a").out().as("b"), __.as("b").in().as("c")),
                        List.of(generateMatchChildTraversal("a", "b", __.out().asAdmin()), generateMatchChildTraversal("b", "c", __.in().asAdmin())),
                        List.of(),
                        null, null
                },
                {MathStep.class,
                        g.V().math("_ + 1"),
                        List.of(),
                        List.of(),
                        null, null
                },
                {MathStep.class,
                        g.V().math("_ + 1").by("age"),
                        List.of(),
                        List.of(new ValueTraversal<>("age")),
                        null, null
                },
                {MathStep.class,
                        g.V().math("_ + _").by("age").by(__.constant(10)),
                        List.of(),
                        List.of(new ValueTraversal<>("age"), __.constant(10)),
                        null, null
                },
                {MergeStep.class,
                        g.V().fold().merge(__.constant(List.of(1, 2, 3))),
                        List.of(),
                        List.of(__.constant(List.of(1, 2, 3))),
                        null, null
                },
                {NotStep.class,
                        g.V().not(__.has("age", P.gt(27))),
                        List.of(),
                        List.of(__.has("age", P.gt(27))),
                        null, null
                },
                {OptionalStep.class,
                        g.V().optional(__.out("knows")),
                        List.of(),
                        List.of(__.out("knows")),
                        null, null
                },
                {OrderGlobalStep.class,
                        g.V().order(),
                        List.of(),
                        List.of(),
                        null, null
                },
                {OrderGlobalStep.class,
                        g.V().order().by("name"),
                        List.of(),
                        List.of(new ValueTraversal<>("name")),
                        null, null
                },
                {OrderGlobalStep.class,
                        g.V().order().by("name").by(__.constant("value")),
                        List.of(),
                        List.of(new ValueTraversal<>("name"), __.constant("value")),
                        null, null
                },
                {OrderLocalStep.class,
                        g.V().fold().order(Scope.local),
                        List.of(),
                        List.of(),
                        null, null
                },
                {OrderLocalStep.class,
                        g.V().fold().order(Scope.local).by("name"),
                        List.of(),
                        List.of(new ValueTraversal<>("name")),
                        null, null
                },
                {OrderLocalStep.class,
                        g.V().fold().order(Scope.local).by(__.constant("value")),
                        List.of(),
                        List.of(__.constant("value")),
                        null, null
                },
                {PathFilterStep.class,
                        g.V().simplePath(),
                        List.of(),
                        List.of(),
                        null, null
                },
                {PathFilterStep.class,
                        g.V().cyclicPath(),
                        List.of(),
                        List.of(),
                        null, null
                },
                {PathFilterStep.class,
                        g.V().simplePath().by("name"),
                        List.of(),
                        List.of(new ValueTraversal<>("name")),
                        null, null
                },
                {PathFilterStep.class,
                        g.V().simplePath().by(__.constant("value")),
                        List.of(),
                        List.of(__.constant("value")),
                        null, null
                },
                {PathFilterStep.class,
                        g.V().cyclicPath().by("age").by(__.constant("value")),
                        List.of(),
                        List.of(new ValueTraversal<>("age"), __.constant("value")),
                        null, null
                },
                {PathStep.class,
                        g.V().path(),
                        List.of(),
                        List.of(),
                        null, null
                },
                {PathStep.class,
                        g.V().out().path().by("age").by("name"),
                        List.of(),
                        List.of(new ValueTraversal<>("age"), new ValueTraversal<>("name")),
                        null, null
                },
                {PathStep.class,
                        g.V().path().by(__.constant("value")),
                        List.of(),
                        List.of(__.constant("value")),
                        null, null
                },
                {ProductStep.class,
                        g.V().fold().product(__.constant(List.of(1, 2, 3))),
                        List.of(),
                        List.of(__.constant(List.of(1, 2, 3))),
                        null, null
                },
                {ProjectStep.class,
                        g.V().project("a", "b").by("name").by("age"),
                        List.of(),
                        List.of(new ValueTraversal<>("name"), new ValueTraversal<>("age")),
                        null, null
                },
                {ProjectStep.class,
                        g.V().project("x").by(__.constant("value")),
                        List.of(),
                        List.of(__.constant("value")),
                        null, null
                },
                {PropertyMapStep.class,
                        g.V().valueMap(),
                        List.of(),
                        List.of(),
                        null, null
                },
                {PropertyMapStep.class,
                        g.V().valueMap().by(__.constant("transformed")),
                        List.of(),
                        List.of(__.constant("transformed")),
                        null, null
                },
                {RepeatStep.class,
                        g.V().repeat(__.out()).emit().times(2),
                        List.of(appendRepeatEndStep(__.out())),
                        List.of(new LoopTraversal<>(2), TrueTraversal.instance()),
                        null, null
                },
                {RepeatStep.class,
                        g.V().repeat(__.out()).until(__.has("name", "josh")),
                        List.of(appendRepeatEndStep(__.out())),
                        List.of(__.has("name", "josh")),
                        null, null
                },
                {RepeatStep.class,
                        g.V().repeat(__.out()).emit(__.has("age")).until(__.has("name", "josh")),
                        List.of(appendRepeatEndStep(__.out())),
                        List.of(__.has("name", "josh"), __.has("age")),
                        null, List.of(__.has("name", "josh"), __.filter(__.properties("age"))),
                },
                {SackValueStep.class,
                        g.withSack(0).V().sack(Operator.sum),
                        List.of(),
                        List.of(),
                        null, null
                },
                {SackValueStep.class,
                        g.withSack(0).V().sack(Operator.sum).by("age"),
                        List.of(),
                        List.of(new ValueTraversal<>("age")),
                        null, null
                },
                {SackValueStep.class,
                        g.withSack(0).V().sack(Operator.sum).by(__.constant("age")),
                        List.of(),
                        List.of(__.constant("age")),
                        null, null
                },
                {SampleGlobalStep.class,
                        g.V().sample(2),
                        List.of(),
                        List.of(new ConstantTraversal<>(1.0)),
                        null, null
                },
                {SampleGlobalStep.class,
                        g.E().sample(2).by("weight"),
                        List.of(),
                        List.of(new ValueTraversal<>("weight")),
                        null, null
                },
                {SampleGlobalStep.class,
                        g.E().sample(2).by(__.constant("weight")),
                        List.of(),
                        List.of(__.constant("weight")),
                        null, null
                },
                {SelectOneStep.class,
                        g.V().as("a").select("a"),
                        List.of(),
                        List.of(),
                        null, null
                },
                {SelectOneStep.class,
                        g.V().as("a").select("a").by("name"),
                        List.of(),
                        List.of(new ValueTraversal<>("name")),
                        null, null
                },
                {SelectOneStep.class,
                        g.V().as("a").select("a").by(__.constant("value")),
                        List.of(),
                        List.of(__.constant("value")),
                        null, null
                },
                {SelectStep.class,
                        g.V().as("a").as("b").select("a", "b"),
                        List.of(),
                        List.of(),
                        null, null
                },
                {SelectStep.class,
                        g.V().as("a").as("b").select("a", "b").by("name"),
                        List.of(),
                        List.of(new ValueTraversal<>("name")),
                        null, null
                },
                {SelectStep.class,
                        g.V().as("a").as("b").select("a", "b").by(__.constant("value")),
                        List.of(),
                        List.of(__.constant("value")),
                        null, null
                },
                {TraversalSelectStep.class,
                        g.V().select((Traversal)__.constant("a")),
                        List.of(),
                        List.of(__.constant("a")),
                        null, null
                },
                {TraversalSelectStep.class,
                        g.V().select((Traversal)__.constant("a")).by("name"),
                        List.of(),
                        List.of(__.constant("a"), new ValueTraversal<>("name")),
                        null, null
                },
                {TraversalFilterStep.class,
                        g.V().filter(__.constant(5)), // a bit of an arbitrary case to avoid getting optimized by strategies
                        List.of(),
                        List.of(__.constant(5)),
                        null, null
                },
                {TraversalFlatMapStep.class,
                        g.V().flatMap(__.out()),
                        List.of(),
                        List.of(__.out()),
                        null, null
                },
                {TraversalFlatMapStep.class,
                        g.V().flatMap(__.constant("value")),
                        List.of(),
                        List.of(__.constant("value")),
                        null, null
                },
                {TraversalMapStep.class,
                        g.V().map(__.values("name")),
                        List.of(),
                        List.of(__.values("name")),
                        null, null
                },
                {TraversalMapStep.class,
                        g.V().map(__.constant("value")),
                        List.of(),
                        List.of(__.constant("value")),
                        null, null
                },
                {TraversalSideEffectStep.class,
                        g.V().sideEffect(__.identity()),
                        List.of(),
                        List.of(__.identity()),
                        null, null
                },
                {TraversalSideEffectStep.class,
                        g.V().sideEffect(__.constant("value")),
                        List.of(),
                        List.of(__.constant("value")),
                        null, null
                },
                {TreeStep.class,
                        g.V().out().tree(),
                        List.of(),
                        List.of(),
                        null, null
                },
                {TreeStep.class,
                        g.V().out().tree().by("name"),
                        List.of(),
                        List.of(new ValueTraversal<>("name")),
                        null, null
                },
                {TreeStep.class,
                        g.V().out().tree().by(__.constant("value")),
                        List.of(),
                        List.of(__.constant("value")),
                        null, null
                },
                {TreeSideEffectStep.class,
                        g.V().out().tree("x"),
                        List.of(),
                        List.of(),
                        null, null
                },
                {TreeSideEffectStep.class,
                        g.V().out().tree("x").by("name"),
                        List.of(),
                        List.of(new ValueTraversal<>("name")),
                        null, null
                },
                {TreeSideEffectStep.class,
                        g.V().out().tree("x").by(__.constant("value")),
                        List.of(),
                        List.of(__.constant("value")),
                        null, null
                },
                {UnionStep.class,
                        g.union(__.V().values("name"), __.V().values("age")),
                        List.of(appendComputerAwareEndStep(__.V().values("name")), appendComputerAwareEndStep(__.V().values("age"))),
                        List.of(new ConstantTraversal<>(Pick.any)),
                        List.of(appendComputerAwareEndStep(__.V().values("name").barrier(2500)), appendComputerAwareEndStep(__.V().values("age").barrier(2500))),
                        null
                },
                {UnionStep.class,
                        g.union(__.constant("a"), __.constant("b")),
                        List.of(appendComputerAwareEndStep(__.constant("a")), appendComputerAwareEndStep(__.constant("b"))),
                        List.of(new ConstantTraversal<>(Pick.any)),
                        null, null
                },
                {WherePredicateStep.class,
                        g.V().as("a").out().as("b").where("a", P.eq("b")),
                        List.of(),
                        List.of(),
                        null, null
                },
                {WherePredicateStep.class,
                        g.V().as("a").out().as("b").where("a", P.eq("b")).by("name"),
                        List.of(),
                        List.of(new ValueTraversal<>("name")),
                        null, null
                },
                {WherePredicateStep.class,
                        g.V().as("a").out().as("b").where("a", P.eq("b")).by(__.constant("value")),
                        List.of(),
                        List.of(__.constant("value")),
                        null, null
                },
                {WhereTraversalStep.class,
                        g.V().as("a").out().as("b").where(__.as("a").out().as("c")),
                        List.of(),
                        List.of(generateWhereChildTraversal("a", "c", __.out().asAdmin())),
                        null, null
                },
        });
    }

    @Test
    public void doTest() {
        verifyExpectedParents(expectedGlobalChildren, expectedLocalChildren, "(pre-strategy application)");

        traversal = traversal.clone();

        verifyExpectedParents(expectedGlobalChildren, expectedLocalChildren, "(cloned traversal, pre-strategy application)");

        traversal.getStrategies().addStrategies(GValueReductionStrategy.instance());
        traversal.applyStrategies();

        verifyExpectedParents(postStrategyExpectedGlobalChildren == null ? expectedGlobalChildren : postStrategyExpectedGlobalChildren,
                postStrategyExpectedLocalChildren == null ? expectedLocalChildren : postStrategyExpectedLocalChildren,
                "(pre-strategy application)");

        traversal = traversal.clone();

        verifyExpectedParents(postStrategyExpectedGlobalChildren == null ? expectedGlobalChildren : postStrategyExpectedGlobalChildren,
                postStrategyExpectedLocalChildren == null ? expectedLocalChildren : postStrategyExpectedLocalChildren,
                "(cloned traversal, pre-strategy application)");
    }

    private void verifyExpectedParents(List<Traversal.Admin<?,?>> expectedGlobalChildren, List<Traversal.Admin<?,?>> expectedLocalChildren, String messageSuffix) {
        List<Step<?,?>> steps = TraversalHelper.getStepsOfAssignableClass(stepClass, traversal);
        assertTrue(String.format("Expected a single step of class %s to test, found %d for Traversal [%s] %s", stepClass.getName(), steps.size(), traversal, messageSuffix), steps.size() == 1);
        assertTrue(String.format("Expected step to implement TraversalParent for Traversal [%s] %s", traversal, messageSuffix), steps.get(0) instanceof TraversalParent);

        TraversalParent testStep = (TraversalParent) steps.get(0);
        assertThat(String.format("getGlobalChildren() did not produce the expected results for Traversal [%s] %s", traversal, messageSuffix), testStep.getGlobalChildren(), containsInAnyOrder(expectedGlobalChildren.toArray()));
        assertThat(String.format("getLocalChildren() did not produce the expected results for Traversal [%s] %s", traversal, messageSuffix), testStep.getLocalChildren(), containsInAnyOrder(expectedLocalChildren.toArray()));

        for (Traversal.Admin<?, ?> child : testStep.getGlobalChildren()) {
            if (!(child instanceof AbstractLambdaTraversal)) {
                // Use reference equality to ensure parent points to the correct object. It's not enough that the parents contents match.
                assertTrue(String.format("Expected child traversal %s to have correctly assigned parent for Traversal [%s] %s", child.toString(), traversal, messageSuffix), testStep == child.getParent());
            }
        }
        for (Traversal.Admin<?, ?> child : testStep.getLocalChildren()) {
            if (!(child instanceof AbstractLambdaTraversal)) {
                // Use reference equality to ensure parent points to the correct object. It's not enough that the parents contents match.
                assertTrue(String.format("Expected child traversal %s to have correctly assigned parent for Traversal [%s] %s", child.toString(), traversal, messageSuffix), testStep == child.getParent());
            }
        }
    }

    private static Traversal<?, ?> appendComputerAwareEndStep(final Traversal<?, ?> traversal) {
        traversal.asAdmin().addStep(new ComputerAwareStep.EndStep<>(traversal.asAdmin()));
        return traversal;
    }

    private static Traversal<?, ?> appendRepeatEndStep(final Traversal<?, ?> traversal) {
        traversal.asAdmin().addStep(new RepeatStep.RepeatEndStep<>(traversal.asAdmin()));
        return traversal;
    }

    private static Traversal<?, ?> generateMatchChildTraversal(final String start, final String end, final Traversal.Admin<?,?> traversal) {
        traversal.addStep(0, new MatchStep.MatchStartStep(traversal, start));
        traversal.addStep(new MatchStep.MatchEndStep(traversal, end));
        return traversal;
    }

    private static Traversal<?, ?> generateWhereChildTraversal(final String start, final String end, final Traversal.Admin<?,?> traversal) {
        traversal.addStep(0, new WhereTraversalStep.WhereStartStep<>(traversal, start));
        traversal.addStep(new WhereTraversalStep.WhereEndStep(traversal, end));
        return traversal;
    }
}
