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
package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.TestDataBuilder;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.PopContaining;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.LambdaFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.PathFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalFlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.path;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.repeat;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.union;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.valueMap;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TraversalHelperTest {

    @Test
    public void shouldSetPreviousStepToEmptyStep() {
        final Traversal.Admin<?, ?> traversal = __.V().out().asAdmin();
        //transform the traversal to __.V().not(out())
        //the VertexStep's previousStep should be the EmptyStep
        Optional<VertexStep> vertexStepOpt = TraversalHelper.getFirstStepOfAssignableClass(VertexStep.class, traversal);
        assertTrue(vertexStepOpt.isPresent());
        Traversal.Admin<?,?> inner = __.start().asAdmin();
        inner.addStep(0, vertexStepOpt.get());
        TraversalHelper.replaceStep(vertexStepOpt.get(), new NotStep<>(__.identity().asAdmin(), inner), traversal);
        List<VertexStep> vertexSteps = TraversalHelper.getStepsOfAssignableClassRecursively(VertexStep.class, traversal);
        assertEquals(1, vertexSteps.size());
        VertexStep vertexStep = vertexSteps.get(0);
        assertTrue("Expected the previousStep to be an EmptyStep, found instead " + vertexStep.getPreviousStep().toString(),vertexStep.getPreviousStep() == EmptyStep.instance());
    }

    @Test
    public void shouldIdentifyLocalChildren() {
        final Traversal.Admin<?, ?> localChild = __.as("x").select("a", "b").by("name").asAdmin();
        new LocalStep<>(new DefaultTraversal(), localChild);
        assertFalse(TraversalHelper.isGlobalChild(localChild));
        ///
        new WhereTraversalStep<>(new DefaultTraversal(), localChild);
        assertFalse(TraversalHelper.isGlobalChild(localChild));
        ///
        new TraversalFilterStep<>(new DefaultTraversal(), localChild);
        assertFalse(TraversalHelper.isGlobalChild(localChild));
        ///
        new TraversalMapStep<>(new DefaultTraversal(), localChild);
        assertFalse(TraversalHelper.isGlobalChild(localChild));
        ///
        new TraversalFlatMapStep<>(new DefaultTraversal(), localChild);
        assertFalse(TraversalHelper.isGlobalChild(localChild));
        ///
        final Traversal.Admin<?, ?> remoteLocalChild = __.repeat(localChild).asAdmin();
        new LocalStep<>(new DefaultTraversal<>(), remoteLocalChild);
        assertFalse(TraversalHelper.isGlobalChild(localChild));
    }

    @Test
    public void shouldIdentifyGlobalChildren() {
        final Traversal.Admin<?, ?> globalChild = __.select("a", "b").by("name").asAdmin();
        TraversalParent parent = new RepeatStep<>(new DefaultTraversal());
        ((RepeatStep) parent).setRepeatTraversal(globalChild);
        assertTrue(TraversalHelper.isGlobalChild(globalChild));
        ///
        new UnionStep<>(new DefaultTraversal(), globalChild);
        assertTrue(TraversalHelper.isGlobalChild(globalChild));
        ///
        new TraversalVertexProgramStep(new DefaultTraversal<>(), globalChild);
        assertTrue(TraversalHelper.isGlobalChild(globalChild));
        ///
        final Traversal.Admin<?, ?> remoteRemoteChild = __.repeat(globalChild).asAdmin();
        new UnionStep<>(new DefaultTraversal(), remoteRemoteChild);
        assertTrue(TraversalHelper.isGlobalChild(globalChild));
    }

    @Test
    public void shouldIdentifyLocalProperties() {
        assertTrue(TraversalHelper.isLocalProperties(__.identity().asAdmin()));
        assertTrue(TraversalHelper.isLocalProperties(__.id().asAdmin()));
        assertTrue(TraversalHelper.isLocalProperties(__.label().asAdmin()));
        assertTrue(TraversalHelper.isLocalProperties(__.values("name").asAdmin()));
        assertFalse(TraversalHelper.isLocalProperties(outE("knows").asAdmin()));
    }

    @Test
    public void shouldNotFindStepOfClassInTraversal() {
        final Traversal.Admin traversal = new DefaultTraversal<>(EmptyGraph.instance());
        traversal.asAdmin().addStep(0, new HasStep(traversal));
        traversal.asAdmin().addStep(0, new HasStep(traversal));
        traversal.asAdmin().addStep(0, new HasStep(traversal));

        assertThat(TraversalHelper.hasStepOfClass(FilterStep.class, traversal), is(false));
    }

    @Test
    public void shouldFindStepOfClassInTraversal() {
        final Traversal.Admin traversal = new DefaultTraversal<>(EmptyGraph.instance());
        traversal.asAdmin().addStep(0, new HasStep(traversal));
        traversal.asAdmin().addStep(0, new IdentityStep<>(traversal));
        traversal.asAdmin().addStep(0, new HasStep(traversal));

        assertThat(TraversalHelper.hasStepOfClass(IdentityStep.class, traversal), is(true));
    }

    @Test
    public void shouldNotFindStepOfAssignableClassInTraversal() {
        final Traversal.Admin traversal = new DefaultTraversal<>(EmptyGraph.instance());
        traversal.asAdmin().addStep(0, new HasStep(traversal));
        traversal.asAdmin().addStep(0, new HasStep(traversal));
        traversal.asAdmin().addStep(0, new HasStep(traversal));

        assertThat(TraversalHelper.hasStepOfAssignableClass(IdentityStep.class, traversal), is(false));
    }

    @Test
    public void shouldFindStepOfAssignableClassInTraversal() {
        final Traversal.Admin traversal = new DefaultTraversal<>(EmptyGraph.instance());
        traversal.asAdmin().addStep(0, new HasStep(traversal));
        traversal.asAdmin().addStep(0, new HasStep(traversal));
        traversal.asAdmin().addStep(0, new HasStep(traversal));

        assertThat(TraversalHelper.hasStepOfAssignableClass(FilterStep.class, traversal), is(true));
    }

    @Test
    public void shouldGetTheStepIndex() {
        final Traversal.Admin traversal = new DefaultTraversal<>(EmptyGraph.instance());
        final HasStep hasStep = new HasStep(traversal);
        traversal.asAdmin().addStep(0, new HasStep(traversal));
        traversal.asAdmin().addStep(0, hasStep);
        traversal.asAdmin().addStep(0, new HasStep(traversal));

        assertEquals(1, TraversalHelper.stepIndex(hasStep, traversal));
    }

    @Test
    public void shouldNotFindTheStepIndex() {
        final Traversal.Admin traversal = new DefaultTraversal<>(EmptyGraph.instance());
        final IdentityStep identityStep = new IdentityStep(traversal);
        traversal.asAdmin().addStep(0, new HasStep(traversal));
        traversal.asAdmin().addStep(0, new HasStep(traversal));
        traversal.asAdmin().addStep(0, new HasStep(traversal));

        assertEquals(-1, TraversalHelper.stepIndex(identityStep, traversal));
    }

    @Test
    public void shouldInsertBeforeStep() {
        final Traversal.Admin traversal = new DefaultTraversal<>(EmptyGraph.instance());
        final HasStep hasStep = new HasStep(traversal);
        final IdentityStep identityStep = new IdentityStep(traversal);
        traversal.asAdmin().addStep(0, new HasStep(traversal));
        traversal.asAdmin().addStep(0, hasStep);
        traversal.asAdmin().addStep(0, new HasStep(traversal));

        TraversalHelper.insertBeforeStep(identityStep, hasStep, traversal);

        assertEquals(traversal.asAdmin().getSteps().get(1), identityStep);
        assertEquals(4, traversal.asAdmin().getSteps().size());
    }

    @Test
    public void shouldInsertAfterStep() {
        final Traversal.Admin traversal = new DefaultTraversal<>(EmptyGraph.instance());
        final HasStep hasStep = new HasStep(traversal);
        final IdentityStep identityStep = new IdentityStep(traversal);
        traversal.asAdmin().addStep(0, new HasStep(traversal));
        traversal.asAdmin().addStep(0, hasStep);
        traversal.asAdmin().addStep(0, new HasStep(traversal));

        TraversalHelper.insertAfterStep(identityStep, hasStep, traversal);

        assertEquals(traversal.asAdmin().getSteps().get(2), identityStep);
        assertEquals(4, traversal.asAdmin().getSteps().size());
    }

    @Test
    public void shouldReplaceStep() {
        final Traversal.Admin traversal = new DefaultTraversal<>(EmptyGraph.instance());
        final HasStep hasStep = new HasStep(traversal);
        final IdentityStep identityStep = new IdentityStep(traversal);
        traversal.asAdmin().addStep(0, new HasStep(traversal));
        traversal.asAdmin().addStep(0, hasStep);
        traversal.asAdmin().addStep(0, new HasStep(traversal));

        TraversalHelper.replaceStep(hasStep, identityStep, traversal);

        assertEquals(traversal.asAdmin().getSteps().get(1), identityStep);
        assertEquals(3, traversal.asAdmin().getSteps().size());
    }

    @Test
    public void shouldChainTogetherStepsWithNextPreviousInALinkedListStructure() {
        final Traversal.Admin traversal = new DefaultTraversal<>(EmptyGraph.instance());
        traversal.asAdmin().addStep(new IdentityStep(traversal));
        traversal.asAdmin().addStep(new HasStep(traversal));
        traversal.asAdmin().addStep(new LambdaFilterStep(traversal, traverser -> true));
        validateToyTraversal(traversal);
    }

    @Test
    public void shouldAddStepsCorrectly() {
        Traversal.Admin traversal = new DefaultTraversal<>(EmptyGraph.instance());
        traversal.asAdmin().addStep(0, new LambdaFilterStep(traversal, traverser -> true));
        traversal.asAdmin().addStep(0, new HasStep(traversal));
        traversal.asAdmin().addStep(0, new IdentityStep(traversal));
        validateToyTraversal(traversal);

        traversal = new DefaultTraversal<>(EmptyGraph.instance());
        traversal.asAdmin().addStep(0, new IdentityStep(traversal));
        traversal.asAdmin().addStep(1, new HasStep(traversal));
        traversal.asAdmin().addStep(2, new LambdaFilterStep(traversal, traverser -> true));
        validateToyTraversal(traversal);
    }

    @Test
    public void shouldRemoveStepsCorrectly() {
        final Traversal.Admin traversal = new DefaultTraversal<>(EmptyGraph.instance());
        traversal.asAdmin().addStep(new IdentityStep(traversal));
        traversal.asAdmin().addStep(new HasStep(traversal));
        traversal.asAdmin().addStep(new LambdaFilterStep(traversal, traverser -> true));

        traversal.asAdmin().addStep(new PropertiesStep(traversal, PropertyType.VALUE, "marko"));
        traversal.asAdmin().removeStep(3);
        validateToyTraversal(traversal);

        traversal.asAdmin().addStep(0, new PropertiesStep(traversal, PropertyType.PROPERTY, "marko"));
        traversal.asAdmin().removeStep(0);
        validateToyTraversal(traversal);

        traversal.asAdmin().removeStep(1);
        traversal.asAdmin().addStep(1, new HasStep(traversal));
        validateToyTraversal(traversal);
    }

    private static void validateToyTraversal(final Traversal traversal) {
        assertEquals(traversal.asAdmin().getSteps().size(), 3);

        assertEquals(IdentityStep.class, traversal.asAdmin().getSteps().get(0).getClass());
        assertEquals(HasStep.class, traversal.asAdmin().getSteps().get(1).getClass());
        assertEquals(LambdaFilterStep.class, traversal.asAdmin().getSteps().get(2).getClass());

        // IDENTITY STEP
        assertEquals(EmptyStep.class, ((Step) traversal.asAdmin().getSteps().get(0)).getPreviousStep().getClass());
        assertEquals(HasStep.class, ((Step) traversal.asAdmin().getSteps().get(0)).getNextStep().getClass());
        assertEquals(LambdaFilterStep.class, ((Step) traversal.asAdmin().getSteps().get(0)).getNextStep().getNextStep().getClass());
        assertEquals(EmptyStep.class, ((Step) traversal.asAdmin().getSteps().get(0)).getNextStep().getNextStep().getNextStep().getClass());

        // HAS STEP
        assertEquals(IdentityStep.class, ((Step) traversal.asAdmin().getSteps().get(1)).getPreviousStep().getClass());
        assertEquals(EmptyStep.class, ((Step) traversal.asAdmin().getSteps().get(1)).getPreviousStep().getPreviousStep().getClass());
        assertEquals(LambdaFilterStep.class, ((Step) traversal.asAdmin().getSteps().get(1)).getNextStep().getClass());
        assertEquals(EmptyStep.class, ((Step) traversal.asAdmin().getSteps().get(1)).getNextStep().getNextStep().getClass());

        // FILTER STEP
        assertEquals(HasStep.class, ((Step) traversal.asAdmin().getSteps().get(2)).getPreviousStep().getClass());
        assertEquals(IdentityStep.class, ((Step) traversal.asAdmin().getSteps().get(2)).getPreviousStep().getPreviousStep().getClass());
        assertEquals(EmptyStep.class, ((Step) traversal.asAdmin().getSteps().get(2)).getPreviousStep().getPreviousStep().getPreviousStep().getClass());
        assertEquals(EmptyStep.class, ((Step) traversal.asAdmin().getSteps().get(2)).getNextStep().getClass());

        assertEquals(3, traversal.asAdmin().getSteps().size());
    }

    @Test
    public void shouldTruncateLongName() {
        Step s = Mockito.mock(Step.class);
        Mockito.when(s.toString()).thenReturn("0123456789");
        assertEquals("0123...", TraversalHelper.getShortName(s, 7));
    }

    @Test
    public void shouldIdentifyStarGraphTraversals() {
        assertTrue(TraversalHelper.isLocalStarGraph(__.identity().asAdmin()));
        assertTrue(TraversalHelper.isLocalStarGraph(__.id().asAdmin()));
        assertTrue(TraversalHelper.isLocalStarGraph(__.out().asAdmin()));
        assertTrue(TraversalHelper.isLocalStarGraph(__.label().asAdmin()));
        assertTrue(TraversalHelper.isLocalStarGraph(__.bothE().asAdmin()));
        assertTrue(TraversalHelper.isLocalStarGraph(__.values().asAdmin()));
        assertTrue(TraversalHelper.isLocalStarGraph(__.properties().asAdmin()));
        assertTrue(TraversalHelper.isLocalStarGraph(__.repeat(__.identity()).asAdmin()));
        assertTrue(TraversalHelper.isLocalStarGraph(__.repeat(__.has("name")).asAdmin()));
        assertTrue(TraversalHelper.isLocalStarGraph(__.out().repeat(__.identity()).asAdmin()));
        assertTrue(TraversalHelper.isLocalStarGraph(__.out().id().asAdmin()));
        assertTrue(TraversalHelper.isLocalStarGraph(__.label().union(__.out(), in()).asAdmin()));
        assertTrue(TraversalHelper.isLocalStarGraph(__.label().union(__.out(), in()).id().asAdmin()));
        assertTrue(TraversalHelper.isLocalStarGraph(__.coalesce(out("likes"), out("knows"), out("created")).groupCount().asAdmin()));
        assertTrue(TraversalHelper.isLocalStarGraph(__.local(__.out()).groupCount().asAdmin()));
        assertTrue(TraversalHelper.isLocalStarGraph(__.local(__.out()).groupCount().by(T.id).asAdmin()));
        // assertTrue(TraversalHelper.isLocalStarGraph(__.out().repeat(__.has("name")).asAdmin()));
        //
        assertFalse(TraversalHelper.isLocalStarGraph(__.out().label().asAdmin()));
        assertFalse(TraversalHelper.isLocalStarGraph(__.out().values().asAdmin()));
        assertFalse(TraversalHelper.isLocalStarGraph(__.out().valueMap().asAdmin()));
        assertFalse(TraversalHelper.isLocalStarGraph(__.repeat(__.out()).asAdmin()));
        assertFalse(TraversalHelper.isLocalStarGraph(__.repeat(__.has("name").out()).asAdmin()));
        assertFalse(TraversalHelper.isLocalStarGraph(__.repeat(__.has("name").union(__.out(), in())).asAdmin()));
        assertFalse(TraversalHelper.isLocalStarGraph(__.union(__.out(), in()).label().asAdmin()));
        assertFalse(TraversalHelper.isLocalStarGraph(__.union(__.out(), in().out()).asAdmin()));
        assertFalse(TraversalHelper.isLocalStarGraph(__.union(__.out(), __.out().union(in(), __.out())).asAdmin()));
        assertFalse(TraversalHelper.isLocalStarGraph(__.union(__.values(), __.out().union(in(), __.out())).out().asAdmin()));
        assertFalse(TraversalHelper.isLocalStarGraph(__.coalesce(out("likes"), out("knows"), out("created")).groupCount().by("name").asAdmin()));
        assertFalse(TraversalHelper.isLocalStarGraph(__.local(__.out()).groupCount().by("name").asAdmin()));
    }

    @Test
    public void shouldGetStepsByClass() {
        Set<String> labels = (Set) TraversalHelper.getStepsOfClass(VertexStep.class, __.out().as("a").values("name").as("b").in().as("c").groupCount().as("d").asAdmin())
                .stream()
                .flatMap(s -> s.getLabels().stream())
                .collect(Collectors.toSet());
        assertEquals(2, labels.size());
        assertTrue(labels.contains("a"));
        assertTrue(labels.contains("c"));
        //
        labels = (Set) TraversalHelper.getStepsOfAssignableClass(VertexStep.class, __.out().as("a").values("name").as("b").in().as("c").groupCount().as("d").asAdmin())
                .stream()
                .flatMap(s -> s.getLabels().stream())
                .collect(Collectors.toSet());
        assertEquals(2, labels.size());
        assertTrue(labels.contains("a"));
        assertTrue(labels.contains("c"));
        //
        labels = (Set) TraversalHelper.getStepsOfAssignableClass(FlatMapStep.class, __.out().as("a").values("name").as("b").in().as("c").groupCount().as("d").asAdmin())
                .stream()
                .flatMap(s -> s.getLabels().stream())
                .collect(Collectors.toSet());
        assertEquals(3, labels.size());
        assertTrue(labels.contains("a"));
        assertTrue(labels.contains("b"));
        assertTrue(labels.contains("c"));
        //
        labels = (Set) TraversalHelper.getStepsOfClass(Step.class, __.out().as("a").values("name").as("b").in().as("c").groupCount().as("d").asAdmin())
                .stream()
                .flatMap(s -> s.getLabels().stream())
                .collect(Collectors.toSet());
        assertEquals(0, labels.size());
        //
        labels = (Set) TraversalHelper.getStepsOfAssignableClass(Step.class, __.out().as("a").values("name").as("b").in().as("c").groupCount().as("d").asAdmin())
                .stream()
                .flatMap(s -> s.getLabels().stream())
                .collect(Collectors.toSet());
        assertEquals(4, labels.size());
        assertTrue(labels.contains("a"));
        assertTrue(labels.contains("b"));
        assertTrue(labels.contains("c"));
        assertTrue(labels.contains("d"));
    }

    @Test
    public void shouldGetLabels() {
        Set<String> labels = (Set) TraversalHelper.getLabels(__.out().as("a").values("name").as("b").in().as("c").groupCount().as("d").asAdmin());
        assertEquals(4, labels.size());
        assertTrue(labels.contains("a"));
        assertTrue(labels.contains("b"));
        assertTrue(labels.contains("c"));
        assertTrue(labels.contains("d"));
        labels = (Set) TraversalHelper.getLabels(__.out().as("a").repeat(__.out("name").as("b")).local(in().as("c")).as("d").groupCount().by(outE().as("e")).as("f").asAdmin());
        assertEquals(6, labels.size());
        assertTrue(labels.contains("a"));
        assertTrue(labels.contains("b"));
        assertTrue(labels.contains("c"));
        assertTrue(labels.contains("d"));
        assertTrue(labels.contains("e"));
        assertTrue(labels.contains("f"));
    }

    @Test
    public void shouldFindStepsRecursively() {
        final Traversal<?,?> traversal = __.V().repeat(__.out().simplePath());
        assertThat(TraversalHelper.anyStepRecursively(s -> s instanceof PathFilterStep, traversal.asAdmin()), is(true));
    }

    @Test
    public void shouldGetStepsOfAssignableClassRecursivelyNoTypes() {
        final Traversal.Admin<?,?> traversal = __.V().repeat(__.out()).project("x").by(out().in().fold()).asAdmin();
        final List<Step<?,?>> steps = TraversalHelper.getStepsOfAssignableClassRecursively(traversal);
        assertEquals(0, steps.size());
    }

    @Test
    public void shouldGetStepsOfAssignableClassRecursivelyOneType() {
        final Traversal.Admin<?,?> traversal = __.V().repeat(__.out()).project("x").by(out().in().fold()).asAdmin();
        final List<Step<?,?>> steps = TraversalHelper.getStepsOfAssignableClassRecursively(traversal, VertexStep.class);
        assertEquals(3, steps.size());
        assertThat(steps.stream().allMatch(s -> s instanceof VertexStep), is(true));
    }

    @Test
    public void shouldGetStepsOfAssignableClassRecursivelyMultipleTypes() {
        final Traversal.Admin<?,?> traversal = __.V().repeat(__.out()).project("x").by(out().in().fold()).asAdmin();
        final List<Step<?,?>> steps = TraversalHelper.getStepsOfAssignableClassRecursively(traversal, VertexStep.class, FoldStep.class);
        assertEquals(4, steps.size());
        assertEquals(3, steps.stream().filter(s -> s instanceof VertexStep).count());
        assertEquals(1, steps.stream().filter(s -> s instanceof FoldStep).count());
    }

    @Test
    public void shouldGetStepsOfAssignableClassRecursivelyFromDepthNoTypes() {
        final Traversal.Admin<?,?> traversal = __.V().repeat(__.out()).project("x").by(out().in().fold()).asAdmin();
        final List<Step<?,?>> steps = TraversalHelper.getStepsOfAssignableClassRecursivelyFromDepth(traversal);
        assertEquals(0, steps.size());
    }

    @Test
    public void shouldGetStepsOfAssignableClassRecursivelyFromDepthOneType() {
        final Traversal.Admin<?,?> traversal = __.V().repeat(__.out()).project("x").by(out().in().fold()).asAdmin();
        final List<Step<?,?>> steps = TraversalHelper.getStepsOfAssignableClassRecursivelyFromDepth(traversal, VertexStep.class);
        assertEquals(3, steps.size());
        assertThat(steps.stream().allMatch(s -> s instanceof VertexStep), is(true));
    }

    @Test
    public void shouldGetStepsOfAssignableClassRecursivelyFromDepthMultipleTypes() {
        final Traversal.Admin<?,?> traversal = __.V().repeat(__.out()).project("x").by(out().in().fold()).asAdmin();
        final List<Step<?,?>> steps = TraversalHelper.getStepsOfAssignableClassRecursivelyFromDepth(traversal, VertexStep.class, FoldStep.class);
        assertEquals(4, steps.size());
        assertEquals(3, steps.stream().filter(s -> s instanceof VertexStep).count());
        assertEquals(1, steps.stream().filter(s -> s instanceof FoldStep).count());
    }

    @Test
    public void shouldGetStepsOfAssignableClassRecursivelyFromDepthEnsureOrder() {
        final Traversal.Admin<?,?> traversal = __.V().union(
                __.union(__.values("a"),
                         __.union(__.values("b"), __.union(__.values("c"))),
                         __.values("d")),
                __.values("e")).values("f").asAdmin();
        final List<Step<?,?>> steps = TraversalHelper.getStepsOfAssignableClassRecursivelyFromDepth(traversal, PropertiesStep.class);
        assertEquals(6, steps.size());
        assertEquals("c", ((PropertiesStep) steps.get(0)).getPropertyKeys()[0]);
        assertEquals("b", ((PropertiesStep) steps.get(1)).getPropertyKeys()[0]);
        assertEquals("d", ((PropertiesStep) steps.get(2)).getPropertyKeys()[0]);
        assertEquals("a", ((PropertiesStep) steps.get(3)).getPropertyKeys()[0]);
        assertEquals("e", ((PropertiesStep) steps.get(4)).getPropertyKeys()[0]);
        assertEquals("f", ((PropertiesStep) steps.get(5)).getPropertyKeys()[0]);
    }

    @Test
    public void shouldGetPopInstructions() {
        final List<Traversal.Admin<?,?>> traversals = new ArrayList<>();
        final List<Set<PopContaining.PopInstruction>> expectedResults = new ArrayList<>();

        ///
        traversals.add(__.V().has("person", "name", "marko").as("start").repeat(out().as("reached").select("start")).times(2).select("reached").asAdmin());
        expectedResults.add(TestDataBuilder.createPopInstructionSet(
                new Object[]{"start", Pop.last},
                new Object[]{"reached", Pop.last}
        ));
        ///
        traversals.add(__.V().select("vertex").asAdmin());
        expectedResults.add(TestDataBuilder.createPopInstructionSet(
                new Object[]{"vertex", Pop.last}
        ));
        ///
        traversals.add(__.V().out().as("a").repeat(union(out().select("a"), path().select(Pop.mixed, "b"))).select(Pop.first,"c").asAdmin());
        expectedResults.add(TestDataBuilder.createPopInstructionSet(
                new Object[]{"a", Pop.last},
                new Object[]{"b", Pop.mixed},
                new Object[]{"c", Pop.first}
        ));
        ///
        traversals.add(__.V().as("b").repeat(select("b").out().as("a")).times(2).select(Pop.first, "a").select(Pop.last, "a").project("bb").by(__.select(Pop.all, "a")).asAdmin());
        expectedResults.add(TestDataBuilder.createPopInstructionSet(
                new Object[]{"b", Pop.last},
                new Object[]{"a", Pop.first},
                new Object[]{"a", Pop.all},
                new Object[]{"a", Pop.last}
        ));
        ///
        traversals.add(__.V("1").as("b").repeat(select("b").out().as("a")).times(2).path().as("c").by("name").asAdmin());
        expectedResults.add(TestDataBuilder.createPopInstructionSet(
                new Object[]{"b", Pop.last}
        ));
        ///
        traversals.add(__.V().union(out().as("a"), repeat(out().as("a")).emit()).select(Pop.last, "a").asAdmin());
        expectedResults.add(TestDataBuilder.createPopInstructionSet(
                new Object[]{"a", Pop.last}
        ));
        ///
        traversals.add(__.V().has("person", "name", "marko").as("start").repeat(out()).times(2).where(P.neq("start")).values("name").asAdmin());
        expectedResults.add(TestDataBuilder.createPopInstructionSet(
                new Object[]{"start", Pop.last}
        ));
        ///
        traversals.add(__.V().union(out(), repeat(out().as("a")).emit()).select(Pop.last, "a").asAdmin());
        expectedResults.add(TestDataBuilder.createPopInstructionSet(
                new Object[]{"a", Pop.last}
        ));
        ///
        traversals.add(__.V().as("a").union(path(), repeat(out().select(Pop.last, "a"))).asAdmin());
        expectedResults.add(TestDataBuilder.createPopInstructionSet(
                new Object[]{"a", Pop.last}
        ));
        ///
        traversals.add(__.V().hasLabel("person").repeat(out("created")).emit().as("software").select("software").values("name", "lang").asAdmin());
        expectedResults.add(TestDataBuilder.createPopInstructionSet(
                new Object[]{"software", Pop.last}
        ));
        ///
        traversals.add(__.V().hasLabel("person").repeat(out("created").as("created_thing")).emit().as("final").select(Pop.mixed, "created_thing", "final"). by("name").by("lang").asAdmin());
        expectedResults.add(TestDataBuilder.createPopInstructionSet(
                new Object[]{"created_thing", Pop.mixed},
                new Object[]{"final", Pop.mixed}
        ));
        ///
        traversals.add(__.V().has("person", "name", "marko").as("start").repeat(out().as("path_element")).until(has("lang")).as("software").select("start", "path_element", "software").by("name").by("name").by(valueMap("name", "lang")).asAdmin());
        expectedResults.add(TestDataBuilder.createPopInstructionSet(
                new Object[]{"start", Pop.last},
                new Object[]{"path_element", Pop.last},
                new Object[]{"software", Pop.last}
        ));

        // Run all the tests
        for (int i = 0; i < traversals.size(); i++) {
            assertEquals(TraversalHelper.getPopInstructions(traversals.get(i)), expectedResults.get(i));
        }
    }
}
