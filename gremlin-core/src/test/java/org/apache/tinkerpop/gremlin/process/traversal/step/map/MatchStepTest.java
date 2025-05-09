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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.CoinStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.and;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.as;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.match;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.or;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.where;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MatchStepTest extends StepTest {
    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.match(as("a").out("knows").as("b")),
                __.match(as("a").out().as("b")),
                ////
                __.match(where(as("a").out("knows").as("b"))),
                __.match(as("a").where(out().as("b"))),
                ///
                match(__.as("a").out().as("b"), __.as("b").out().as("c")),
                match(__.as("b").out().as("c"), __.as("a").out().as("d"))
        );
    }

    @Test
    public void testPreCompilationOfStartAndEnds() {
        final Traversal.Admin<?, ?> traversal = __.match(as("a").out().as("b"), as("c").path().as("d")).asAdmin();
        final MatchStep<?, ?> matchStep = (MatchStep<?, ?>) traversal.getStartStep();
        assertEquals(MatchStep.class, traversal.getStartStep().getClass());
        assertEquals(2, matchStep.getGlobalChildren().size());
        Traversal.Admin<Object, Object> pattern = matchStep.getGlobalChildren().get(0);
        assertEquals("a", ((MatchStep.MatchStartStep) pattern.getStartStep()).getSelectKey().get());
        assertEquals(VertexStep.class, pattern.getStartStep().getNextStep().getClass());
        assertEquals("b", ((MatchStep.MatchEndStep) pattern.getEndStep()).getMatchKey().get());
        //
        pattern = matchStep.getGlobalChildren().get(1);
        assertEquals("c", ((MatchStep.MatchStartStep) pattern.getStartStep()).getSelectKey().get());
        assertEquals(PathStep.class, pattern.getStartStep().getNextStep().getClass());
        assertEquals("d", ((MatchStep.MatchEndStep) pattern.getEndStep()).getMatchKey().get());
    }

    @Test
    public void testPreCompilationOfOr() {
        final List<Traversal.Admin<?, ?>> traversals = Arrays.asList(
                __.match(as("a").out().as("b"), or(as("c").path().as("d"), as("e").coin(0.5).as("f"))).asAdmin(),
                __.match(as("a").out().as("b"), as("c").path().as("d").or().as("e").coin(0.5).as("f")).asAdmin());
        assertEquals(1, new HashSet<>(traversals).size()); // the two patterns should pre-compile to the same traversal
        traversals.forEach(traversal -> {
            final MatchStep<?, ?> matchStep = (MatchStep<?, ?>) traversal.getStartStep();
            assertEquals(2, matchStep.getGlobalChildren().size());
            Traversal.Admin<Object, Object> pattern = matchStep.getGlobalChildren().get(0);
            assertEquals("a", ((MatchStep.MatchStartStep) pattern.getStartStep()).getSelectKey().get());
            assertEquals(VertexStep.class, pattern.getStartStep().getNextStep().getClass());
            assertEquals("b", ((MatchStep.MatchEndStep) pattern.getEndStep()).getMatchKey().get());
            //
            pattern = matchStep.getGlobalChildren().get(1);
            assertEquals(MatchStep.class, pattern.getStartStep().getClass());
            assertEquals(ConnectiveStep.Connective.OR, ((MatchStep<?, ?>) pattern.getStartStep()).getConnective());
            assertEquals("c", ((MatchStep.MatchStartStep) ((MatchStep<?, ?>) pattern.getStartStep()).getGlobalChildren().get(0).getStartStep()).getSelectKey().get());
            assertEquals(PathStep.class, ((MatchStep<?, ?>) pattern.getStartStep()).getGlobalChildren().get(0).getStartStep().getNextStep().getClass());
            assertEquals("d", ((MatchStep.MatchEndStep) ((MatchStep<?, ?>) pattern.getStartStep()).getGlobalChildren().get(0).getEndStep()).getMatchKey().get());
            assertEquals("e", ((MatchStep.MatchStartStep) ((MatchStep<?, ?>) pattern.getStartStep()).getGlobalChildren().get(1).getStartStep()).getSelectKey().get());
            assertEquals(CoinStep.class, ((MatchStep<?, ?>) pattern.getStartStep()).getGlobalChildren().get(1).getStartStep().getNextStep().getClass());
            assertEquals("f", ((MatchStep.MatchEndStep) ((MatchStep<?, ?>) pattern.getStartStep()).getGlobalChildren().get(1).getEndStep()).getMatchKey().get());
        });
    }

    @Test
    public void testPreCompilationOfAnd() {
        final List<Traversal.Admin<?, ?>> traversals = Arrays.asList(
                __.match(as("a").out().as("b"), and(as("c").path().as("d"), as("e").barrier())).asAdmin(),
                __.match(as("a").out().as("b"), as("c").path().as("d").and().as("e").barrier()).asAdmin());
        assertEquals(1, new HashSet<>(traversals).size());   // the two patterns should pre-compile to the same traversal
        traversals.forEach(traversal -> {
            final MatchStep<?, ?> matchStep = (MatchStep<?, ?>) traversal.getStartStep();
            assertEquals(2, matchStep.getGlobalChildren().size());
            Traversal.Admin<Object, Object> pattern = matchStep.getGlobalChildren().get(0);
            assertEquals("a", ((MatchStep.MatchStartStep) pattern.getStartStep()).getSelectKey().get());
            assertEquals(VertexStep.class, pattern.getStartStep().getNextStep().getClass());
            assertEquals("b", ((MatchStep.MatchEndStep) pattern.getEndStep()).getMatchKey().get());
            //
            pattern = matchStep.getGlobalChildren().get(1);
            assertEquals(MatchStep.class, pattern.getStartStep().getClass());
            assertEquals(ConnectiveStep.Connective.AND, ((MatchStep<?, ?>) pattern.getStartStep()).getConnective());
            assertEquals("c", ((MatchStep.MatchStartStep) ((MatchStep<?, ?>) pattern.getStartStep()).getGlobalChildren().get(0).getStartStep()).getSelectKey().get());
            assertEquals(PathStep.class, ((MatchStep<?, ?>) pattern.getStartStep()).getGlobalChildren().get(0).getStartStep().getNextStep().getClass());
            assertEquals("d", ((MatchStep.MatchEndStep) ((MatchStep<?, ?>) pattern.getStartStep()).getGlobalChildren().get(0).getEndStep()).getMatchKey().get());
            assertEquals("e", ((MatchStep.MatchStartStep) ((MatchStep<?, ?>) pattern.getStartStep()).getGlobalChildren().get(1).getStartStep()).getSelectKey().get());
            assertEquals(NoOpBarrierStep.class, ((MatchStep<?, ?>) pattern.getStartStep()).getGlobalChildren().get(1).getStartStep().getNextStep().getClass());
            assertFalse(((MatchStep.MatchEndStep) ((MatchStep<?, ?>) pattern.getStartStep()).getGlobalChildren().get(1).getEndStep()).getMatchKey().isPresent());
        });
    }

    @Test
    public void testPreCompilationOfWhereTraversal() {
        final List<Traversal.Admin<?, ?>> traversals = Arrays.asList(
                __.match(as("a").out().as("b"), as("c").where(in().as("d"))).asAdmin(),
                __.match(as("a").out().as("b"), where(as("c").in().as("d"))).asAdmin());
        assertEquals(1, new HashSet<>(traversals).size()); // the two patterns should pre-compile to the same traversal
        traversals.forEach(traversal -> {
            final MatchStep<?, ?> matchStep = (MatchStep<?, ?>) traversal.getStartStep();
            //assertFalse(matchStep.getStartLabel().isPresent());
            assertEquals(2, matchStep.getGlobalChildren().size());
            Traversal.Admin<Object, Object> pattern = matchStep.getGlobalChildren().get(0);
            assertEquals("a", ((MatchStep.MatchStartStep) pattern.getStartStep()).getSelectKey().get());
            assertEquals(VertexStep.class, pattern.getStartStep().getNextStep().getClass());
            assertEquals("b", ((MatchStep.MatchEndStep) pattern.getEndStep()).getMatchKey().get());
            //
            pattern = matchStep.getGlobalChildren().get(1);
            assertEquals(MatchStep.MatchStartStep.class, pattern.getStartStep().getClass());
            assertEquals("c", ((MatchStep.MatchStartStep) pattern.getStartStep()).getSelectKey().get());
            assertEquals(WhereTraversalStep.class, pattern.getStartStep().getNextStep().getClass());
            assertEquals(MatchStep.MatchEndStep.class, pattern.getStartStep().getNextStep().getNextStep().getClass());
            assertEquals(1, ((WhereTraversalStep<?>) pattern.getStartStep().getNextStep()).getLocalChildren().size());
            Traversal.Admin<?, ?> whereTraversal = ((WhereTraversalStep<?>) pattern.getStartStep().getNextStep()).getLocalChildren().get(0);
            assertEquals(WhereTraversalStep.WhereStartStep.class, whereTraversal.getStartStep().getClass());
            assertTrue(((WhereTraversalStep.WhereStartStep) whereTraversal.getStartStep()).getScopeKeys().isEmpty());
            assertEquals(VertexStep.class, whereTraversal.getStartStep().getNextStep().getClass());
            assertEquals(WhereTraversalStep.WhereEndStep.class, whereTraversal.getStartStep().getNextStep().getNextStep().getClass());
            assertEquals(1, ((WhereTraversalStep.WhereEndStep) whereTraversal.getStartStep().getNextStep().getNextStep()).getScopeKeys().size());
            assertEquals("d", ((WhereTraversalStep.WhereEndStep) whereTraversal.getStartStep().getNextStep().getNextStep()).getScopeKeys().iterator().next());
        });
    }

    @Test
    public void testPreCompilationOfWherePredicate() {
        final List<Traversal.Admin<?, ?>> traversals = Arrays.asList(
                __.match(as("a").out().as("b"), as("c").where(P.neq("d"))).asAdmin(),
                __.match(as("a").out().as("b"), where("c", P.neq("d"))).asAdmin());
        assertEquals(1, new HashSet<>(traversals).size()); // the two patterns should pre-compile to the same traversal
        traversals.forEach(traversal -> {
            final MatchStep<?, ?> matchStep = (MatchStep<?, ?>) traversal.getStartStep();
            //assertFalse(matchStep.getStartLabel().isPresent());
            assertEquals(2, matchStep.getGlobalChildren().size());
            Traversal.Admin<Object, Object> pattern = matchStep.getGlobalChildren().get(0);
            assertEquals("a", ((MatchStep.MatchStartStep) pattern.getStartStep()).getSelectKey().get());
            assertEquals(VertexStep.class, pattern.getStartStep().getNextStep().getClass());
            assertEquals("b", ((MatchStep.MatchEndStep) pattern.getEndStep()).getMatchKey().get());
            //
            pattern = matchStep.getGlobalChildren().get(1);
            assertEquals(MatchStep.MatchStartStep.class, pattern.getStartStep().getClass());
            assertEquals("c", ((MatchStep.MatchStartStep) pattern.getStartStep()).getSelectKey().get());
            assertEquals(WherePredicateStep.class, pattern.getStartStep().getNextStep().getClass());
            assertEquals(MatchStep.MatchEndStep.class, pattern.getStartStep().getNextStep().getNextStep().getClass());
            assertFalse(((WherePredicateStep<?>) pattern.getStartStep().getNextStep()).getStartKey().isPresent());
            assertEquals("d", ((WherePredicateStep<?>) pattern.getStartStep().getNextStep()).getPredicate().get().getOriginalValue());
        });
    }

    @Test
    public void testCountMatchAlgorithm() {
        // MAKE SURE THE SORT ORDER CHANGES AS MORE RESULTS ARE RETURNED BY ONE OR THE OTHER TRAVERSAL
        Traversal.Admin<?, ?> traversal = __.match(as("a").out().as("b"), as("c").in().as("d")).asAdmin();
        MatchStep.CountMatchAlgorithm countMatchAlgorithm = new MatchStep.CountMatchAlgorithm();
        countMatchAlgorithm.initialize(false, ((MatchStep<?, ?>) traversal.getStartStep()).getGlobalChildren());
        Traversal.Admin<Object, Object> firstPattern = ((MatchStep<?, ?>) traversal.getStartStep()).getGlobalChildren().get(0);
        Traversal.Admin<Object, Object> secondPattern = ((MatchStep<?, ?>) traversal.getStartStep()).getGlobalChildren().get(1);
        //
        assertEquals(2, countMatchAlgorithm.bundles.size());
        countMatchAlgorithm.bundles.stream().forEach(bundle -> assertEquals(0.0d, bundle.multiplicity, 0.0d));
        assertEquals(firstPattern, countMatchAlgorithm.bundles.get(0).traversal);
        assertEquals(secondPattern, countMatchAlgorithm.bundles.get(1).traversal);
        countMatchAlgorithm.recordStart(EmptyTraverser.instance(), firstPattern);
        countMatchAlgorithm.recordStart(EmptyTraverser.instance(), firstPattern);
        countMatchAlgorithm.recordStart(EmptyTraverser.instance(), secondPattern);
        countMatchAlgorithm.recordStart(EmptyTraverser.instance(), secondPattern);
        countMatchAlgorithm.recordStart(EmptyTraverser.instance(), secondPattern);
        assertEquals(firstPattern, countMatchAlgorithm.bundles.get(0).traversal);
        assertEquals(secondPattern, countMatchAlgorithm.bundles.get(1).traversal);
        assertEquals(MatchStep.TraversalType.MATCH_TRAVERSAL, countMatchAlgorithm.getBundle(firstPattern).traversalType);
        assertEquals(MatchStep.TraversalType.MATCH_TRAVERSAL, countMatchAlgorithm.getBundle(secondPattern).traversalType);
        assertEquals(2l, countMatchAlgorithm.getBundle(firstPattern).startsCount);
        assertEquals(3l, countMatchAlgorithm.getBundle(secondPattern).startsCount);
        assertEquals(0l, countMatchAlgorithm.getBundle(firstPattern).endsCount);
        assertEquals(0l, countMatchAlgorithm.getBundle(secondPattern).endsCount);
        ////
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), firstPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), secondPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), secondPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), secondPattern);
        assertEquals(2l, countMatchAlgorithm.getBundle(firstPattern).startsCount);
        assertEquals(3l, countMatchAlgorithm.getBundle(secondPattern).startsCount);
        assertEquals(1l, countMatchAlgorithm.getBundle(firstPattern).endsCount);
        assertEquals(3l, countMatchAlgorithm.getBundle(secondPattern).endsCount);
        assertEquals(0.5d, countMatchAlgorithm.getBundle(firstPattern).multiplicity, 0.01d);
        assertEquals(1.0d, countMatchAlgorithm.getBundle(secondPattern).multiplicity, 0.01d);
        assertEquals(firstPattern, countMatchAlgorithm.bundles.get(0).traversal);
        assertEquals(secondPattern, countMatchAlgorithm.bundles.get(1).traversal);
        // CHECK RE-SORTING AS MORE DATA COMES IN
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), firstPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), firstPattern);
        assertEquals(1.5d, countMatchAlgorithm.getBundle(firstPattern).multiplicity, 0.01d);
        assertEquals(1.0d, countMatchAlgorithm.getBundle(secondPattern).multiplicity, 0.01d);
        assertEquals(secondPattern, countMatchAlgorithm.bundles.get(0).traversal);
        assertEquals(firstPattern, countMatchAlgorithm.bundles.get(1).traversal);


        ///////  MAKE SURE WHERE PREDICATE TRAVERSALS ARE ALWAYS FIRST AS THEY ARE SIMPLY .hasNext() CHECKS
        traversal = __.match(as("a").out().as("b"), as("c").in().as("d"), where("a", P.eq("b"))).asAdmin();
        countMatchAlgorithm = new MatchStep.CountMatchAlgorithm();
        countMatchAlgorithm.initialize(false, ((MatchStep<?, ?>) traversal.getStartStep()).getGlobalChildren());
        assertEquals(3, countMatchAlgorithm.bundles.size());
        firstPattern = ((MatchStep<?, ?>) traversal.getStartStep()).getGlobalChildren().get(0);
        secondPattern = ((MatchStep<?, ?>) traversal.getStartStep()).getGlobalChildren().get(1);
        Traversal.Admin<Object, Object> thirdPattern = ((MatchStep<?, ?>) traversal.getStartStep()).getGlobalChildren().get(2);
        ///
        countMatchAlgorithm.bundles.stream().forEach(bundle -> assertEquals(0.0d, bundle.multiplicity, 0.0d));
        assertEquals(MatchStep.TraversalType.MATCH_TRAVERSAL, countMatchAlgorithm.getBundle(firstPattern).traversalType);
        assertEquals(MatchStep.TraversalType.MATCH_TRAVERSAL, countMatchAlgorithm.getBundle(secondPattern).traversalType);
        assertEquals(MatchStep.TraversalType.WHERE_PREDICATE, countMatchAlgorithm.getBundle(thirdPattern).traversalType);
        assertEquals(firstPattern, countMatchAlgorithm.bundles.get(0).traversal);
        assertEquals(secondPattern, countMatchAlgorithm.bundles.get(1).traversal);
        assertEquals(thirdPattern, countMatchAlgorithm.bundles.get(2).traversal);
        countMatchAlgorithm.recordStart(EmptyTraverser.instance(), firstPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), firstPattern);
        assertEquals(1l, countMatchAlgorithm.getBundle(firstPattern).startsCount);
        assertEquals(0l, countMatchAlgorithm.getBundle(secondPattern).startsCount);
        assertEquals(0l, countMatchAlgorithm.getBundle(thirdPattern).startsCount);
        assertEquals(1l, countMatchAlgorithm.getBundle(firstPattern).endsCount);
        assertEquals(0l, countMatchAlgorithm.getBundle(secondPattern).endsCount);
        assertEquals(0l, countMatchAlgorithm.getBundle(thirdPattern).endsCount);
        assertEquals(1.0d, countMatchAlgorithm.getBundle(firstPattern).multiplicity, 0.01d);
        assertEquals(0.0d, countMatchAlgorithm.getBundle(secondPattern).multiplicity, 0.01d);
        assertEquals(0.0d, countMatchAlgorithm.getBundle(thirdPattern).multiplicity, 0.01d);
        assertEquals(thirdPattern, countMatchAlgorithm.bundles.get(0).traversal);
        assertEquals(secondPattern, countMatchAlgorithm.bundles.get(1).traversal);
        assertEquals(firstPattern, countMatchAlgorithm.bundles.get(2).traversal);
        //
        countMatchAlgorithm.recordStart(EmptyTraverser.instance(), thirdPattern);
        countMatchAlgorithm.recordStart(EmptyTraverser.instance(), thirdPattern);
        countMatchAlgorithm.recordStart(EmptyTraverser.instance(), thirdPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), thirdPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), thirdPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), thirdPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), thirdPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), thirdPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), thirdPattern);
        assertEquals(1l, countMatchAlgorithm.getBundle(firstPattern).startsCount);
        assertEquals(0l, countMatchAlgorithm.getBundle(secondPattern).startsCount);
        assertEquals(3l, countMatchAlgorithm.getBundle(thirdPattern).startsCount);
        assertEquals(1l, countMatchAlgorithm.getBundle(firstPattern).endsCount);
        assertEquals(0l, countMatchAlgorithm.getBundle(secondPattern).endsCount);
        assertEquals(6l, countMatchAlgorithm.getBundle(thirdPattern).endsCount);
        assertEquals(1.0d, countMatchAlgorithm.getBundle(firstPattern).multiplicity, 0.01d);
        assertEquals(0.0d, countMatchAlgorithm.getBundle(secondPattern).multiplicity, 0.01d);
        assertEquals(2.0d, countMatchAlgorithm.getBundle(thirdPattern).multiplicity, 0.01d);
        assertEquals(thirdPattern, countMatchAlgorithm.bundles.get(0).traversal);
        assertEquals(secondPattern, countMatchAlgorithm.bundles.get(1).traversal);
        assertEquals(firstPattern, countMatchAlgorithm.bundles.get(2).traversal);
        //
        countMatchAlgorithm.recordStart(EmptyTraverser.instance(), secondPattern);
        countMatchAlgorithm.recordStart(EmptyTraverser.instance(), secondPattern);
        countMatchAlgorithm.recordStart(EmptyTraverser.instance(), secondPattern);
        countMatchAlgorithm.recordStart(EmptyTraverser.instance(), secondPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), secondPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), secondPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), secondPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), secondPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), secondPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), secondPattern);
        assertEquals(1l, countMatchAlgorithm.getBundle(firstPattern).startsCount);
        assertEquals(4l, countMatchAlgorithm.getBundle(secondPattern).startsCount);
        assertEquals(3l, countMatchAlgorithm.getBundle(thirdPattern).startsCount);
        assertEquals(1l, countMatchAlgorithm.getBundle(firstPattern).endsCount);
        assertEquals(6l, countMatchAlgorithm.getBundle(secondPattern).endsCount);
        assertEquals(6l, countMatchAlgorithm.getBundle(thirdPattern).endsCount);
        assertEquals(1.0d, countMatchAlgorithm.getBundle(firstPattern).multiplicity, 0.01d);
        assertEquals(1.5d, countMatchAlgorithm.getBundle(secondPattern).multiplicity, 0.01d);
        assertEquals(2.0d, countMatchAlgorithm.getBundle(thirdPattern).multiplicity, 0.01d);
        assertEquals(thirdPattern, countMatchAlgorithm.bundles.get(0).traversal);
        assertEquals(firstPattern, countMatchAlgorithm.bundles.get(1).traversal);
        assertEquals(secondPattern, countMatchAlgorithm.bundles.get(2).traversal);
    }

    @Test
    public void testComputerAwareCountMatchAlgorithm() {
        // MAKE SURE OLAP JOBS ARE BIASED TOWARDS STAR GRAPH DATA
        final Consumer doNothing = s -> {
        };
        final Traversal.Admin<?, ?> traversal = __.match(
                as("a").sideEffect(doNothing).as("b"),    // 1
                as("b").sideEffect(doNothing).as("c"),    // 2
                as("a").sideEffect(doNothing).as("d"),    // 5
                as("c").sideEffect(doNothing).as("e"),    // 4
                as("c").sideEffect(doNothing).as("f"))    // 3
                .asAdmin();
        traversal.applyStrategies(); // necessary to enure step ids are unique
        final MatchStep.CountMatchAlgorithm countMatchAlgorithm = new MatchStep.CountMatchAlgorithm();
        countMatchAlgorithm.initialize(true, ((MatchStep<?, ?>) traversal.getStartStep()).getGlobalChildren());
        Traversal.Admin<Object, Object> firstPattern = ((MatchStep<?, ?>) traversal.getStartStep()).getGlobalChildren().get(0);
        Traversal.Admin<Object, Object> secondPattern = ((MatchStep<?, ?>) traversal.getStartStep()).getGlobalChildren().get(1);
        Traversal.Admin<Object, Object> thirdPattern = ((MatchStep<?, ?>) traversal.getStartStep()).getGlobalChildren().get(2);
        Traversal.Admin<Object, Object> forthPattern = ((MatchStep<?, ?>) traversal.getStartStep()).getGlobalChildren().get(3);
        Traversal.Admin<Object, Object> fifthPattern = ((MatchStep<?, ?>) traversal.getStartStep()).getGlobalChildren().get(4);
        countMatchAlgorithm.bundles.stream().forEach(bundle -> assertEquals(0.0d, bundle.multiplicity, 0.0d));
        assertEquals(MatchStep.TraversalType.MATCH_TRAVERSAL, countMatchAlgorithm.getBundle(firstPattern).traversalType);
        assertEquals(MatchStep.TraversalType.MATCH_TRAVERSAL, countMatchAlgorithm.getBundle(secondPattern).traversalType);
        assertEquals(MatchStep.TraversalType.MATCH_TRAVERSAL, countMatchAlgorithm.getBundle(thirdPattern).traversalType);
        assertEquals(MatchStep.TraversalType.MATCH_TRAVERSAL, countMatchAlgorithm.getBundle(forthPattern).traversalType);
        assertEquals(MatchStep.TraversalType.MATCH_TRAVERSAL, countMatchAlgorithm.getBundle(fifthPattern).traversalType);
        assertEquals(firstPattern, countMatchAlgorithm.bundles.get(0).traversal);
        assertEquals(secondPattern, countMatchAlgorithm.bundles.get(1).traversal);
        assertEquals(thirdPattern, countMatchAlgorithm.bundles.get(2).traversal);
        assertEquals(forthPattern, countMatchAlgorithm.bundles.get(3).traversal);
        assertEquals(fifthPattern, countMatchAlgorithm.bundles.get(4).traversal);
        // MAKE THE SECOND PATTERN EXPENSIVE
        countMatchAlgorithm.recordStart(EmptyTraverser.instance(), secondPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), secondPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), secondPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), secondPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), secondPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), secondPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), secondPattern);
        // MAKE THE THIRD PATTERN MORE EXPENSIVE THAN FORTH
        countMatchAlgorithm.recordStart(EmptyTraverser.instance(), thirdPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), thirdPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), thirdPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), thirdPattern);
        // MAKE THE FORTH PATTERN EXPENSIVE
        countMatchAlgorithm.recordStart(EmptyTraverser.instance(), forthPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), forthPattern);
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), forthPattern);
        //
        Traverser.Admin traverser = B_LP_O_P_S_SE_SL_TraverserGenerator.instance().generate(1, EmptyStep.instance(), 1l);
        traverser.addLabels(Collections.singleton("a"));
        assertEquals(firstPattern, countMatchAlgorithm.apply(traverser));
        traverser = traverser.split(1, EmptyStep.instance());
        traverser.getTags().add(firstPattern.getStartStep().getId());
        traverser.addLabels(Collections.singleton("b"));
        //
        assertEquals(secondPattern, countMatchAlgorithm.apply(traverser));
        traverser = traverser.split(1, EmptyStep.instance());
        traverser.getTags().add(secondPattern.getStartStep().getId());
        traverser.addLabels(Collections.singleton("c"));
        //
        assertEquals(fifthPattern, countMatchAlgorithm.apply(traverser));
        traverser = traverser.split(1, EmptyStep.instance());
        traverser.getTags().add(fifthPattern.getStartStep().getId());
        traverser.addLabels(Collections.singleton("f"));
        //
        assertEquals(forthPattern, countMatchAlgorithm.apply(traverser));
        traverser = traverser.split(1, EmptyStep.instance());
        traverser.getTags().add(forthPattern.getStartStep().getId());
        traverser.addLabels(Collections.singleton("e"));
        //
        assertEquals(thirdPattern, countMatchAlgorithm.apply(traverser));
        traverser = traverser.split(1, EmptyStep.instance());
        traverser.getTags().add(thirdPattern.getStartStep().getId());
        traverser.addLabels(Collections.singleton("d"));
    }

    @Test
    public void shouldCalculateStartLabelCorrectly() {
        Traversal.Admin<?, ?> traversal = match(
                where(and(
                        as("a").out("created").as("b"),
                        as("b").in("created").count().is(eq(3)))),
                as("a").both().as("b"),
                where(as("b").in())).asAdmin();
        assertEquals("a", MatchStep.Helper.computeStartLabel(((MatchStep<?, ?>) traversal.getStartStep()).getGlobalChildren()));
        /////
        traversal = match(
                where("a", P.neq("c")),
                as("a").out("created").as("b"),
                or(
                        as("a").out("knows").has("name", "vadas"),
                        as("a").in("knows").and().as("a").has(T.label, "person")
                ),
                as("b").in("created").as("c"),
                as("b").in("created").count().is(P.gt(1))).asAdmin();
        assertEquals("a", MatchStep.Helper.computeStartLabel(((MatchStep<?, ?>) traversal.getStartStep()).getGlobalChildren()));
    }

    @Test
    public void testScopingInfo() {
        final Traversal.Admin<?, ?> traversal = __.match(as("a").out().as("b"), as("c").path().as("d")).asAdmin();
        final MatchStep<?, ?> matchStep = (MatchStep<?, ?>) traversal.getStartStep();

        final Scoping.ScopingInfo scopingInfo = new Scoping.ScopingInfo();
        scopingInfo.label = "c";
        scopingInfo.pop = Pop.last;

        final HashSet<Scoping.ScopingInfo> scopingInfoSet = new HashSet<>();
        scopingInfoSet.add(scopingInfo);

        assertEquals(matchStep.getScopingInfo(), scopingInfoSet);
    }

}
