/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.CoinStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConjunctionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MatchStepTest extends StepTest {
    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.match("a", as("a").out("knows").as("b")),
                __.match(as("a").out("knows").as("b")),
                __.match("a", as("a").out().as("b")),
                __.match(as("a").out().as("b")),
                ////
                __.match("a", where(as("a").out("knows").as("b"))),
                __.match(where(as("a").out("knows").as("b"))),
                __.match("a", as("a").where(out().as("b"))),
                __.match(as("a").where(out().as("b")))
        );
    }

    @Test
    public void testPreCompilationOfStartAndEnds() {
        final Traversal.Admin<?, ?> traversal = __.match("a", as("a").out().as("b"), as("c").path().as("d")).asAdmin();
        final MatchStep<?, ?> matchStep = (MatchStep<?, ?>) traversal.getStartStep();
        assertEquals(MatchStep.class, traversal.getStartStep().getClass());
        assertEquals("a", matchStep.getStartKey().get());
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
                __.match("a", as("a").out().as("b"), or(as("c").path().as("d"), as("e").coin(0.5).as("f"))).asAdmin(),
                __.match("a", as("a").out().as("b"), as("c").path().as("d").or().as("e").coin(0.5).as("f")).asAdmin());
        assertEquals(1, new HashSet<>(traversals).size()); // the two patterns should pre-compile to the same traversal
        traversals.forEach(traversal -> {
            final MatchStep<?, ?> matchStep = (MatchStep<?, ?>) traversal.getStartStep();
            assertEquals("a", matchStep.getStartKey().get());
            assertEquals(2, matchStep.getGlobalChildren().size());
            Traversal.Admin<Object, Object> pattern = matchStep.getGlobalChildren().get(0);
            assertEquals("a", ((MatchStep.MatchStartStep) pattern.getStartStep()).getSelectKey().get());
            assertEquals(VertexStep.class, pattern.getStartStep().getNextStep().getClass());
            assertEquals("b", ((MatchStep.MatchEndStep) pattern.getEndStep()).getMatchKey().get());
            //
            pattern = matchStep.getGlobalChildren().get(1);
            assertEquals(MatchStep.class, pattern.getStartStep().getClass());
            assertEquals(ConjunctionStep.Conjunction.OR, ((MatchStep<?, ?>) pattern.getStartStep()).getConjunction());
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
                __.match("a", as("a").out().as("b"), and(as("c").path().as("d"), as("e").barrier())).asAdmin(),
                __.match("a", as("a").out().as("b"), as("c").path().as("d").and().as("e").barrier()).asAdmin());
        assertEquals(1, new HashSet<>(traversals).size());   // the two patterns should pre-compile to the same traversal
        traversals.forEach(traversal -> {
            MatchStep<?, ?> matchStep = (MatchStep<?, ?>) traversal.getStartStep();
            assertEquals("a", matchStep.getStartKey().get());
            assertEquals(2, matchStep.getGlobalChildren().size());
            Traversal.Admin<Object, Object> pattern = matchStep.getGlobalChildren().get(0);
            assertEquals("a", ((MatchStep.MatchStartStep) pattern.getStartStep()).getSelectKey().get());
            assertEquals(VertexStep.class, pattern.getStartStep().getNextStep().getClass());
            assertEquals("b", ((MatchStep.MatchEndStep) pattern.getEndStep()).getMatchKey().get());
            //
            pattern = matchStep.getGlobalChildren().get(1);
            assertEquals(MatchStep.class, pattern.getStartStep().getClass());
            assertEquals(ConjunctionStep.Conjunction.AND, ((MatchStep<?, ?>) pattern.getStartStep()).getConjunction());
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
            MatchStep<?, ?> matchStep = (MatchStep<?, ?>) traversal.getStartStep();
            assertFalse(matchStep.getStartKey().isPresent());
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
            MatchStep<?, ?> matchStep = (MatchStep<?, ?>) traversal.getStartStep();
            assertFalse(matchStep.getStartKey().isPresent());
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
        Traversal.Admin<?, ?> traversal = __.match("a", as("a").out().as("b"), as("c").in().as("d")).asAdmin();
        MatchStep.CountMatchAlgorithm countMatchAlgorithm = new MatchStep.CountMatchAlgorithm();
        countMatchAlgorithm.initialize(((MatchStep<?, ?>) traversal.getStartStep()).getGlobalChildren());
        assertEquals(2, countMatchAlgorithm.counts.size());
        countMatchAlgorithm.counts.stream().forEach(ints -> assertEquals(Integer.valueOf(0), ints[1]));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(0));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(0));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(1));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(1));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(1));
        assertEquals(Integer.valueOf(0), countMatchAlgorithm.counts.get(0)[0]);
        assertEquals(Integer.valueOf(2), countMatchAlgorithm.counts.get(0)[1]);
        //
        assertEquals(Integer.valueOf(1), countMatchAlgorithm.counts.get(1)[0]);
        assertEquals(Integer.valueOf(3), countMatchAlgorithm.counts.get(1)[1]);
        // CHECK RE-SORTING AS MORE DATA COMES IN
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(0));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(0));
        assertEquals(Integer.valueOf(1), countMatchAlgorithm.counts.get(0)[0]);
        assertEquals(Integer.valueOf(3), countMatchAlgorithm.counts.get(0)[1]);
        //
        assertEquals(Integer.valueOf(0), countMatchAlgorithm.counts.get(1)[0]);
        assertEquals(Integer.valueOf(4), countMatchAlgorithm.counts.get(1)[1]);


        ///////  MAKE SURE WHERE PREDICATE TRAVERSALS ARE ALWAYS FIRST AS THEY ARE SIMPLY .hasNext() CHECKS
        traversal = __.match("a", as("a").out().as("b"), as("c").in().as("d"), where("a", P.eq("b"))).asAdmin();
        countMatchAlgorithm = new MatchStep.CountMatchAlgorithm();
        countMatchAlgorithm.initialize(((MatchStep<?, ?>) traversal.getStartStep()).getGlobalChildren());
        assertEquals(3, countMatchAlgorithm.counts.size());
        countMatchAlgorithm.counts.stream().forEach(ints -> assertEquals(Integer.valueOf(0), ints[1]));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(0));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(1));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(1));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(2));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(2));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(2));
        //
        assertEquals(Integer.valueOf(2), countMatchAlgorithm.counts.get(0)[0]);
        assertEquals(Integer.valueOf(3), countMatchAlgorithm.counts.get(0)[1]);
        //
        assertEquals(Integer.valueOf(0), countMatchAlgorithm.counts.get(1)[0]);
        assertEquals(Integer.valueOf(1), countMatchAlgorithm.counts.get(1)[1]);
        //
        assertEquals(Integer.valueOf(1), countMatchAlgorithm.counts.get(2)[0]);
        assertEquals(Integer.valueOf(2), countMatchAlgorithm.counts.get(2)[1]);
        //
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(2));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(2));
        countMatchAlgorithm.recordEnd(EmptyTraverser.instance(), countMatchAlgorithm.traversals.get(2));
        //
        assertEquals(Integer.valueOf(2), countMatchAlgorithm.counts.get(0)[0]);
        assertEquals(Integer.valueOf(6), countMatchAlgorithm.counts.get(0)[1]);
        //
        assertEquals(Integer.valueOf(0), countMatchAlgorithm.counts.get(1)[0]);
        assertEquals(Integer.valueOf(1), countMatchAlgorithm.counts.get(1)[1]);
        //
        assertEquals(Integer.valueOf(1), countMatchAlgorithm.counts.get(2)[0]);
        assertEquals(Integer.valueOf(2), countMatchAlgorithm.counts.get(2)[1]);
        //

    }
}
