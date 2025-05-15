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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static org.apache.tinkerpop.gremlin.process.traversal.P.within;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.as;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class WhereStepTest extends StepTest {

    private static final Logger log = LoggerFactory.getLogger(WhereStepTest.class);

    @Override
    public List<Traversal> getTraversals() {
        return Arrays.asList(
                as("a").out().as("b").where(as("a").out()),
                as("a").out().as("b").where(as("a").out().as("b")),
                as("a").out().as("b").where("a", P.neq("b")),
                as("a").out().as("b").where("a", P.neq("c")),
                as("a").out().as("b").where("a", P.neq("b")).by("name"),
                as("a").out().as("b").where("a", P.neq("b")).by("name").by("age"),
                as("a").out().as("b").where("a", P.neq("b")).by("age"),
                as("a").out().as("b").where("a", P.neq("b").and(P.neq("a"))).by("age")
        );
    }

    @Test
    public void shouldRequirePathsAccordingly() {
        Object[][] traversalPaths = new Object[][]{
                {false, __.where(P.not(P.within("x"))).asAdmin()},
                {true, __.as("x").where(P.not(P.within("x"))).asAdmin()},
                {true, __.as("a").where(P.not(P.within("x"))).asAdmin()},
                {false, __.local(__.where(P.not(P.within("x")))).asAdmin()},
                {true, __.as("x").local(__.where(P.not(P.within("x")))).asAdmin()},
                {false, __.local(__.where(P.not(P.within("x")))).asAdmin()},
        };
        for (final Object[] traversalPath : traversalPaths) {
            assertEquals(traversalPath[0], ((Traversal.Admin<?, ?>) traversalPath[1]).getTraverserRequirements().contains(TraverserRequirement.LABELED_PATH));
        }
    }

    @Test
    public void testScopingInfo() {

        // Testing WherePredicate Step
        final WherePredicateStep wherePredicateStep = new WherePredicateStep(__.identity().asAdmin(), Optional.of("key1"), P.neq("label1"));

        final Scoping.ScopingInfo scopingInfo1 = new Scoping.ScopingInfo();
        scopingInfo1.label = "key1";
        scopingInfo1.pop = Pop.last;

        final Scoping.ScopingInfo scopingInfo2 = new Scoping.ScopingInfo();
        scopingInfo2.label = "label1";
        scopingInfo2.pop = Pop.last;

        HashSet<Scoping.ScopingInfo> scopingInfoHashSet = new HashSet<>();
        scopingInfoHashSet.add(scopingInfo1);
        scopingInfoHashSet.add(scopingInfo2);

        assertEquals(wherePredicateStep.getScopingInfo(), scopingInfoHashSet);

        // Testing WhereTraversal Test
        final WhereTraversalStep whereTraversalStep = new WhereTraversalStep<>(new DefaultTraversal(), __.as("x").select("a", "b").asAdmin());
        final Scoping.ScopingInfo scopingInfo = new Scoping.ScopingInfo();
        scopingInfo.label = "x";
        scopingInfo.pop = Pop.last;

        scopingInfoHashSet = new HashSet<>();
        scopingInfoHashSet.add(scopingInfo);

        assertEquals(whereTraversalStep.getScopingInfo(), scopingInfoHashSet);

    }
}
