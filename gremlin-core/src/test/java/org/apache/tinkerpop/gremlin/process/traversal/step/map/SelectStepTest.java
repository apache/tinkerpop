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

import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class SelectStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.select(Pop.all, "x", "y"),
                __.select(Pop.first, "x", "y"),
                __.select(Pop.last, "x", "y"),
                __.select(Pop.mixed, "x", "y"),
                __.select(Pop.all, "x", "y").by("name").by("age"),
                __.select(Pop.first, "x", "y").by("name").by("age"),
                __.select(Pop.last, "x", "y").by("name").by("age"),
                __.select(Pop.mixed, "x", "y").by("name").by("age")
        );
    }

    @Test
    public void shouldRequirePathsAccordingly() {
        Object[][] traversalPaths = new Object[][]{
                {false, __.select("x", "y").asAdmin()},
                {true, __.as("x").select("x", "y").asAdmin()},
                {true, __.as("x").out().as("y").select("x", "y").asAdmin()},
                {false, __.local(__.select("x", "y")).asAdmin()},
                {true, __.as("x").local(__.select("x", "y")).asAdmin()},
                {true, __.as("x").out().as("y").local(__.select("x", "y")).asAdmin()},
        };
        for (final Object[] traversalPath : traversalPaths) {
            assertEquals(traversalPath[0], ((Traversal.Admin<?, ?>) traversalPath[1]).getTraverserRequirements().contains(TraverserRequirement.LABELED_PATH));
        }
    }

    @Test
    public void testScopingInfo() {
        final GraphTraversal<Object, Object> traversal = __.identity();

        // 2 keys, and Pop.all
        final SelectStep selectStepAll = new SelectStep<>((Traversal.Admin) traversal, Pop.all, "x", "y");
        HashSet<Scoping.ScopingInfo> scopingInfoSet = new HashSet<>();

        Scoping.ScopingInfo scopingInfo1 = new Scoping.ScopingInfo();
        scopingInfo1.label = "x";
        scopingInfo1.pop = Pop.all;

        Scoping.ScopingInfo scopingInfo2 = new Scoping.ScopingInfo();
        scopingInfo2.label = "y";
        scopingInfo2.pop = Pop.all;

        scopingInfoSet.add(scopingInfo1);
        scopingInfoSet.add(scopingInfo2);

        assertEquals(selectStepAll.getScopingInfo(), scopingInfoSet);


        // 3 keys, and Pop.last
        final SelectStep selectStepLast = new SelectStep<>((Traversal.Admin) traversal, Pop.last, "x", "y", "z");
        scopingInfoSet = new HashSet<>();

        scopingInfo1 = new Scoping.ScopingInfo();
        scopingInfo1.label = "x";
        scopingInfo1.pop = Pop.last;

        scopingInfo2 = new Scoping.ScopingInfo();
        scopingInfo2.label = "y";
        scopingInfo2.pop = Pop.last;

        final Scoping.ScopingInfo scopingInfo3 = new Scoping.ScopingInfo();
        scopingInfo3.label = "z";
        scopingInfo3.pop = Pop.last;

        scopingInfoSet.add(scopingInfo1);
        scopingInfoSet.add(scopingInfo2);
        scopingInfoSet.add(scopingInfo3);

        assertEquals(selectStepLast.getScopingInfo(), scopingInfoSet);


        // 2 keys, and Pop.first
        final SelectStep selectStepFirst = new SelectStep<>((Traversal.Admin) traversal, Pop.first, "x", "y");
        scopingInfoSet = new HashSet<>();

        scopingInfo1 = new Scoping.ScopingInfo();
        scopingInfo1.label = "x";
        scopingInfo1.pop = Pop.first;

        scopingInfo2 = new Scoping.ScopingInfo();
        scopingInfo2.label = "y";
        scopingInfo2.pop = Pop.first;

        scopingInfoSet.add(scopingInfo1);
        scopingInfoSet.add(scopingInfo2);

        assertEquals(selectStepFirst.getScopingInfo(), scopingInfoSet);


        // 2 keys, and Pop.mixed
        final SelectStep selectStepMixed = new SelectStep<>((Traversal.Admin) traversal, Pop.mixed, "x", "y");
        scopingInfoSet = new HashSet<>();

        scopingInfo1 = new Scoping.ScopingInfo();
        scopingInfo1.label = "x";
        scopingInfo1.pop = Pop.mixed;

        scopingInfo2 = new Scoping.ScopingInfo();
        scopingInfo2.label = "y";
        scopingInfo2.pop = Pop.mixed;

        scopingInfoSet.add(scopingInfo1);
        scopingInfoSet.add(scopingInfo2);

        assertEquals(selectStepMixed.getScopingInfo(), scopingInfoSet);

    }
}
