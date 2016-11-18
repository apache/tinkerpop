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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class SelectOneStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.select(Pop.all, "x"),
                __.select(Pop.first, "x"),
                __.select(Pop.last, "x"),
                __.select(Pop.mixed, "x"),
                __.select(Pop.all, "x").by("name"),
                __.select(Pop.first, "x").by("name"),
                __.select(Pop.last, "x").by("name"),
                __.select(Pop.mixed, "x").by("name")
        );
    }

    @Test
    public void shouldRequirePathsAccordingly() {
        Object[][] traversalPaths = new Object[][]{
                {false, __.select("x").asAdmin()},
                {true, __.as("x").select("x").asAdmin()},
                {false, __.local(__.select("x")).asAdmin()},
                {true, __.as("x").local(__.select("x")).asAdmin()},
        };
        for (final Object[] traversalPath : traversalPaths) {
            assertEquals(traversalPath[0], ((Traversal.Admin<?, ?>) traversalPath[1]).getTraverserRequirements().contains(TraverserRequirement.LABELED_PATH));
        }
    }
}
