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
}
