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

import org.apache.tinkerpop.gremlin.TestDataBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;


import org.apache.tinkerpop.gremlin.process.traversal.step.PopContaining;


/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class AddEdgeStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.addE("knows").property("a", "b"),
                __.addE("created").property("a", "b"),
                __.addE("knows").property("a", "b").property("c", "d"),
                __.addE("knows").property("c", "d")
        );
    }

    @Test
    public void shouldObtainPopInstructions() {
        // Edge Step Test
        final AddEdgeStep<Object> addEdgeStep = new AddEdgeStep<>(__.identity().asAdmin(),
                (Traversal.Admin) __.select(Pop.first, "b").select("a"));

        final HashSet<PopContaining.PopInstruction> expectedOutput = TestDataBuilder.createPopInstructionSet(
                new Object[]{"b", Pop.first},
                new Object[]{"a", Pop.last}
        );

        assertEquals(addEdgeStep.getPopInstructions(), expectedOutput);

        // Edge Step Start test
        final AddEdgeStartStep addEdgeStartStep = new AddEdgeStartStep(__.identity().asAdmin(),
                __.select(Pop.first, "b").select("a"));

        assertEquals(addEdgeStartStep.getPopInstructions(), expectedOutput);
    }
}
