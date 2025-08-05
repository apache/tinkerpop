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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.TestDataBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueStepTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;


import org.apache.tinkerpop.gremlin.process.traversal.step.PopContaining;


/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class AddEdgeStepTest extends GValueStepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.addE("knows").property("a", "b"),
                __.addE("created").property("a", "b"),
                __.addE("knows").property("a", "b").property("c", "e"),
                __.addE("knows").property("c", "e"),
                __.addE(GValue.of("label", "knows")).property("a", "b"),
                __.addE(GValue.of("label", "created")).property("a", GValue.of("prop", "b")),
                __.addE(GValue.of("label", "knows")).property("a", GValue.of("prop1", "b")).property("c", GValue.of("prop2", "e"))
        );
    }

    @Override
    protected List<Pair<Traversal, Set<String>>> getGValueTraversals() {
        return List.of(
                Pair.of(__.addE(GValue.of("label", "knows")).property("a", "b"), Set.of("label")),
                Pair.of(__.addE(GValue.of("label", "created")).property("a", GValue.of("prop", "b")), Set.of("label", "prop")),
                Pair.of(__.addE(GValue.of("label", "knows")).property("a", GValue.of("prop1", "b")).property("c", GValue.of("prop2", "e")), Set.of("label", "prop1", "prop2"))
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
