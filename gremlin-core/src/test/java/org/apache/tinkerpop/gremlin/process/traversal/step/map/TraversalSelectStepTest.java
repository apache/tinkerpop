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
 * specific language governing permissions anmvn  limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import org.apache.tinkerpop.gremlin.TestDataBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.PopContaining;
import org.junit.Test;

public class TraversalSelectStepTest {
    
    @Test
    public void shouldObtainPopInstructions() {
        TraversalSelectStep step = new TraversalSelectStep(__.select("a").asAdmin(), Pop.first, __.select(Pop.first, "b").select("c"));

        HashSet<PopContaining.PopInstruction> expectedOutput = TestDataBuilder.createPopInstructionSet(
                new Object[]{"b", Pop.first},
                new Object[]{"c", Pop.last}
        );

        assertEquals(step.getPopInstructions(), expectedOutput);

        step = new TraversalSelectStep(__.identity().asAdmin(), Pop.first, __.select(Pop.first, "a", "b", "c"));

        // 3 keys, and Pop.first
        expectedOutput = TestDataBuilder.createPopInstructionSet(
                new Object[]{"a", Pop.first},
                new Object[]{"c", Pop.first},
                new Object[]{"b", Pop.first}
        );

        assertEquals(step.getPopInstructions(), expectedOutput);
    

}
}