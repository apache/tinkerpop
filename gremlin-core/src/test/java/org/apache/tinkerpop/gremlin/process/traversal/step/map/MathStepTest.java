/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.TestDataBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.PopContaining;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MathStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.math("a+b"),
                __.math("a+b").by("age"),
                __.math("a/b").by("age"),
                __.math("a+b").by("age").by(both().count()),
                __.math("sin a + b")
        );
    }

    @Test
    public void shouldParseVariablesCorrectly() {
        assertEquals(Collections.emptyList(), new ArrayList<>(MathStep.getVariables("1 + 2")));
        assertEquals(Arrays.asList("a", "b"), new ArrayList<>(MathStep.getVariables("a + b / 2")));
        assertEquals(Arrays.asList("a", "b"), new ArrayList<>(MathStep.getVariables("a + b / sin 2")));
        assertEquals(Arrays.asList("a", "b"), new ArrayList<>(MathStep.getVariables("a + b / sin (2 + 6.67e−11)")));
        assertEquals(Arrays.asList("a", "b", "_", "x", "z"), new ArrayList<>(MathStep.getVariables("(a + b / _) + log2 (x^3)^z")));
        assertEquals(Arrays.asList("a", "b", "_", "x", "z"), new ArrayList<>(MathStep.getVariables("(a + b / _) + log2 (x^3)^z + b + a")));
        assertEquals(Arrays.asList("a_ASDF", "b", "_", "x", "z", "a"), new ArrayList<>(MathStep.getVariables("(a_ASDF + b / _) + log2 (x^3)^z + b + a")));
        assertEquals(Arrays.asList("a_ASDF", "bzz_", "_", "x", "z", "a_var", "d"), new ArrayList<>(MathStep.getVariables("((a_ASDF + bzz_ / _) + log2 (x^3)^z + bzz_ + (sinh (a_var + 10))) / (2.0265 * d)")));
        assertEquals(Arrays.asList("ac", "b", "_", "x", "z2"), new ArrayList<>(MathStep.getVariables("(ac + b / _) + log2 (x^3)^z2 + b + (tan (log10 ac / sqrt b))")));
    }

    @Test
    public void shouldParseVariablesWithFunctionNameMatches() {
        assertEquals(Arrays.asList("c", "expa"), new ArrayList<>(MathStep.getVariables("c - expa")));
        assertEquals(Arrays.asList("c", "expa"), new ArrayList<>(MathStep.getVariables("c - expa - exp(13)")));
        assertEquals(Arrays.asList("c", "expa"), new ArrayList<>(MathStep.getVariables("sin 3 - c - expa - exp(13)")));
        assertEquals(Arrays.asList("c", "cosa"), new ArrayList<>(MathStep.getVariables("c + cosa - 2cos(13)")));
        assertEquals(Arrays.asList("cosa", "c"), new ArrayList<>(MathStep.getVariables("cosa + c")));
        assertEquals(Arrays.asList("number1", "expected_value"), new ArrayList<>(MathStep.getVariables("number1-expected_value")));
    }

    @Test
    public void testPopInstruction() {
        final GraphTraversal<Object, Object> traversal = __.identity();

        final MathStep mathStep = new MathStep<>((Traversal.Admin) traversal, "a + b - c");

        // Expected Output
        final HashSet<PopContaining.PopInstruction> popInstructionSet = TestDataBuilder.createPopInstructionSet(
                new Object[]{"a", Pop.last},
                new Object[]{"b", Pop.last},
                new Object[]{"c", Pop.last}
        );

        assertEquals(popInstructionSet, mathStep.getPopInstructions());
    }

}