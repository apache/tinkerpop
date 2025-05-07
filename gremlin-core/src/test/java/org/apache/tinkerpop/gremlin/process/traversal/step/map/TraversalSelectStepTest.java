package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import java.util.HashSet;

import org.apache.tinkerpop.gremlin.TestDataBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.PopContaining;

import junit.framework.TestCase;

public class TraversalSelectStepTest extends TestCase {

    public void testGetPopInstructions() {
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