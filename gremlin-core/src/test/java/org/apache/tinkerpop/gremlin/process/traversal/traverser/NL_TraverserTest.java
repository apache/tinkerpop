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

package org.apache.tinkerpop.gremlin.process.traversal.traverser;

import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public abstract class NL_TraverserTest {
    protected static final String STEP_LABEL = "testStep";
    protected static final String LOOP_NAME = "loopName";
    protected final long BULK = 1L;
    protected final String TEST = "test";
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();
    @Mock
    protected Step<String, ?> mockStep;

    @Before
    public void setUpParent() {
        when(mockStep.getTraversal()).thenReturn(EmptyTraversal.instance());
    }

    abstract NL_SL_Traverser<String> createTraverser();

    @Test
    public void shouldInitializeNestedLoopsWithLoopName() {
        NL_SL_Traverser<String> traverser = createTraverser();
        traverser.initialiseLoops(STEP_LABEL, LOOP_NAME);

        assertEquals(1, traverser.getNestedLoops().size());
        assertEquals(0, traverser.loops());
        assertEquals(0, traverser.loops(STEP_LABEL));
        assertEquals(0, traverser.loops(LOOP_NAME));
        assertEquals(2, traverser.getLoopNames().size());
        assertTrue(traverser.getLoopNames().containsAll(Set.of(LOOP_NAME, STEP_LABEL)));
    }

    @Test
    public void shouldInitializeNestedLoopsWithoutLoopName() {
        NL_SL_Traverser<String> traverser = createTraverser();
        traverser.initialiseLoops(STEP_LABEL, null);

        assertEquals(1, traverser.getNestedLoops().size());
        assertEquals(0, traverser.loops());
        assertEquals(0, traverser.loops(STEP_LABEL));
        assertEquals(1, traverser.getLoopNames().size());
        assertEquals(STEP_LABEL, traverser.getLoopNames().iterator().next());
    }

    @Test
    public void shouldInitializeNestedLoopsWithLoopNameSameAsStepLabel() {
        NL_SL_Traverser<String> traverser = createTraverser();
        traverser.initialiseLoops(STEP_LABEL, STEP_LABEL);

        assertEquals(1, traverser.getNestedLoops().size());
        assertEquals(0, traverser.loops());
        assertEquals(0, traverser.loops(STEP_LABEL));
        assertEquals(1, traverser.getLoopNames().size());
        assertEquals(STEP_LABEL, traverser.getLoopNames().iterator().next());
    }

    @Test
    public void shouldIncrementNestedLoops() {
        NL_SL_Traverser<String> traverser = createTraverser();
        traverser.initialiseLoops(STEP_LABEL, null);
        traverser.incrLoops();
        traverser.incrLoops();

        assertEquals(2, traverser.loops());
        assertEquals(2, traverser.loops(STEP_LABEL));
    }

    @Test
    public void shouldIncrementMultipleNestedLoops() {
        NL_SL_Traverser<String> traverser = createTraverser();
        traverser.initialiseLoops(STEP_LABEL, LOOP_NAME);
        traverser.incrLoops();
        traverser.initialiseLoops(STEP_LABEL + "2", LOOP_NAME + "2");
        traverser.incrLoops();
        traverser.incrLoops();

        assertEquals(1, traverser.loops(LOOP_NAME));
        assertEquals(1, traverser.loops(STEP_LABEL));
        assertEquals(2, traverser.loops(LOOP_NAME + "2"));
        assertEquals(2, traverser.loops(STEP_LABEL + "2"));
        assertEquals(2, traverser.loops());
    }

    @Test
    public void shouldResetNestedLoops() {
        NL_SL_Traverser<String> traverser = createTraverser();
        traverser.initialiseLoops(STEP_LABEL, null);
        traverser.incrLoops();
        assertEquals(1, traverser.loops());
        assertEquals(1, traverser.loops(STEP_LABEL));

        traverser.resetLoops();
        assertTrue(traverser.getNestedLoops().isEmpty());
    }

    @Test
    public void shouldCloneLoopStateOnSplit() {
        NL_SL_Traverser<String> original = createTraverser();
        original.initialiseLoops(STEP_LABEL, LOOP_NAME);
        original.incrLoops();
        NL_SL_Traverser<String> clone = (NL_SL_Traverser<String>) original.split();

        assertEquals(1, original.loops());
        assertEquals(1, original.getNestedLoops().size());
        assertEquals(2, original.getNestedLoopNames().size());
        assertEquals(original.loops(), clone.loops());
        assertNotSame(original.getNestedLoops(), clone.getNestedLoops());
        assertEquals(original.getNestedLoops(), clone.getNestedLoops());
        assertNotSame(original.getNestedLoopNames(), clone.getNestedLoopNames());
        assertEquals(original.getNestedLoopNames(), clone.getNestedLoopNames());
    }

    @Test
    public void shouldReturnNestedLoopRequirement() {
        assertEquals(TraverserRequirement.NESTED_LOOP, createTraverser().getLoopRequirement());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForInvalidLoopName() {
        NL_SL_Traverser<String> traverser = createTraverser();
        traverser.initialiseLoops(STEP_LABEL, null);
        traverser.loops("doesnotexist");
    }
}