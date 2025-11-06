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

import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_NL_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class SampleGlobalStepTest extends StepTest {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();
    @Mock
    protected Step<String, ?> mockStep;

    @Before
    public void setup() {
        when(mockStep.getTraversal()).thenReturn(EmptyTraversal.instance());
    }
    
    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.sample(5),
                __.sample(10)
        );
    }

    @Test
    public void shouldSelectSubsetsCorrectly() {
        final List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add(i);
        }
        for(int i=0; i<100; i++) {
            List<Integer> result;
            result = (List) __.inject(list).unfold().sample(i).toList();
            assertEquals(i, result.size());
            assertEquals(i, new HashSet<>(result).size());
            assertTrue(list.containsAll(result));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowForMultipleByModulators() {
        __.V().sample(1).by("age").by(T.id);
    }

    @Test
    public void testSampleBiOperatorCombinesTraverserSetsWithLoops() {
        final SampleGlobalStep.SampleBiOperator<String> operator = new SampleGlobalStep.SampleBiOperator<>();

        final TraverserSet<String> setA = new TraverserSet<>();
        setA.add(createTraverser("a", 1));
        setA.add(createTraverser("b", 2));

        final TraverserSet<String> setB = new TraverserSet<>();
        setB.add(createTraverser("c", 2));
        setB.add(createTraverser("d", 1));
        setB.add(createTraverser("e", 2));
        setB.add(createTraverser("f", 0));

        final TraverserSet<String> result = operator.apply(setA, setB);
        assertEquals(3, result.size());
        assertTrue(result.stream().map(Traverser::get).collect(Collectors.toList()).containsAll(List.of("b", "c", "e")));
    }

    @Test
    public void testSampleBiOperatorCombinesTraverserSetsWithoutLoops() {
        final SampleGlobalStep.SampleBiOperator<String> operator = new SampleGlobalStep.SampleBiOperator<>();

        final TraverserSet<String> setA = new TraverserSet<>();
        setA.add(createTraverser("a", 0));
        setA.add(createTraverser("b", 0));

        final TraverserSet<String> setB = new TraverserSet<>();
        setB.add(createTraverser("c", 0));

        final TraverserSet<String> result = operator.apply(setA, setB);
        assertEquals(3, result.size());
        assertTrue(result.stream().map(Traverser::get).collect(Collectors.toList()).containsAll(List.of("a", "b", "c")));
    }
    
    private B_O_S_SE_SL_Traverser.Admin<String> createTraverser(final String str, final int loops) {
        final Traverser.Admin<String> traverser = new B_NL_O_S_SE_SL_Traverser<>(str, mockStep, 1).asAdmin();
        if (loops > 0) {
            traverser.initialiseLoops("repeatStep1", null);
            for (int i = 0; i < loops; i++) {
                traverser.incrLoops();
            }
        }
        return traverser;
    }
    
    
}
