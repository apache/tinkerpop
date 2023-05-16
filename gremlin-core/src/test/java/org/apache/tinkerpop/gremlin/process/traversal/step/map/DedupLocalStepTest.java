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

import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.apache.tinkerpop.gremlin.util.function.TraverserSetSupplier;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class DedupLocalStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Collections.singletonList(__.dedup(Scope.local));
    }

    @Test
    public void shouldHandlePrimitiveArrays() {
        testDedupLocalStep(new int[]{1, 2, 1}, Arrays.asList(1, 2));
    }

    @Test
    public void shouldHandleObjectArrays() {
        testDedupLocalStep(new Integer[]{1, 2, 1}, Arrays.asList(1, 2));
    }

    @Test
    public void shouldHandleSingleArray() {
        testDedupLocalStep(1, Collections.singletonList(1));
    }

    private void testDedupLocalStep(final Object input, final List expectedResult) {
        final Traversal.Admin traversal = mock(Traversal.Admin.class);
        when(traversal.getTraverserSetSupplier()).thenReturn(TraverserSetSupplier.instance());
        final Traverser.Admin traverser = mock(Traverser.Admin.class);
        when(traverser.get()).thenReturn(input);

        final DedupLocalStep dedupStep = new DedupLocalStep(traversal);
        final Set result = dedupStep.map(traverser);
        assertEquals(new LinkedHashSet(expectedResult), result);
    }
}
