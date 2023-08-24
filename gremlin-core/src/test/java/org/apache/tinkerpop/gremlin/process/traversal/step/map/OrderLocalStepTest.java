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

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.apache.tinkerpop.gremlin.util.function.TraverserSetSupplier;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class OrderLocalStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.order(Scope.local),
                __.order(Scope.local).by(Order.desc),
                __.order(Scope.local).by("age", Order.asc),
                __.order(Scope.local).by("age", Order.desc),
                __.order(Scope.local).by(outE().count(), Order.asc),
                __.order(Scope.local).by("age", Order.asc).by(outE().count(), Order.asc),
                __.order(Scope.local).by(outE().count(), Order.asc).by("age", Order.asc)
        );
    }

    @Test
    public void shouldNotThrowContractException() {
        for (int x = 0; x < 1000; x++) {
            final List<Integer> list = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                list.add(i);
            }
            __.inject(list).order(Scope.local).by(Order.shuffle).iterate();
            __.inject(list).order(Scope.local).by().by(Order.shuffle).iterate();
            __.inject(list).order(Scope.local).by(Order.shuffle).by().iterate();
            __.inject(list).order(Scope.local).by(__.identity(), Order.shuffle).iterate();
            __.inject(list).order(Scope.local).by().by(__.identity(), Order.shuffle).iterate();
            __.inject(list).order(Scope.local).by(__.identity(), Order.shuffle).by().iterate();
        }
    }

    @Test
    public void shouldHandleSingleValue() {
        testOrderLocalStep(1, 1);
    }

    @Test
    public void shouldHandleEmptyArray() {
        testOrderLocalStep(new int[0], new ArrayList<>());
    }

    @Test
    public void shouldHandlePrimitiveArray() {
        testOrderLocalStep(new int[] {2,-1,3}, Arrays.asList(-1,2,3));
    }

    @Test
    public void shouldHandleObjectArray() {
        testOrderLocalStep(new Integer[] {2,1,3}, Arrays.asList(1,2,3));
    }

    private void testOrderLocalStep(final Object input, final Object expectedResult) {
        final Traversal.Admin traversal = mock(Traversal.Admin.class);
        when(traversal.getTraverserSetSupplier()).thenReturn(TraverserSetSupplier.instance());
        final Traverser.Admin traverser = mock(Traverser.Admin.class);
        when(traverser.get()).thenReturn(input);

        final OrderLocalStep orderStep = new OrderLocalStep<>(traversal);
        final Object result = orderStep.map(traverser);
        assertEquals(expectedResult, result);
    }
}
