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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(Enclosed.class)
public class ContainsBulkSetTest {

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void bulkSetElements() {
        final BulkSet<Object> bulkSet = Mockito.spy(new BulkSet<>());
        bulkSet.add(new ReferenceVertex("1"), 1);
        bulkSet.add(new ReferenceVertex("2"), 1);
        bulkSet.add(new ReferenceVertex("3"), 1);

        // If bulkset contains only one type of element, we should use contains
        assertEquals(true, Contains.within.test(new ReferenceVertex("3"), bulkSet));
        assertEquals(false, Contains.within.test(new ReferenceVertex("4"), bulkSet));
        verify(bulkSet, times(2)).contains(any());
        // we should also not use contains if we test for different type
        assertEquals(false, Contains.within.test(new ReferenceVertexProperty<>("2", "label", "value"), bulkSet));
        verify(bulkSet, times(2)).contains(any());

        // If bulkset contains different types of elements, we can no longer use contains
        bulkSet.add("test string", 1);
        assertEquals(true, Contains.within.test(new ReferenceVertex("3"), bulkSet));
        assertEquals(false, Contains.within.test(new ReferenceVertex("4"), bulkSet));
        assertEquals(false, Contains.within.test(new ReferenceVertexProperty<>("2", "label", "value"), bulkSet));
        verify(bulkSet, times(2)).contains(any());
    }
}
