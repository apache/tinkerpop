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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.util.tools.CollectionFactory;
import org.junit.Test;

import java.util.Map;

public class MergeVertexStepTest {

    @Test
    public void shouldValidateWithTokens() {
        final Map<Object,Object> m = CollectionFactory.asMap("k", "v",
                T.label, "person",
                T.id, 10000);
        MergeVertexStep.validateMapInput(m, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithTokensBecauseOfValue() {
        final Map<Object,Object> m = CollectionFactory.asMap("k", "v",
                T.value, "nope",
                T.label, "person",
                T.id, 10000);
        MergeVertexStep.validateMapInput(m, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithTokensBecauseOfWeirdKey() {
        final Map<Object,Object> m = CollectionFactory.asMap("k", "v",
                Direction.IN, "weird",
                T.id, 10000);
        MergeVertexStep.validateMapInput(m, false);
    }

    @Test
    public void shouldValidateWithoutTokens() {
        final Map<Object,Object> m = CollectionFactory.asMap("k", "v");
        MergeVertexStep.validateMapInput(m, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithoutTokens() {
        final Map<Object,Object> m = CollectionFactory.asMap("k", "v",
                Direction.IN, 101,
                Direction.BOTH, new ReferenceVertex(100),
                T.label, "knows",
                T.id, 10000);
        MergeVertexStep.validateMapInput(m, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithoutTokensBecauseOfWeirdKey() {
        final Map<Object,Object> m = CollectionFactory.asMap("k", "v",
                new ReferenceVertex("weird"), 100000);
        MergeVertexStep.validateMapInput(m, true);
    }
}
