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
package org.apache.tinkerpop.gremlin.process.traversal.lambda;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ConstantTraversalTest {

    @Test
    public void shouldHaveSameHashCode() {
        final ConstantTraversal<Object,String> t1 = new ConstantTraversal<>("100");
        final ConstantTraversal<Object,String> t2 = new ConstantTraversal<>("100");
        assertEquals(t1.hashCode(), t2.hashCode());
    }

    @Test
    public void shouldNotHaveSameHashCode() {
        final ConstantTraversal<Object,String> t1 = new ConstantTraversal<>("100");
        final ConstantTraversal<Object,String> t2 = new ConstantTraversal<>("101");
        assertNotEquals(t1.hashCode(), t2.hashCode());
    }

    @Test
    public void shouldHaveSameHashCodeForNull() {
        final ConstantTraversal<Object,String> t1 = new ConstantTraversal<>(null);
        final ConstantTraversal<Object,String> t2 = new ConstantTraversal<>(null);
        assertEquals(t1.hashCode(), t2.hashCode());
    }
}
