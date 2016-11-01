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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class UnfoldStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Collections.singletonList(__.unfold());
    }

    @Test
    public void shouldSupportArrayIteration() {
        final Traversal<String[], String> t1 = __.<String[]>inject(new String[]{"a", "b", "c"}).unfold();
        assertEquals("a", t1.next());
        assertEquals("b", t1.next());
        assertEquals("c", t1.next());
        assertFalse(t1.hasNext());
        //
        final Traversal<Object[], Object> t2 = __.<Object[]>inject(new Object[]{"a", "b", 3}).unfold();
        assertEquals("a", t2.next());
        assertEquals("b", t2.next());
        assertEquals(3, t2.next());
        assertFalse(t2.hasNext());
    }
}
