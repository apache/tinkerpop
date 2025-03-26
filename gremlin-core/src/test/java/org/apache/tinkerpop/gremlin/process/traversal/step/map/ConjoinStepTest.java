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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConjoinStepTest extends StepTest {
    @Override
    protected List<Traversal> getTraversals() { return Collections.singletonList(__.conjoin("a")); }

    @Test
    public void testReturnTypes() {
        try {
            __.__(Collections.emptyList()).conjoin((String) null).next();
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Input delimiter to conjoin step can't be null"));
        }

        assertEquals("", __.__(Collections.emptyList()).conjoin("a").next());
        assertEquals("5AA8AA10", __.__(new long[] {5L, 8L, 10L}).conjoin("AA").next());
        assertEquals("715", __.__(1).constant(new Long[] {7L, 15L}).conjoin("").next());
        assertEquals("5.5,8.0,10.1", __.__(new double[] {5.5, 8.0, 10.1}).conjoin(",").next());
        assertNull(__.__(Arrays.asList(null, null)).conjoin(",").next());

        final Set<Integer> set = new LinkedHashSet<>();
        set.add(10); set.add(11); set.add(12);
        assertEquals("10.11.12", __.__(set).conjoin(".").next());
    }
}
