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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Yang Xia (http://github.com/xiazcy)
 */
public class ReverseStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {return Collections.singletonList(__.reverse());}

    @Test
    public void testReturnTypes() {
        assertEquals(Collections.emptyList(), __.__(Collections.emptyList()).reverse().next());
        assertEquals("tset", __.__("test").reverse().next());
        assertArrayEquals(new String[]{"dlrow olleh", "tset", "321.on", null, ""},
                __.inject("hello world", "test", "no.123", null, "").reverse().toList().toArray());
    }

    @Test
    public void shouldAcceptPrimitiveArrayTraverser() {
        List result = (List) __.__(new long[] {10L, 7L}).reverse().next();
        assertEquals(7L, result.get(0));
        assertEquals(10L, result.get(1));
        assertEquals(2, result.size());
    }

    @Test
    public void shouldAcceptObjectArrayTraverser() {
        List result = (List) __.__(Arrays.asList(2, "hello", 10L)).reverse().next();
        assertArrayEquals(new Object[] {10L, "hello", 2}, result.toArray());
    }

}
