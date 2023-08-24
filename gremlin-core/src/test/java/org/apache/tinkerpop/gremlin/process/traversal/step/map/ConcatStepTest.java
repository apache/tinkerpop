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
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ConcatStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Collections.singletonList(__.concat("test"));
    }

    @Test
    public void testReturnTypes() {
        assertEquals("abc", __.__("a").concat("b", "c").next());
        assertEquals("ab", __.__("a").concat(__.inject("b", "c")).next());
        assertEquals("ab", __.__("a").concat(__.inject(Arrays.asList("b", "c")).unfold()).next());
        assertEquals("", __.__("").concat("").next());

        assertArrayEquals(new String[]{"a", "b", "c"},
                __.__("a", "b", "c").concat().toList().toArray());
        assertArrayEquals(new String[]{"ade", "bde", "cde"},
                __.__("a", "b", "c").concat("d", "e").toList().toArray());
        assertArrayEquals(new String[]{"ad", "be", "c"},
                __.__("a", "b", "c").concat(__.inject("d", "e")).toList().toArray());

        assertArrayEquals(new String[]{"Mr.a", "Mr.b", "Mr.c", "Mr.d"},
                __.__("a", "b", "c", "d").as("letters").constant("Mr.").concat(__.select("letters")).toList().toArray());

        String nullStr = null;
        assertNull(__.inject(null).concat(nullStr).next());
        assertArrayEquals(new String[]{"a", "b", null},
                __.__("a", "b", null).concat(nullStr).toList().toArray());
        assertArrayEquals(new String[]{"acd", "bcd", "cd"},
                __.__("a", "b", null).concat("c", "d", nullStr).toList().toArray());

    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowWithIncomingArrayList() {
        __.__(Arrays.asList("a", "b", "c")).concat("d").next();
    }

}
