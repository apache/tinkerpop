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

public class SubstringStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.substring(1),
                __.substring(1, 2));
    }

    @Test
    public void testReturnTypes() {
        assertEquals("ello wor", __.__("hello world").substring(1, 8).next());
        assertEquals("ello", __.__("hello").substring(1, 8).next());
        assertEquals("world", __.__("hello world").substring(6).next());

        assertEquals("world", __.__("world").substring(-10).next());
        assertEquals("orld", __.__("world").substring(-4).next());
        assertEquals("orl", __.__("world").substring(-4, 3).next());
        assertEquals("", __.__("world").substring(1, -1).next());

        assertArrayEquals(new String[]{"st", "llo worl", null, ""},
                __.__("test", "hello world", null, "").substring(2, 8).toList().toArray());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowWithIncomingArrayList() {
        __.__(Arrays.asList("aa", "bb", "cc")).substring(1).next();
    }

}
