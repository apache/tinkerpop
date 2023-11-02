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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SubstringLocalStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.substring(Scope.local, 1),
                __.substring(Scope.local, 1, 2));
    }

    @Test
    public void testReturnTypes() {
        assertEquals("hello world", __.__("hello world").substring(Scope.local, 0).next());
        assertEquals("ello world", __.__("hello world").substring(Scope.local, 1).next());
        assertEquals("d", __.__("hello world").substring(Scope.local, -1).next());

        assertEquals("", __.__("hello world").substring(Scope.local, 0, 0).next());
        assertEquals("ello worl", __.__("hello world").substring(Scope.local, 1, -1).next());
        assertEquals("", __.__("hello world").substring(Scope.local, -1, -3).next());
        assertEquals("hello world", __.__("hello world").substring(Scope.local, -11, 11).next());

        assertArrayEquals(new String[]{"st", "llo wo", null, ""},
                ((List<?>) __.__(Arrays.asList("test", "hello world", null, "")).substring(Scope.local, 2, 8).next()).toArray());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowWithIncomingNonStringArrayList() {
        __.__(Arrays.asList(1, 2, 3)).substring(Scope.local, 1).next();
    }

}
