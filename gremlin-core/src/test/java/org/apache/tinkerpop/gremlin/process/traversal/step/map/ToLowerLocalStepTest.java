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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author Yang Xia (http://github.com/xiazcy)
 */
public class ToLowerLocalStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Collections.singletonList(__.toLower(Scope.local));
    }

    @Test
    public void testReturnTypes() {
        assertEquals("hello", __.__("HELLO").toLower(Scope.local).next());
        assertArrayEquals(new String[]{"test"}, ((List<?>) __.__(Collections.singletonList("TEST")).toLower(Scope.local).next()).toArray());
        assertArrayEquals(new String[]{"test"}, ((List<?>) __.__(new HashSet<>(Collections.singletonList("TEST"))).toLower(Scope.local).next()).toArray());
        assertArrayEquals(new String[]{"hello", "test", "no.123", null, ""},
                ((List<?>) __.__(Arrays.asList("hElLo", "TEST", "NO.123", null, "")).toLower(Scope.local).next()).toArray());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowWithIncomingNonStringList() {
        __.__(Arrays.asList(1, 2, 3)).toLower(Scope.local).next();
    }

}
