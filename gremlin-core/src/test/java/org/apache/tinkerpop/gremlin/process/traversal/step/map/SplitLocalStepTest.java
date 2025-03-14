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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class SplitLocalStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Collections.singletonList(__.split(Scope.local, "a"));
    }

    @Test
    public void testReturnTypes() {
        assertArrayEquals(new String[]{"h", "llo"}, __.__("hello").split(Scope.local, "e").next().toArray());
        assertArrayEquals(new String[]{"ab", "bc"}, __.__("abc|abc").split(Scope.local, "c|a").next().toArray());
        assertArrayEquals(new String[]{"t","e","s","t"}, __.__("test").split(Scope.local, "").next().toArray());

        List<List<String>> resList = new ArrayList<>();
        resList.add(Arrays.asList("helloworld"));
        resList.add(Arrays.asList("hello", "world"));
        resList.add(Arrays.asList("hello", "world"));
        resList.add(null);
        assertArrayEquals(resList.toArray(),
                __.__(Arrays.asList("helloworld", "hello world", "hello   world", null)).split(Scope.local, null).next().toArray());

        resList = new ArrayList<>();
        resList.add(Arrays.asList("t", "is"));
        resList.add(Arrays.asList("t", "at"));
        resList.add(Collections.singletonList("test"));
        resList.add(null);
        resList.add(Collections.emptyList());
        assertArrayEquals(resList.toArray(),
                __.__(Arrays.asList("this", "that", "test", null, "")).split(Scope.local, "h").next().toArray());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowWithIncomingNonStringArrayList() {
        __.__(Arrays.asList(1, 2, 2)).split(Scope.local, "1").next();
    }

}
