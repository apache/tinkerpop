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

import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.junit.Test;
import org.mockito.internal.util.StringUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SplitGlobalStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Collections.singletonList(__.split("a"));
    }

    @Test
    public void testReturnTypes() {
        assertArrayEquals(new String[]{"h", "llo"}, __.__("hello").split("e").next().toArray());
        assertArrayEquals(new String[]{"ab", "bc"}, __.__("abc|abc").split("c|a").next().toArray());

        assertArrayEquals(new String[]{"helloworld"}, __.__("helloworld").split(null).next().toArray());
        assertArrayEquals(new String[]{"hello", "world"}, __.__("hello world").split(null).next().toArray());
        assertArrayEquals(new String[]{"hello", "world"}, __.__("hello   world").split(null).next().toArray());
        assertArrayEquals(new String[]{"h","e","l","l","o"," ", "w","o","r","l","d"}, __.__("hello world").split("").next().toArray());
        assertArrayEquals(new String[]{"hello", "world"}, __.__("hello world").split(" ").next().toArray());
        assertArrayEquals(new String[]{"hello world"}, __.__("hello world").split("  ").next().toArray());

        List<List<String>> resList = new ArrayList<>();
        resList.add(Arrays.asList("t", "is"));
        resList.add(Arrays.asList("t", "at"));
        resList.add(Collections.singletonList("test"));
        resList.add(null);
        resList.add(Collections.emptyList());
        assertArrayEquals(resList.toArray(),
                __.__("this", "that", "test", null, "").split("h").toList().toArray());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowWithIncomingArrayList() {
        __.__(Arrays.asList("a1a", "b1b", "c1c")).split("1").next();
    }

}
