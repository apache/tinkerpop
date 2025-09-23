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

/**
 * @author Yang Xia (http://github.com/xiazcy)
 */
public class AsStringGlobalStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Collections.singletonList(__.asString());
    }

    @Test
    public void testReturnTypes() {
        assertEquals("1", __.__(1).asString().next());
        assertEquals("[]", __.__(Collections.emptyList()).asString().next());
        assertEquals("[1, 2]", __.__(Arrays.asList(1, 2)).asString().next());
        assertEquals("1", __.__(Arrays.asList(1, 2)).unfold().asString().next());
        assertArrayEquals(new String[]{"1", "2"}, __.inject(Arrays.asList(1, 2)).unfold().asString().toList().toArray());

        assertEquals("[1, 2]test", __.__(Arrays.asList(1, 2)).asString().concat("test").next());
        assertEquals("1test", __.__(Arrays.asList(1, 2)).unfold().asString().concat("test").next());
        assertArrayEquals(new String[]{"1test", "2test"},
                __.__(Arrays.asList(1, 2)).unfold().asString().concat("test").toList().toArray());
        assertArrayEquals(new String[]{"1test", "2test"},
                __.__(Arrays.asList(1, 2)).unfold().asString().concat("test").fold().next().toArray());
    }


    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenNullInput() {
        __.__(null).asString().next();
    }

}
