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
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author Yang Xia (http://github.com/xiazcy)
 */
public class AsStringLocalStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Collections.singletonList(__.asString(Scope.local));
    }

    @Test
    public void testReturnTypes() {
        assertEquals("1", __.__(1).asString(Scope.local).next());
        assertArrayEquals(new String[]{"1", "2"}, __.inject(1, 2).asString(Scope.local).toList().toArray());
        assertArrayEquals(new String[]{}, ((List<?>) __.__(Collections.emptyList()).asString(Scope.local).next()).toArray());

        assertArrayEquals(new String[]{"1test", "2test"},
                __.__(Arrays.asList(1, 2)).asString(Scope.local).unfold().concat("test").toList().toArray());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenNullInput() {
        __.__(Arrays.asList(1, 2, null)).asString(Scope.local).next();
    }

}
