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

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AsBoolStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Collections.singletonList(__.asBool());
    }

    @Test
    public void shouldParseBoolean() {
        assertEquals(true, __.__(-1).asBool().next());
        assertEquals(false, __.__(0).asBool().next());
        assertEquals(true, __.__(1).asBool().next());
        assertEquals(true, __.__(3.14).asBool().next());
        assertEquals(true, __.__(new BigInteger("1000000000000000000000")).asBool().next());
        assertEquals(true, __.__("TRUE").asBool().next());
        assertEquals(false, __.__("false").asBool().next());
        assertNull(__.__(Double.NaN).asBool().next());
        assertNull(__.__((Object) null).asBool().next());
        assertNull(__.__("hello world").asBool().next());
        assertNull(__.__("1").asBool().next());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidString() {
        __.__(Arrays.asList(1, 2)).asBool().next();
    }
}
