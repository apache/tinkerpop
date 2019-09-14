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
package org.apache.tinkerpop.gremlin.structure.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Test;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphVariableHelperTest {
    @Test
    public void shouldBeUtilityClass() throws Exception {
        TestHelper.assertIsUtilityClass(GraphVariableHelper.class);
    }

    @Test
    public void shouldThrowIfValueIsNull() {
        try {
            GraphVariableHelper.validateVariable("test", null);
            fail("Should have thrown an exception when value is null");
        } catch (Exception iae) {
            final Exception expected = Graph.Variables.Exceptions.variableValueCanNotBeNull();
            assertEquals(expected.getClass(), iae.getClass());
            assertEquals(expected.getMessage(), iae.getMessage());
        }
    }

    @Test
    public void shouldThrowIfKeyIsNull() {
        try {
            GraphVariableHelper.validateVariable(null, "test");
            fail("Should have thrown an exception when key is null");
        } catch (Exception iae) {
            final Exception expected = Graph.Variables.Exceptions.variableKeyCanNotBeNull();
            assertEquals(expected.getClass(), iae.getClass());
            assertEquals(expected.getMessage(), iae.getMessage());
        }
    }

    @Test
    public void shouldThrowIfKeyIsEmpty() {
        try {
            GraphVariableHelper.validateVariable("", "test");
            fail("Should have thrown an exception when key is empty");
        } catch (Exception iae) {
            final Exception expected = Graph.Variables.Exceptions.variableKeyCanNotBeEmpty();
            assertEquals(expected.getClass(), iae.getClass());
            assertEquals(expected.getMessage(), iae.getMessage());
        }
    }
}
