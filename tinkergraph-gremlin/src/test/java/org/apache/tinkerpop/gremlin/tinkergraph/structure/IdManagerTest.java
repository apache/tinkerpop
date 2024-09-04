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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IdManagerTest {
    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void shouldGenerateNiceErrorOnConversionOfStringToInt() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Expected an id that is convertible to");
        final TinkerGraph.IdManager manager = TinkerGraph.DefaultIdManager.INTEGER;
        manager.convert("string-id");
    }

    @Test
    public void shouldGenerateNiceErrorOnConversionOfJunkToInt() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Expected an id that is convertible to");
        final TinkerGraph.IdManager manager = TinkerGraph.DefaultIdManager.INTEGER;
        manager.convert(UUID.randomUUID());
    }

    @Test
    public void shouldGenerateNiceErrorOnConversionOfStringToLong() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Expected an id that is convertible to");
        final TinkerGraph.IdManager manager = TinkerGraph.DefaultIdManager.LONG;
        manager.convert("string-id");
    }

    @Test
    public void shouldGenerateNiceErrorOnConversionOfJunkToLong() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Expected an id that is convertible to");
        final TinkerGraph.IdManager manager = TinkerGraph.DefaultIdManager.LONG;
        manager.convert(UUID.randomUUID());
    }

    @Test
    public void shouldGenerateNiceErrorOnConversionOfStringToUUID() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Expected an id that is convertible to");
        final TinkerGraph.IdManager manager = TinkerGraph.DefaultIdManager.UUID;
        manager.convert("string-id");
    }

    @Test
    public void shouldGenerateNiceErrorOnConversionOfJunkToUUID() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Expected an id that is convertible to");
        final TinkerGraph.IdManager manager = TinkerGraph.DefaultIdManager.UUID;
        manager.convert(Double.NaN);
    }

    @Test
    public void shouldGenerateNiceErrorOnConversionOfJunkToString() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Expected an id that is convertible to");
        final TinkerGraph.IdManager manager = TinkerGraph.DefaultIdManager.STRING;
        manager.convert(Double.NaN);
    }

    @Test
    public void shouldGenerateNiceErrorOnConversionOfUUIDToString() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Expected an id that is convertible to");
        final TinkerGraph.IdManager manager = TinkerGraph.DefaultIdManager.STRING;
        manager.convert(UUID.randomUUID());
    }

    @Test
    public void shouldGenerateNiceErrorOnConversionOfEmptyStringToString() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Expected a non-empty string but received an empty string.");
        final TinkerGraph.IdManager manager = TinkerGraph.DefaultIdManager.STRING;
        manager.convert("");
    }
}
