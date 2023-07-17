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
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.tinkerpop.shaded.jackson.core.exc.StreamConstraintsException;
import org.apache.tinkerpop.shaded.jackson.databind.JsonMappingException;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class GraphSONStreamConstraintsTest extends AbstractGraphSONTest{

    final ObjectMapper defaultMapper = GraphSONMapper.build().create().createMapper();

    @Test
    public void testMaxNumberLengthConfig() throws Exception {
        final int serializedData = 1000;

        final ObjectMapper mapper = GraphSONMapper.build().maxNumberLength(2).create().createMapper();

        // should pass with default serializer config
        assertEquals((Integer)serializedData, serializeDeserializeAuto(defaultMapper, serializedData));

        // should fail with small limit config
        final Exception exception = assertThrows(JsonMappingException.class, () -> {
            serializeDeserializeAuto(mapper, serializedData);
        });
        assertTrue("Expected StreamConstraintsException for exceeding max number length, found: "+exception.getMessage(),
                exception.getMessage().contains("org.apache.tinkerpop.shaded.jackson.core.exc.StreamConstraintsException: Number length")
                && exception.getMessage().contains("exceeds the maximum length (2)"));
    }

    @Test
    public void testMaxStringLengthConfig() throws Exception {
        final String serializedData = "This string is more than 20 chars long";

        final ObjectMapper mapper = GraphSONMapper.build().maxStringLength(20).create().createMapper();

        // should pass with default serializer config
        assertEquals(serializedData, serializeDeserializeAuto(defaultMapper, serializedData));

        // should fail with small limit config
        final Exception exception = assertThrows(StreamConstraintsException.class, () -> {
            serializeDeserializeAuto(mapper, serializedData);
        });
        assertTrue("Expected StreamConstraintsException for exceeding max String length, found: "+exception.getMessage(),
                exception.getMessage().contains("String length")
                        && exception.getMessage().contains("exceeds the maximum length (20)"));
    }

    @Test
    public void testMaxNestingDepthConfig() throws Exception {
        final Map<String, Object> serializedData = new HashMap<>();
        serializedData.put(
                "key1", new HashMap<>().put(
                        "key2", new HashMap<>().put(
                                "key3", "val1")));
        final ObjectMapper mapper = GraphSONMapper.build().maxNestingDepth(1).create().createMapper();

        // should pass with default serializer config
        assertEquals(serializedData, serializeDeserializeAuto(defaultMapper, serializedData));

        // should fail with small limit config
        final Exception exception = assertThrows(JsonMappingException.class, () -> {
            serializeDeserializeAuto(mapper, serializedData);
        });
        assertTrue("Expected StreamConstraintsException for exceeding max nesting depth,  found: "+exception.getMessage(),
                exception.getMessage().contains("org.apache.tinkerpop.shaded.jackson.core.exc.StreamConstraintsException: Depth")
                        && exception.getMessage().contains("exceeds the maximum allowed nesting depth (1)"));
    }
}
