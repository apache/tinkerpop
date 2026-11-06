/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.tinkerpop.gremlin.process.traversal.GType;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class PGraphSONSerializerTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"v2", GraphSONMapper.build().version(GraphSONVersion.V2_0).
                        typeInfo(TypeInfo.PARTIAL_TYPES).create().createMapper()},
                {"v3", GraphSONMapper.build().version(GraphSONVersion.V3_0).
                        typeInfo(TypeInfo.PARTIAL_TYPES).create().createMapper()}
        });
    }

    @Parameterized.Parameter(0)
    public String version;

    @Parameterized.Parameter(1)
    public ObjectMapper mapper;

    @Test
    public void shouldRoundTripTypeOfClassAsRegisteredTypeName() throws Exception {
        final P<Object> decoded = roundTrip(P.typeOf(Boolean.class));

        assertEquals("Boolean", decoded.getValue());
        assertTrue(decoded.test(true));
        assertFalse(decoded.test("true"));
    }

    @Test
    public void shouldPreserveTypeOfGTypeAndStringValues() throws Exception {
        assertEquals(GType.BOOLEAN, roundTrip(P.typeOf(GType.BOOLEAN)).getValue());
        assertEquals("Boolean", roundTrip(P.typeOf("Boolean")).getValue());
    }

    @SuppressWarnings("unchecked")
    private P<Object> roundTrip(final P<?> predicate) throws Exception {
        return mapper.readValue(mapper.writeValueAsString(predicate), P.class);
    }
}
