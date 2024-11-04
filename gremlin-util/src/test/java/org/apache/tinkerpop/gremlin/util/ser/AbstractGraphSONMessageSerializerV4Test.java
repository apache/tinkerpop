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
package org.apache.tinkerpop.gremlin.util.ser;

import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.shaded.jackson.core.StreamReadConstraints;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AbstractGraphSONMessageSerializerV4Test {
    @Test
    public void shouldApplyMaxTokenLengthsOnConfigure() {
        // Initialize bare-bones AbstractGraphSONMessageSerializerV2
        final AbstractGraphSONMessageSerializerV4 serializer = new AbstractGraphSONMessageSerializerV4() {
            @Override GraphSONMapper.Builder configureBuilder(GraphSONMapper.Builder builder) { return builder;}
            @Override public String[] mimeTypesSupported() {return new String[0]; }
        };

        final Map<String, Object> config = new HashMap<>();
        config.put("maxNumberLength", 999);
        config.put("maxStringLength", 12345);
        config.put("maxNestingDepth", 55);

        serializer.configure(config, null);

        final StreamReadConstraints constraints = serializer.getMapper().getFactory().streamReadConstraints();

        assertEquals(999, constraints.getMaxNumberLength());
        assertEquals(12345, constraints.getMaxStringLength());
        assertEquals(55, constraints.getMaxNestingDepth());
    }
}
