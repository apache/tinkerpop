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

import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONMapperV1d0ToV2d0BuilderSettingsTest {

    @Test
    public void shouldHandleMapWithTypesUsingEmbedTypeSetting() throws Exception {
        final ObjectMapper mapper = GraphSONMapper.build()
                .version(GraphSONVersion.V1_0)
                .typeInfo(TypeInfo.PARTIAL_TYPES)
                .create()
                .createMapper();

        final Map<String,Object> m = new HashMap<>();
        m.put("test", 100L);

        final String json = mapper.writeValueAsString(m);
        final Map read = mapper.readValue(json, HashMap.class);

        assertEquals(100L, read.get("test"));
    }

    @Test
    public void shouldNotHandleMapWithTypesUsingEmbedTypeSetting() throws Exception {
        final ObjectMapper mapper = GraphSONMapper.build()
                .version(GraphSONVersion.V1_0)
                .typeInfo(TypeInfo.NO_TYPES)
                .create()
                .createMapper();

        final Map<String,Object> m = new HashMap<>();
        m.put("test", 100L);

        final String json = mapper.writeValueAsString(m);
        final Map read = mapper.readValue(json, HashMap.class);

        assertEquals(100, read.get("test"));
    }
}
