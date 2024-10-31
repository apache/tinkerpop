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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Version 4.0 of GraphSON extensions.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GraphSONXModuleV4 extends GraphSONModule {

    private static final Map<Class, String> TYPE_DEFINITIONS = Collections.unmodifiableMap(
            new LinkedHashMap<Class, String>() {{
                put(ByteBuffer.class, "Binary");
                put(Short.class, "Int16");
                put(BigInteger.class, "BigInteger");
                put(BigDecimal.class, "BigDecimal");
                put(Byte.class, "Byte");
                put(Character.class, "Char");

                // Time serializers/deserializers
                put(Duration.class, "Duration");
                put(OffsetDateTime.class, "DateTime");
            }});

    /**
     * Constructs a new object.
     */
    protected GraphSONXModuleV4(final boolean normalize) {
        super("graphsonx-4.0");

        /////////////////////// SERIALIZERS ////////////////////////////

        // java.time
        addSerializer(Duration.class, new JavaTimeSerializersV4.DurationJacksonSerializer());
        addSerializer(OffsetDateTime.class, new JavaTimeSerializersV4.OffsetDateTimeJacksonSerializer());

        /////////////////////// DESERIALIZERS ////////////////////////////

        // java.time
        addDeserializer(Duration.class, new JavaTimeSerializersV4.DurationJacksonDeserializer());
        addDeserializer(OffsetDateTime.class, new JavaTimeSerializersV4.OffsetDateTimeJacksonDeserializer());
    }

    public static GraphSONModuleBuilder build() {
        return new Builder();
    }

    @Override
    public Map<Class, String> getTypeDefinitions() {
        return TYPE_DEFINITIONS;
    }

    @Override
    public String getTypeNamespace() {
        return GraphSONTokens.GREMLIN_TYPE_NAMESPACE;
    }

    public static final class Builder implements GraphSONModuleBuilder {

        private Builder() {
        }

        @Override
        public GraphSONModule create(final boolean normalize, final TypeInfo typeInfo) {
            return new GraphSONXModuleV4(normalize);
        }
    }
}