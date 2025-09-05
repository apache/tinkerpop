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
package org.apache.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.util.NumberHelper;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Gremlin types
 */
public enum GType implements GremlinDataType {
    BIG_DECIMAL(BigDecimal.class),
    BIG_INTEGER(BigInteger.class),
    BOOLEAN(Boolean.class),
    BYTE(Byte.class),
    CHARACTER(Character.class),
    DATETIME(OffsetDateTime.class),
    DOUBLE(Double.class),
    FLOAT(Float.class),
    INT(int.class),
    INTEGER(Integer.class),
    LIST(List.class),
    LONG(Long.class),
    MAP(Map.class),
    NUMBER(Number.class),
    SET(Set.class),
    SHORT(Short.class),
    STRING(String.class),
    UNKNOWN(null),
    UUID(UUID.class),
    VERTEX(Vertex.class),;

    private final Class<?> javaType;

    GType(Class<?> javaType) {
        this.javaType = javaType;
    }

    @Override
    public String getName() { return this.name(); }

    @Override
    public Class<?> getType() { return javaType; }

    @Override
    public GremlinDataType fromName(String name) { return GType.valueOf(name.toUpperCase()); }

}
