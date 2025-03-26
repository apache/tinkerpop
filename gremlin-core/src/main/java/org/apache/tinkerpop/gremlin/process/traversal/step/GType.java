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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * An enum that describes types that are used in the Gremlin language.
 */
public enum GType {
    BIG_DECIMAL(BigDecimal.class),
    BIG_INTEGER(BigInteger.class),
    BOOLEAN(Boolean.class),
    BYTE(Byte.class),
    CHARACTER(Character.class),
    DATETIME(OffsetDateTime.class),
    DOUBLE(Double.class),
    FLOAT(Float.class),
    INTEGER(Integer.class),
    LIST(List.class),
    LONG(Long.class),
    MAP(Map.class),
    SET(Set.class),
    SHORT(Short.class),
    STRING(String.class),
    UNKNOWN(null),
    UUID(UUID.class),
    VERTEX(Vertex.class);

    private Class<?> javaType;

    GType(final Class<?> javaType) {
        this.javaType = javaType;
    }

    public Class<?> getJavaType() {
        return this.javaType;
    }

    /**
     * Returns {@code true} if the type is a number.
     */
    public boolean isNumeric() {
        return this == BYTE || this == SHORT || this == INTEGER || this == LONG ||
                 this == FLOAT || this == DOUBLE || this == BIG_INTEGER || this == BIG_DECIMAL;
    }

    /**
     * Returns {@code true} if the type is a collection.v
     */
    public boolean isCollection() {
        return this == LIST || this == SET;
    }

    /**
     * Convert an object to a matching {@link GType} and if not matched return {@link GType#UNKNOWN}.
     */
    public static GType getType(final Object object) {
        if (object instanceof String) return STRING;
        else if (object instanceof Byte) return BYTE;
        else if (object instanceof Short) return SHORT;
        else if (object instanceof Character) return CHARACTER;
        else if (object instanceof Integer) return INTEGER;
        else if (object instanceof Boolean) return BOOLEAN;
        else if (object instanceof Float) return FLOAT;
        else if (object instanceof Double) return DOUBLE;
        else if (object instanceof Long) return LONG;
        else if (object instanceof Map) return MAP;
        else if (object instanceof List) return LIST;
        else if (object instanceof Set) return SET;
        else if (object instanceof Vertex) return VERTEX;
        else if (object instanceof BigInteger) return BIG_INTEGER;
        else if (object instanceof BigDecimal) return BIG_DECIMAL;
        else if (object instanceof OffsetDateTime) return DATETIME;
        else if (object instanceof UUID) return UUID;
        else return UNKNOWN;
    }
}
