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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Enumeration of Gremlin data types used for type checking and filtering operations.
 * <p>
 * Each {@code GType} constant represents a specific Java type that can be encountered
 * during graph traversals. This enum is primarily used with the {@code typeOf()} predicate
 * to filter values based on their runtime type.
 * 
 * <h3>Usage Examples:</h3>
 * <pre>{@code
 * // Filter vertices by checking property types
 * g.V().has("age", P.typeOf(GType.INT))
 * 
 * // Filter values by string type
 * g.V().values("name").is(P.typeOf(GType.STRING))
 * 
 * // Check for numeric types
 * g.V().values().is(P.typeOf(GType.NUMBER))
 * }</pre>
 * 
 * @see org.apache.tinkerpop.gremlin.process.traversal.P#typeOf(GType)
 * @since 3.8.0
 */
public enum GType {
    BIGDECIMAL(BigDecimal.class),
    BIGINT(BigInteger.class),
    BINARY(ByteBuffer.class),
    BOOLEAN(Boolean.class),
    BYTE(Byte.class),
    CHAR(Character.class),
    DATETIME(OffsetDateTime.class),
    DOUBLE(Double.class),
    DURATION(Duration.class),
    EDGE(Edge.class),
    FLOAT(Float.class),
    GRAPH(Graph.class),
    INT(Integer.class),
    LIST(List.class),
    LONG(Long.class),
    MAP(Map.class),
    NULL(null),
    NUMBER(Number.class),
    PATH(Path.class),
    PROPERTY(Property.class),
    SET(Set.class),
    SHORT(Short.class),
    STRING(String.class),
    TREE(Tree.class),
    UUID(UUID.class),
    VERTEX(Vertex.class),
    VPROPERTY(VertexProperty.class),;

    private final Class<?> javaType;

    GType(final Class<?> javaType) {
        this.javaType = javaType;
    }

    public Class<?> getType() { return javaType; }

    public boolean isNumeric() {
        final Class<?> type = getType();
        return Number.class.isAssignableFrom(type);
    }

    public static Class<?> getType(final String name) {
        return valueOf(name).javaType;
    }

}
