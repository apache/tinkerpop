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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.geo.Geometry;
import org.apache.tinkerpop.gremlin.structure.geo.GeometryCollection;
import org.apache.tinkerpop.gremlin.structure.geo.LineString;
import org.apache.tinkerpop.gremlin.structure.geo.Model;
import org.apache.tinkerpop.gremlin.structure.geo.MultiLineString;
import org.apache.tinkerpop.gremlin.structure.geo.MultiPoint;
import org.apache.tinkerpop.gremlin.structure.geo.MultiPolygon;
import org.apache.tinkerpop.gremlin.structure.geo.Point;
import org.apache.tinkerpop.gremlin.structure.geo.Polygon;

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
 * <p>
 * Most constants name a Java class and match any instance of it. {@code CARTESIAN} and
 * {@code GEOGRAPHIC} instead match a geometry by its coordinate model, so they test the value
 * rather than only its class.
 * </p>
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
    CARTESIAN(Geometry.class, Model.CARTESIAN),
    CHAR(Character.class),
    DATETIME(OffsetDateTime.class),
    DOUBLE(Double.class),
    DURATION(Duration.class),
    EDGE(Edge.class),
    FLOAT(Float.class),
    GEOGRAPHIC(Geometry.class, Model.GEOGRAPHIC),
    GEOMETRY(Geometry.class),
    GEOMETRYCOLLECTION(GeometryCollection.class),
    GRAPH(Graph.class),
    INT(Integer.class),
    LINESTRING(LineString.class),
    LIST(List.class),
    LONG(Long.class),
    MAP(Map.class),
    MULTILINESTRING(MultiLineString.class),
    MULTIPOINT(MultiPoint.class),
    MULTIPOLYGON(MultiPolygon.class),
    NULL(null),
    NUMBER(Number.class),
    PATH(Path.class),
    POINT(Point.class),
    POLYGON(Polygon.class),
    PROPERTY(Property.class),
    SET(Set.class),
    SHORT(Short.class),
    STRING(String.class),
    TREE(Tree.class),
    UUID(UUID.class),
    VERTEX(Vertex.class),
    VPROPERTY(VertexProperty.class),;

    private final Class<?> javaType;

    /**
     * The coordinate model a geometry must have to match this type. Only {@link #CARTESIAN}
     * and {@link #GEOGRAPHIC} set it; every other type has null and matches by class alone.
     */
    private final Model requiredModel;

    GType(final Class<?> javaType) {
        this(javaType, null);
    }

    GType(final Class<?> javaType, final Model requiredModel) {
        this.javaType = javaType;
        this.requiredModel = requiredModel;
    }

    public Class<?> getType() { return javaType; }

    public boolean isNumeric() {
        final Class<?> type = getType();
        return Number.class.isAssignableFrom(type);
    }

    public static Class<?> getType(final String name) {
        return valueOf(name).javaType;
    }

    /**
     * Returns the coordinate model a geometry must have to match this type, or null when the
     * type matches by class alone. Only {@link #CARTESIAN} and {@link #GEOGRAPHIC} return a
     * model.
     */
    Model requiredModel() {
        return requiredModel;
    }

    /**
     * Returns true if the value is of this type. A type with a required model matches a geometry
     * in that model, and every other type matches an instance of its class.
     */
    boolean matches(final Object value) {
        if (value == null) {
            return javaType == null;
        }
        if (requiredModel != null) {
            return value instanceof Geometry && ((Geometry) value).model() == requiredModel;
        }
        return javaType != null && javaType.isAssignableFrom(value.getClass());
    }

}
