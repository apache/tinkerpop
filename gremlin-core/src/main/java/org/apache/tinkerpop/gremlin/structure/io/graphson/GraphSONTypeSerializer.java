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

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonTypeInfo;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.databind.BeanProperty;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeIdResolver;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Extension of the Jackson's default TypeSerializer. An instance of this object will be passed to the serializers
 * on which they can safely call the utility methods to serialize types and making it compatible with the version
 * 2.0 of GraphSON.
 *
 * @author Kevin Gallardo (https://kgdo.me)
 */
public class GraphSONTypeSerializer extends TypeSerializer {

    private final TypeIdResolver idRes;
    private final String propertyName;
    private final TypeInfo typeInfo;
    private final String valuePropertyName;

    GraphSONTypeSerializer(TypeIdResolver idRes, String propertyName, TypeInfo typeInfo, String valuePropertyName) {
        this.idRes = idRes;
        this.propertyName = propertyName;
        this.typeInfo = typeInfo;
        this.valuePropertyName = valuePropertyName;
    }

    @Override
    public TypeSerializer forProperty(BeanProperty beanProperty) {
        return this;
    }

    @Override
    public JsonTypeInfo.As getTypeInclusion() {
        return null;
    }

    @Override
    public String getPropertyName() {
        return propertyName;
    }

    @Override
    public TypeIdResolver getTypeIdResolver() {
        return idRes;
    }

    @Override
    public void writeTypePrefixForScalar(Object o, JsonGenerator jsonGenerator) throws IOException {
        if (canWriteTypeId()) {
            writeTypePrefix(jsonGenerator, getTypeIdResolver().idFromValueAndType(o, getClassFromObject(o)));
        }
    }

    @Override
    public void writeTypePrefixForObject(Object o, JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeStartObject();
        // TODO: FULL_TYPES should be implemented here as : if (fullTypesModeEnabled()) writeTypePrefix(Map);
    }

    @Override
    public void writeTypePrefixForArray(Object o, JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeStartArray();
        // TODO: FULL_TYPES should be implemented here as : if (fullTypesModeEnabled()) writeTypePrefix(List);
    }

    @Override
    public void writeTypeSuffixForScalar(Object o, JsonGenerator jsonGenerator) throws IOException {
        if (canWriteTypeId()) {
            writeTypeSuffix(jsonGenerator);
        }
    }

    @Override
    public void writeTypeSuffixForObject(Object o, JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeEndObject();
        // TODO: FULL_TYPES should be implemented here as : if (fullTypesModeEnabled()) writeTypeSuffix(Map);
    }

    @Override
    public void writeTypeSuffixForArray(Object o, JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeEndArray();
        // TODO: FULL_TYPES should be implemented here as : if (fullTypesModeEnabled()) writeTypeSuffix(List);
    }

    @Override
    public void writeCustomTypePrefixForScalar(Object o, JsonGenerator jsonGenerator, String s) throws IOException {
        if (canWriteTypeId()) {
            writeTypePrefix(jsonGenerator, s);
        }
    }

    @Override
    public void writeCustomTypePrefixForObject(Object o, JsonGenerator jsonGenerator, String s) throws IOException {
        jsonGenerator.writeStartObject();
        // TODO: FULL_TYPES should be implemented here as : if (fullTypesModeEnabled()) writeTypePrefix(s);
    }

    @Override
    public void writeCustomTypePrefixForArray(Object o, JsonGenerator jsonGenerator, String s) throws IOException {
        jsonGenerator.writeStartArray();
        // TODO: FULL_TYPES should be implemented here as : if (fullTypesModeEnabled()) writeTypePrefix(s);
    }

    @Override
    public void writeCustomTypeSuffixForScalar(Object o, JsonGenerator jsonGenerator, String s) throws IOException {
        if (canWriteTypeId()) {
            writeTypeSuffix(jsonGenerator);
        }
    }

    @Override
    public void writeCustomTypeSuffixForObject(Object o, JsonGenerator jsonGenerator, String s) throws IOException {
        jsonGenerator.writeEndObject();
        // TODO: FULL_TYPES should be implemented here as : if (fullTypesModeEnabled()) writeTypeSuffix(s);
    }

    @Override
    public void writeCustomTypeSuffixForArray(Object o, JsonGenerator jsonGenerator, String s) throws IOException {
        jsonGenerator.writeEndArray();
        // TODO: FULL_TYPES should be implemented here as : if (fullTypesModeEnabled()) writeTypeSuffix(s);
    }

    private boolean canWriteTypeId() {
        return typeInfo != null
                && typeInfo == TypeInfo.PARTIAL_TYPES;
    }

    private void writeTypePrefix(JsonGenerator jsonGenerator, String s) throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(this.getPropertyName(), s);
        jsonGenerator.writeFieldName(this.valuePropertyName);
    }

    private void writeTypeSuffix(JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeEndObject();
    }

    /* We force only **one** translation of a Java object to a domain specific object.
     i.e. users register typeIDs and serializers/deserializers for the predefined
     types we have in the spec. Graph, Vertex, Edge, VertexProperty, etc... And
     **not** their implementations (TinkerGraph, DetachedVertex, TinkerEdge,
     etc..)
    */
    private Class getClassFromObject(Object o) {
        // not the most efficient
        Class c = o.getClass();
        if (Vertex.class.isAssignableFrom(c)) {
            return Vertex.class;
        } else if (Edge.class.isAssignableFrom(c)) {
            return Edge.class;
        } else if (Path.class.isAssignableFrom(c)) {
            return Path.class;
        } else if (VertexProperty.class.isAssignableFrom(c)) {
            return VertexProperty.class;
        } else if (Metrics.class.isAssignableFrom(c)) {
            return Metrics.class;
        } else if (TraversalMetrics.class.isAssignableFrom(c)) {
            return TraversalMetrics.class;
        } else if (Property.class.isAssignableFrom(c)) {
            return Property.class;
        } else if (ByteBuffer.class.isAssignableFrom(c)) {
            return ByteBuffer.class;
        }
        return c;
    }
}
