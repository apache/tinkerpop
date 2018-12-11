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

import org.apache.tinkerpop.shaded.jackson.annotation.JsonTypeInfo;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.core.type.WritableTypeId;
import org.apache.tinkerpop.shaded.jackson.databind.BeanProperty;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeIdResolver;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Extension of the Jackson's default TypeSerializer. An instance of this object will be passed to the serializers
 * on which they can safely call the utility methods to serialize types and making it compatible with the version
 * 2.0+ of GraphSON.
 *
 * @author Kevin Gallardo (https://kgdo.me)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGraphSONTypeSerializer extends TypeSerializer {

    protected final TypeIdResolver idRes;
    protected final String propertyName;
    protected final TypeInfo typeInfo;
    protected final String valuePropertyName;
    protected final Map<Class, Class> classMap = new HashMap<>();

    AbstractGraphSONTypeSerializer(final TypeIdResolver idRes, final String propertyName, final TypeInfo typeInfo,
                                   final String valuePropertyName) {
        this.idRes = idRes;
        this.propertyName = propertyName;
        this.typeInfo = typeInfo;
        this.valuePropertyName = valuePropertyName;
    }


    @Override
    public TypeSerializer forProperty(final BeanProperty beanProperty) {
        return this;
    }

    @Override
    public JsonTypeInfo.As getTypeInclusion() {
        return JsonTypeInfo.As.WRAPPER_OBJECT;
    }

    @Override
    public String getPropertyName() {
        return propertyName;
    }

    @Override
    public TypeIdResolver getTypeIdResolver() {
        return idRes;
    }

    protected boolean canWriteTypeId() {
        return typeInfo != null
                && typeInfo == TypeInfo.PARTIAL_TYPES;
    }

    protected void writeTypePrefix(final JsonGenerator jsonGenerator, final String s) throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(this.propertyName, s);
        jsonGenerator.writeFieldName(this.valuePropertyName);
    }

    protected void writeTypeSuffix(final JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeEndObject();
    }

    /**
     * We force only **one** translation of a Java object to a domain specific object. i.e. users register typeIDs
     * and serializers/deserializers for the predefined types we have in the spec. Graph, Vertex, Edge,
     * VertexProperty, etc... And **not** their implementations (TinkerGraph, DetachedVertex, TinkerEdge, etc..)
     */
    protected abstract Class getClassFromObject(final Object o);
}