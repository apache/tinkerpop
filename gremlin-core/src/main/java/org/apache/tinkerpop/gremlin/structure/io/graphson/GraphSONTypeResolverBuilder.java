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

import org.apache.tinkerpop.shaded.jackson.databind.DeserializationConfig;
import org.apache.tinkerpop.shaded.jackson.databind.JavaType;
import org.apache.tinkerpop.shaded.jackson.databind.SerializationConfig;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.NamedType;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeIdResolver;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.impl.StdTypeResolverBuilder;

import java.util.Collection;

/**
 * Creates the Type serializers as well as the Typed deserializers that will be provided to the serializers and
 * deserializers. Contains the typeInfo level that should be provided by the GraphSONMapper.
 *
 * @author Kevin Gallardo (https://kgdo.me)
 */
public class GraphSONTypeResolverBuilder extends StdTypeResolverBuilder {

    private TypeInfo typeInfo;
    private String valuePropertyName;

    @Override
    public TypeDeserializer buildTypeDeserializer(DeserializationConfig config, JavaType baseType, Collection<NamedType> subtypes) {
        TypeIdResolver idRes = this.idResolver(config, baseType, subtypes, false, true);
        return new GraphSONTypeDeserializer(baseType, idRes, this.getTypeProperty(), typeInfo, valuePropertyName);
    }


    @Override
    public TypeSerializer buildTypeSerializer(SerializationConfig config, JavaType baseType, Collection<NamedType> subtypes) {
        TypeIdResolver idRes = this.idResolver(config, baseType, subtypes, true, false);
        return new GraphSONTypeSerializer(idRes, this.getTypeProperty(), typeInfo, valuePropertyName);
    }

    public GraphSONTypeResolverBuilder valuePropertyName(String valuePropertyName) {
        this.valuePropertyName = valuePropertyName;
        return this;
    }

    public GraphSONTypeResolverBuilder typesEmbedding(TypeInfo typeInfo) {
        this.typeInfo = typeInfo;
        return this;
    }
}
