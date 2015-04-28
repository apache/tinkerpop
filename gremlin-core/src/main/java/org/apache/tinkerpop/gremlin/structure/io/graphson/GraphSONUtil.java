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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;

import java.io.IOException;

/**
 * Utility methods for GraphSON serialization. Functions in here might be used by external serializers.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONUtil {
    public static void writeWithType(final String key, final Object object, final JsonGenerator jsonGenerator,
                                     final SerializerProvider serializerProvider,
                                     final TypeSerializer typeSerializer) throws IOException {
        final JsonSerializer<Object> serializer = serializerProvider.findValueSerializer(object.getClass(), null);
        if (typeSerializer != null) {
            jsonGenerator.writeFieldName(key);
            serializer.serializeWithType(object, jsonGenerator, serializerProvider, typeSerializer);
        } else {
            jsonGenerator.writeObjectField(key, object);
        }
    }
}
