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
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.databind.BeanProperty;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.JavaType;
import org.apache.tinkerpop.shaded.jackson.databind.JsonDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeIdResolver;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.impl.TypeDeserializerBase;
import org.apache.tinkerpop.shaded.jackson.databind.type.TypeFactory;
import org.apache.tinkerpop.shaded.jackson.databind.util.TokenBuffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * Contains main logic for the whole JSON to Java deserialization. Handles types embedded with the version 2.0 of GraphSON.
 *
 * @author Kevin Gallardo (https://kgdo.me)
 */
public class GraphSONTypeDeserializer extends TypeDeserializerBase {
    private final TypeIdResolver idRes;
    private final String propertyName;
    private final String valuePropertyName;
    private final JavaType baseType;
    private final TypeInfo typeInfo;

    private static final JavaType mapJavaType = TypeFactory.defaultInstance().constructType(LinkedHashMap.class);
    private static final JavaType arrayJavaType = TypeFactory.defaultInstance().constructType(ArrayList.class);

    GraphSONTypeDeserializer(final JavaType baseType, final TypeIdResolver idRes, final String typePropertyName,
                             final TypeInfo typeInfo, final String valuePropertyName){
        super(baseType, idRes, typePropertyName, false, null);
        this.baseType = baseType;
        this.idRes = idRes;
        this.propertyName = typePropertyName;
        this.typeInfo = typeInfo;
        this.valuePropertyName = valuePropertyName;
    }

    @Override
    public TypeDeserializer forProperty(BeanProperty beanProperty) {
        return this;
    }

    @Override
    public JsonTypeInfo.As getTypeInclusion() {
        return JsonTypeInfo.As.WRAPPER_ARRAY;
    }


    @Override
    public TypeIdResolver getTypeIdResolver() {
        return idRes;
    }

    @Override
    public Class<?> getDefaultImpl() {
        return null;
    }

    @Override
    public Object deserializeTypedFromObject(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {
        return deserialize(jsonParser, deserializationContext);
    }

    @Override
    public Object deserializeTypedFromArray(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {
        return deserialize(jsonParser, deserializationContext);
    }

    @Override
    public Object deserializeTypedFromScalar(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {
        return deserialize(jsonParser, deserializationContext);
    }

    @Override
    public Object deserializeTypedFromAny(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {
        return deserialize(jsonParser, deserializationContext);
    }

    /**
     * Main logic for the deserialization.
     */
    private Object deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {
        final TokenBuffer buf = new TokenBuffer(jsonParser.getCodec(), false);
        final TokenBuffer localCopy = new TokenBuffer(jsonParser.getCodec(), false);

        // Detect type
        try {
            // The Type pattern is START_OBJECT -> TEXT_FIELD(propertyName) && TEXT_FIELD(valueProp).
            if (jsonParser.getCurrentToken() == JsonToken.START_OBJECT) {
                buf.writeStartObject();
                String typeName = null;
                boolean valueDetected = false;
                boolean valueDetectedFirst = false;

                for (int i = 0; i < 2; i++) {
                    String nextFieldName = jsonParser.nextFieldName();
                    if (nextFieldName == null) {
                        // empty map or less than 2 fields, go out.
                        break;
                    }
                    if (!nextFieldName.equals(this.propertyName) && !nextFieldName.equals(this.valuePropertyName)) {
                        // no type, go out.
                        break;
                    }

                    if (nextFieldName.equals(this.propertyName)) {
                        // detected "@type" field.
                        typeName = jsonParser.nextTextValue();
                        // keeping the spare buffer up to date in case it's a false detection (only the "@type" property)
                        buf.writeStringField(this.propertyName, typeName);
                        continue;
                    }
                    if (nextFieldName.equals(this.valuePropertyName)) {
                        // detected "@value" field.
                        jsonParser.nextValue();

                        if (typeName == null) {
                            // keeping the spare buffer up to date in case it's a false detection (only the "@value" property)
                            // the problem is that the fields "@value" and "@type" could be in any order
                            buf.writeFieldName(this.valuePropertyName);
                            valueDetectedFirst = true;
                            localCopy.copyCurrentStructure(jsonParser);
                        }
                        valueDetected = true;
                    }
                }

                if (typeName != null && valueDetected) {
                    // Type has been detected pattern detected.
                    final JavaType typeFromId = idRes.typeFromId(deserializationContext, typeName);

                    if (!baseType.isJavaLangObject() && !baseType.equals(typeFromId)) {
                        throw new InstantiationException(
                                String.format("Cannot deserialize the value with the detected type contained in the JSON ('%s') " +
                                        "to the type specified in parameter to the object mapper (%s). " +
                                        "Those types are incompatible.", typeName, baseType.getRawClass().toString())
                        );
                    }

                    final JsonDeserializer jsonDeserializer = deserializationContext.findContextualValueDeserializer(typeFromId, null);

                    JsonParser tokenParser;

                    if (valueDetectedFirst) {
                        tokenParser = localCopy.asParser();
                        tokenParser.nextToken();
                    } else {
                        tokenParser = jsonParser;
                    }

                    final Object value = jsonDeserializer.deserialize(tokenParser, deserializationContext);

                    final JsonToken t = jsonParser.nextToken();
                    if (t == JsonToken.END_OBJECT) {
                        // we're good to go
                        return value;
                    } else {
                        // detected the type pattern entirely but the Map contained other properties
                        // For now we error out because we assume that pattern is *only* reserved to
                        // typed values.
                        throw deserializationContext.mappingException("Detected the type pattern in the JSON payload " +
                                "but the map containing the types and values contains other fields. This is not " +
                                "allowed by the deserializer.");
                    }
                }
            }
        } catch (Exception e) {
            throw deserializationContext.mappingException("Could not deserialize the JSON value as required. Nested exception: " + e);
        }

        // Type pattern wasn't detected, however,
        // while searching for the type pattern, we may have moved the cursor of the original JsonParser in param.
        // To compensate, we have filled consistently a TokenBuffer that should contain the equivalent of
        // what we skipped while searching for the pattern.
        // This has a huge positive impact on performances, since JsonParser does not have a 'rewind()',
        // the only other solution would have been to copy the whole original JsonParser. Which we avoid here and use
        // an efficient structure made of TokenBuffer + JsonParserSequence/Concat.
        // Concatenate buf + localCopy + end of original content(jsonParser).
        final JsonParser[] concatenatedArray = {buf.asParser(), localCopy.asParser(), jsonParser};
        final JsonParser parserToUse = new JsonParserConcat(concatenatedArray);
        parserToUse.nextToken();

        // If a type has been specified in parameter, use it to find a deserializer and deserialize:
        if (!baseType.isJavaLangObject()) {
            final JsonDeserializer jsonDeserializer = deserializationContext.findContextualValueDeserializer(baseType, null);
            return jsonDeserializer.deserialize(parserToUse, deserializationContext);
        }
        // Otherwise, detect the current structure:
        else {
            if (parserToUse.isExpectedStartArrayToken()) {
                return deserializationContext.findContextualValueDeserializer(arrayJavaType, null).deserialize(parserToUse, deserializationContext);
            } else if (parserToUse.isExpectedStartObjectToken()) {
                return deserializationContext.findContextualValueDeserializer(mapJavaType, null).deserialize(parserToUse, deserializationContext);
            } else {
                // There's "java.lang.Object" in param, there's no type detected in the payload, the payload isn't a JSON Map or JSON List
                // then consider it a simple type, even though we shouldn't be here if it was a simple type.
                // TODO : maybe throw an error instead?
                // throw deserializationContext.mappingException("Roger, we have a problem deserializing");
                final JsonDeserializer jsonDeserializer = deserializationContext.findContextualValueDeserializer(baseType, null);
                return jsonDeserializer.deserialize(parserToUse, deserializationContext);
            }
        }
    }

    private boolean canReadTypeId() {
        return this.typeInfo == TypeInfo.PARTIAL_TYPES;
    }
}
