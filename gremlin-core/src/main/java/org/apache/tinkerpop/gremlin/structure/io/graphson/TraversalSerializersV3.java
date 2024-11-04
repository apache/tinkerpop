/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.commons.configuration2.ConfigurationConverter;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdScalarSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class TraversalSerializersV3 {

    private TraversalSerializersV3() {
    }

    /////////////////
    // SERIALIZERS //
    ////////////////

    final static class TraversalJacksonSerializer extends StdSerializer<Traversal> {

        public TraversalJacksonSerializer() {
            super(Traversal.class);
        }

        @Override
        public void serialize(final Traversal traversal, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeObject(traversal.asAdmin().getGremlinLang());
        }

        @Override
        public void serializeWithType(final Traversal traversal, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider, final TypeSerializer typeSerializer)
                throws IOException {
            serialize(traversal, jsonGenerator, serializerProvider);
        }

    }

    static class EnumJacksonSerializer extends StdScalarSerializer<Enum> {

        public EnumJacksonSerializer() {
            super(Enum.class);
        }

        @Override
        public void serialize(final Enum enumInstance, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeString(enumInstance.name());
        }

    }

    final static class PJacksonSerializer extends StdScalarSerializer<P> {

        public PJacksonSerializer() {
            super(P.class);
        }

        @Override
        public void serialize(final P p, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(GraphSONTokens.PREDICATE, p.getPredicateName());
            if (p instanceof ConnectiveP) {
                jsonGenerator.writeArrayFieldStart(GraphSONTokens.VALUE);
                for (final P<?> predicate : ((ConnectiveP<?>) p).getPredicates()) {
                    jsonGenerator.writeObject(predicate);
                }
                jsonGenerator.writeEndArray();
            } else
                jsonGenerator.writeObjectField(GraphSONTokens.VALUE, p.getValue());

            jsonGenerator.writeEndObject();
        }

    }

    final static class LambdaJacksonSerializer extends StdScalarSerializer<Lambda> {

        public LambdaJacksonSerializer() {
            super(Lambda.class);
        }

        @Override
        public void serialize(final Lambda lambda, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(GraphSONTokens.SCRIPT, lambda.getLambdaScript());
            jsonGenerator.writeStringField(GraphSONTokens.LANGUAGE, lambda.getLambdaLanguage());
            jsonGenerator.writeNumberField(GraphSONTokens.ARGUMENTS, lambda.getLambdaArguments());
            jsonGenerator.writeEndObject();
        }

    }

    final static class BulkSetJacksonSerializer extends StdScalarSerializer<BulkSet> {

        public BulkSetJacksonSerializer() {
            super(BulkSet.class);
        }

        @Override
        public void serialize(final BulkSet bulkSet, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartArray();
            for (Map.Entry entry : (Set<Map.Entry>) bulkSet.asBulk().entrySet()) {
                jsonGenerator.writeObject(entry.getKey());
                jsonGenerator.writeObject(entry.getValue());
            }
            jsonGenerator.writeEndArray();
        }
    }

    final static class TraverserJacksonSerializer extends StdScalarSerializer<Traverser> {

        public TraverserJacksonSerializer() {
            super(Traverser.class);
        }

        @Override
        public void serialize(final Traverser traverserInstance, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectField(GraphSONTokens.BULK, traverserInstance.bulk());
            jsonGenerator.writeObjectField(GraphSONTokens.VALUE, traverserInstance.get());
            jsonGenerator.writeEndObject();
        }
    }

    final static class TraversalStrategyJacksonSerializer extends StdScalarSerializer<TraversalStrategy> {

        public TraversalStrategyJacksonSerializer() {
            super(TraversalStrategy.class);
        }

        @Override
        public void serialize(final TraversalStrategy traversalStrategy, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();
            for (final Map.Entry<Object, Object> entry : ConfigurationConverter.getMap(traversalStrategy.getConfiguration()).entrySet()) {
                jsonGenerator.writeObjectField((String) entry.getKey(), entry.getValue());
            }
            jsonGenerator.writeEndObject();
        }
    }

    ///////////////////
    // DESERIALIZERS //
    //////////////////

    final static class EnumJacksonDeserializer<A extends Enum> extends StdDeserializer<A> {

        public EnumJacksonDeserializer(final Class<A> enumClass) {
            super(enumClass);
        }

        @Override
        public A deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final Class<A> enumClass = (Class<A>) this._valueClass;
            final String enumName = jsonParser.getText();
            for (final Enum a : enumClass.getEnumConstants()) {
                if (a.name().equals(enumName))
                    return (A) a;
            }
            throw new IOException("Unknown enum type: " + enumClass);
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

    final static class PJacksonDeserializer extends AbstractReflectJacksonDeserializer<P> {

        public PJacksonDeserializer() {
            super(P.class);
        }

        @Override
        public P deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            String predicate = null;
            Object value = null;

            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                if (jsonParser.getCurrentName().equals(GraphSONTokens.PREDICATE)) {
                    jsonParser.nextToken();
                    predicate = jsonParser.getText();
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.VALUE)) {
                    jsonParser.nextToken();
                    value = deserializationContext.readValue(jsonParser, Object.class);
                }
            }

            if (predicate.equals(GraphSONTokens.AND) || predicate.equals(GraphSONTokens.OR)) {
                return predicate.equals(GraphSONTokens.AND) ? new AndP((List<P>) value) : new OrP((List<P>) value);
            } else if (predicate.equals(GraphSONTokens.NOT) && value instanceof P) {
                return P.not((P<?>) value);
            } else {
                try {
                    if (value instanceof Collection) {
                        if (predicate.equals("between"))
                            return P.between(((List) value).get(0), ((List) value).get(1));
                        else if (predicate.equals("inside"))
                            return P.between(((List) value).get(0), ((List) value).get(1));
                        else if (predicate.equals("outside"))
                            return P.outside(((List) value).get(0), ((List) value).get(1));
                        else if (predicate.equals("within"))
                            return P.within((Collection) value);
                        else if (predicate.equals("without"))
                            return P.without((Collection) value);
                        else
                            return (P) tryFindMethod(P.class, predicate, Collection.class).invoke(null, (Collection) value);
                    } else {
                        try {
                            return (P) tryFindMethod(P.class, predicate, Object.class).invoke(null, value);
                        } catch (final NoSuchMethodException e) {
                            return (P) tryFindMethod(P.class, predicate, Object[].class).invoke(null, (Object) new Object[]{value});
                        }
                    }
                } catch (final Exception e) {
                    throw new IllegalStateException(e.getMessage(), e);
                }
            }
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

    /**
     * Deserializers that make reflection calls can use this class as a base and thus cache reflected methods to avoid
     * future lookups.
     */
    static abstract class AbstractReflectJacksonDeserializer<T> extends StdDeserializer<T> {
        private final Map<CacheKey, Method> CACHE = new ConcurrentHashMap<>();

        public AbstractReflectJacksonDeserializer(final Class<? extends T> clazz) {
            super(clazz);
        }

        protected Method tryFindMethod(final Class<?> base, final String methodName, final Class<?> parameterType) throws Exception {
            return CACHE.computeIfAbsent(new CacheKey(methodName, parameterType),
                    cacheKey -> {
                        try {
                            return base.getMethod(methodName, parameterType);
                        } catch (Exception e) {
                            throw new IllegalStateException(e.getMessage(), e);
                        }
                    });
        }

        private static class CacheKey {
            private final String predicate;
            private final Class<?> parameterType;

            public CacheKey(final String predicate, final Class<?> parameterType) {
                this.predicate = Objects.requireNonNull(predicate);
                this.parameterType = Objects.requireNonNull(parameterType);
            }

            @Override
            public boolean equals(final Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                final CacheKey cacheKey = (CacheKey) o;
                return predicate.equals(cacheKey.predicate) &&
                        parameterType.equals(cacheKey.parameterType);
            }

            @Override
            public int hashCode() {
                return Objects.hash(predicate, parameterType);
            }
        }
    }

    final static class TextPJacksonDeserializer extends AbstractReflectJacksonDeserializer<TextP> {

        public TextPJacksonDeserializer() {
            super(TextP.class);
        }

        @Override
        public TextP deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            String predicate = null;
            String value = null;

            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                if (jsonParser.getCurrentName().equals(GraphSONTokens.PREDICATE)) {
                    jsonParser.nextToken();
                    predicate = jsonParser.getText();
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.VALUE)) {
                    jsonParser.nextToken();
                    value = deserializationContext.readValue(jsonParser, String.class);
                }
            }

            try {
                return (TextP) tryFindMethod(TextP.class, predicate, String.class).invoke(null, value);
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

    final static class LambdaJacksonDeserializer extends StdDeserializer<Lambda> {

        public LambdaJacksonDeserializer() {
            super(Lambda.class);
        }

        @Override
        public Lambda deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            String script = null;
            String language = null;
            int arguments = -1;

            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                if (jsonParser.getCurrentName().equals(GraphSONTokens.SCRIPT)) {
                    jsonParser.nextToken();
                    script = jsonParser.getText();
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.LANGUAGE)) {
                    jsonParser.nextToken();
                    language = jsonParser.getText();
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.ARGUMENTS)) {
                    jsonParser.nextToken();
                    arguments = jsonParser.getIntValue();
                }
            }

            if (-1 == arguments || arguments > 2)
                return new Lambda.UnknownArgLambda(script, language, arguments);
            else if (0 == arguments)
                return new Lambda.ZeroArgLambda<>(script, language);
            else if (1 == arguments)
                return new Lambda.OneArgLambda<>(script, language);
            else
                return new Lambda.TwoArgLambda<>(script, language);
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

    final static class BulkSetJacksonDeserializer extends StdDeserializer<BulkSet> {
        public BulkSetJacksonDeserializer() {
            super(BulkSet.class);
        }

        @Override
        public BulkSet deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {

            final BulkSet<Object> bulkSet = new BulkSet<>();

            while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                final Object key = deserializationContext.readValue(jsonParser, Object.class);
                jsonParser.nextToken();
                final Long val = deserializationContext.readValue(jsonParser, Long.class);
                bulkSet.add(key, val);
            }

            return bulkSet;
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

    static class TraverserJacksonDeserializer extends StdDeserializer<Traverser> {

        public TraverserJacksonDeserializer() {
            super(Traverser.class);
        }

        @Override
        public Traverser deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            long bulk = 1;
            Object v = null;

            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                if (jsonParser.getCurrentName().equals(GraphSONTokens.BULK)) {
                    jsonParser.nextToken();
                    bulk = deserializationContext.readValue(jsonParser, Long.class);
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.VALUE)) {
                    jsonParser.nextToken();
                    v = deserializationContext.readValue(jsonParser, Object.class);
                }
            }

            return new DefaultRemoteTraverser<>(v, bulk);
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

    final static class TraversalStrategyProxyJacksonDeserializer<T extends TraversalStrategy> extends AbstractObjectDeserializer<TraversalStrategyProxy> {

        private final Class<T> clazz;

        public TraversalStrategyProxyJacksonDeserializer(final Class<T> clazz) {
            super(TraversalStrategyProxy.class);
            this.clazz = clazz;
        }

        @Override
        public TraversalStrategyProxy<T> createObject(final Map<String, Object> data) {
            return new TraversalStrategyProxy<>(this.clazz, new MapConfiguration(data));
        }
    }
}
