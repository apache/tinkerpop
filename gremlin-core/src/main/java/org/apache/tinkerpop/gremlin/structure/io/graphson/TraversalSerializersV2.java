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
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class TraversalSerializersV2 {

    private TraversalSerializersV2() {
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
            jsonGenerator.writeObject(traversal.asAdmin().getBytecode());
        }

        @Override
        public void serializeWithType(final Traversal traversal, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider, final TypeSerializer typeSerializer)
                throws IOException {
            serialize(traversal, jsonGenerator, serializerProvider);
        }

    }

    final static class BytecodeJacksonSerializer extends StdScalarSerializer<Bytecode> {

        public BytecodeJacksonSerializer() {
            super(Bytecode.class);
        }

        @Override
        public void serialize(final Bytecode bytecode, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();
            if (bytecode.getSourceInstructions().iterator().hasNext()) {
                jsonGenerator.writeArrayFieldStart(GraphSONTokens.SOURCE);
                for (final Bytecode.Instruction instruction : bytecode.getSourceInstructions()) {
                    jsonGenerator.writeStartArray();
                    jsonGenerator.writeString(instruction.getOperator());
                    for (final Object argument : instruction.getArguments()) {
                        jsonGenerator.writeObject(argument);
                    }
                    jsonGenerator.writeEndArray();
                }
                jsonGenerator.writeEndArray();
            }
            if (bytecode.getStepInstructions().iterator().hasNext()) {
                jsonGenerator.writeArrayFieldStart(GraphSONTokens.STEP);
                for (final Bytecode.Instruction instruction : bytecode.getStepInstructions()) {
                    jsonGenerator.writeStartArray();
                    jsonGenerator.writeString(instruction.getOperator());
                    for (final Object argument : instruction.getArguments()) {
                        jsonGenerator.writeObject(argument);
                    }
                    jsonGenerator.writeEndArray();
                }
                jsonGenerator.writeEndArray();
            }

            jsonGenerator.writeEndObject();
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
            } else {
                if (p.getValue() instanceof Collection) {
                    jsonGenerator.writeArrayFieldStart(GraphSONTokens.VALUE);
                    for (final Object object : (Collection) p.getValue()) {
                        jsonGenerator.writeObject(object);
                    }
                    jsonGenerator.writeEndArray();
                } else
                    jsonGenerator.writeObjectField(GraphSONTokens.VALUE, p.getValue());
            }
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

    final static class BindingJacksonSerializer extends StdScalarSerializer<Bytecode.Binding> {

        public BindingJacksonSerializer() {
            super(Bytecode.Binding.class);
        }

        @Override
        public void serialize(final Bytecode.Binding binding, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(GraphSONTokens.KEY, binding.variable());
            jsonGenerator.writeObjectField(GraphSONTokens.VALUE, binding.value());
            jsonGenerator.writeEndObject();
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

    final static class BytecodeJacksonDeserializer extends StdDeserializer<Bytecode> {

        public BytecodeJacksonDeserializer() {
            super(Bytecode.class);
        }

        @Override
        public Bytecode deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final Bytecode bytecode = new Bytecode();

            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                final String current = jsonParser.getCurrentName();
                if (current.equals(GraphSONTokens.SOURCE) || current.equals(GraphSONTokens.STEP)) {
                    jsonParser.nextToken();

                    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {

                        // there should be a list now and the first item in the list is always string and is the step name
                        // skip the start array
                        jsonParser.nextToken();
                        
                        final String stepName = jsonParser.getText();

                        // iterate through the rest of the list for arguments until it gets to the end
                        final List<Object> arguments = new ArrayList<>();
                        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                            // we don't know the types here, so let the deserializer figure that business out
                            arguments.add(deserializationContext.readValue(jsonParser, Object.class));
                        }

                        // if it's not a "source" then it must be a "step"
                        if (current.equals(GraphSONTokens.SOURCE))
                            bytecode.addSource(stepName, arguments.toArray());
                        else
                            bytecode.addStep(stepName, arguments.toArray());
                    }
                }
            }
            return bytecode;
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

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

    final static class PJacksonDeserializer extends StdDeserializer<P> {

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
                            return (P) P.class.getMethod(predicate, Collection.class).invoke(null, (Collection) value);
                    } else {
                        try {
                            return (P) P.class.getMethod(predicate, Object.class).invoke(null, value);
                        } catch (final NoSuchMethodException e) {
                            return (P) P.class.getMethod(predicate, Object[].class).invoke(null, (Object) new Object[]{value});
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

    final static class TextPJacksonDeserializer extends StdDeserializer<TextP> {

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
                return (TextP) TextP.class.getMethod(predicate, String.class).invoke(null, value);
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

    final static class BindingJacksonDeserializer extends StdDeserializer<Bytecode.Binding> {

        public BindingJacksonDeserializer() {
            super(Bytecode.Binding.class);
        }

        @Override
        public Bytecode.Binding deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            String k = null;
            Object v = null;

            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                if (jsonParser.getCurrentName().equals(GraphSONTokens.KEY)) {
                    jsonParser.nextToken();
                    k = jsonParser.getText();
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.VALUE)) {
                    jsonParser.nextToken();
                    v = deserializationContext.readValue(jsonParser, Object.class);
                }
            }
            return new Bytecode.Binding<>(k, v);
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
