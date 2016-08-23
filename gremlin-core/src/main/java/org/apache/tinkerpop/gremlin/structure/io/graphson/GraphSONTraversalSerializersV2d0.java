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

import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdScalarSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class GraphSONTraversalSerializersV2d0 {

    private GraphSONTraversalSerializersV2d0() {
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
            jsonGenerator.writeStringField(GraphSONTokens.PREDICATE,
                    p instanceof ConnectiveP ?
                            p instanceof AndP ?
                                    GraphSONTokens.AND :
                                    GraphSONTokens.OR :
                            p.getBiPredicate().toString());
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

    final static class TraverserSerializer extends StdScalarSerializer<Traverser> {

        public TraverserSerializer() {
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

    ///////////////////
    // DESERIALIZERS //
    //////////////////

    final static class BytecodeJacksonDeserializer extends AbstractObjectDeserializer<Bytecode> {

        public BytecodeJacksonDeserializer() {
            super(Bytecode.class);
        }

        @Override
        Bytecode createObject(final Map<String, Object> data) {
            final Bytecode bytecode = new Bytecode();
            if (data.containsKey(GraphSONTokens.SOURCE)) {
                final List<List<Object>> instructions = (List) data.get(GraphSONTokens.SOURCE);
                for (final List<Object> instruction : instructions) {
                    bytecode.addSource((String) instruction.get(0), Arrays.copyOfRange(instruction.toArray(), 1, instruction.size()));
                }
            }
            if (data.containsKey(GraphSONTokens.STEP)) {
                final List<List<Object>> instructions = (List) data.get(GraphSONTokens.STEP);
                for (final List<Object> instruction : instructions) {
                    bytecode.addStep((String) instruction.get(0), Arrays.copyOfRange(instruction.toArray(), 1, instruction.size()));
                }
            }
            return bytecode;
        }
    }

    final static class EnumJacksonDeserializer<A extends Enum> extends StdDeserializer<A> {

        private final A enumInstance;

        public EnumJacksonDeserializer(final A enumInstance) {
            super(enumInstance.getClass());
            this.enumInstance = enumInstance;
        }

        @Override
        public A deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final Class<A> enumClass = (Class<A>) this.enumInstance.getDeclaringClass();
            final String enumName = jsonParser.getText();
            for (final Enum a : enumClass.getEnumConstants()) {
                if (a.name().equals(enumName))
                    return (A) a;
            }
            throw new IOException("Unknown enum type: " + enumClass);
        }
    }

    final static class PJacksonDeserializer extends AbstractObjectDeserializer<P> {

        public PJacksonDeserializer() {
            super(P.class);
        }

        @Override
        P createObject(final Map<String, Object> data) {
            final String predicate = (String) data.get(GraphSONTokens.PREDICATE);
            final Object value = data.get(GraphSONTokens.VALUE);
            if (predicate.equals(GraphSONTokens.AND) || predicate.equals(GraphSONTokens.OR)) {
                return predicate.equals(GraphSONTokens.AND) ? new AndP((List<P>) value) : new OrP((List<P>) value);
            } else {
                try {
                    return (P) P.class.getMethod(predicate, value instanceof Collection ? Collection.class : Object.class).invoke(null, value); // TODO: number stuff, eh?
                } catch (final Exception e) {
                    throw new IllegalStateException(e.getMessage(), e);
                }
            }
        }
    }

    final static class LambdaJacksonDeserializer extends AbstractObjectDeserializer<Lambda> {

        public LambdaJacksonDeserializer() {
            super(Lambda.class);
        }

        @Override
        Lambda createObject(final Map<String, Object> data) {
            final String script = (String) data.get(GraphSONTokens.SCRIPT);
            final String language = (String) data.get(GraphSONTokens.LANGUAGE);
            final int arguments = ((Number) data.getOrDefault(GraphSONTokens.ARGUMENTS, -1)).intValue();
            //
            if (-1 == arguments || arguments > 2)
                return new Lambda.UnknownArgLambda(script, language, arguments);
            else if (0 == arguments)
                return new Lambda.ZeroArgLambda<>(script, language);
            else if (1 == arguments)
                return new Lambda.OneArgLambda<>(script, language);
            else
                return new Lambda.TwoArgLambda<>(script, language);
        }
    }

    final static class BindingJacksonDeserializer extends AbstractObjectDeserializer<Bytecode.Binding> {

        public BindingJacksonDeserializer() {
            super(Bytecode.Binding.class);
        }

        @Override
        Bytecode.Binding createObject(final Map<String, Object> data) {
            return new Bytecode.Binding<>((String) data.get(GraphSONTokens.KEY), data.get(GraphSONTokens.VALUE));
        }
    }

    static class TraverserJacksonDeserializer extends AbstractObjectDeserializer<Traverser> {

        public TraverserJacksonDeserializer() {
            super(Traverser.class);
        }

        @Override
        Traverser createObject(final Map<String, Object> data) {
            return new DefaultRemoteTraverser<>(data.get(GraphSONTokens.VALUE), (Long) data.get(GraphSONTokens.BULK));
        }
    }
}
