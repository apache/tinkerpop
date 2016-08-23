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
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.core.ObjectCodec;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.node.JsonNodeType;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.ArrayList;
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

    final static class BytecodeJacksonSerializer extends StdSerializer<Bytecode> {

        public BytecodeJacksonSerializer() {
            super(Bytecode.class);
        }

        @Override
        public void serialize(final Bytecode bytecode, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("@type", "Bytecode");
            if (bytecode.getSourceInstructions().iterator().hasNext()) {
                jsonGenerator.writeArrayFieldStart("source");
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
                jsonGenerator.writeArrayFieldStart("step");
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

    final static class EnumJacksonSerializer extends StdSerializer<Enum> {

        public EnumJacksonSerializer() {
            super(Enum.class);
        }

        @Override
        public void serialize(final Enum enumInstance, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectField("@type", enumInstance.getDeclaringClass().getSimpleName());
            jsonGenerator.writeObjectField("value", enumInstance.name());
            jsonGenerator.writeEndObject();
        }

    }

    final static class PJacksonSerializer extends StdSerializer<P> {

        public PJacksonSerializer() {
            super(P.class);
        }

        @Override
        public void serialize(final P p, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("@type", "P");
            jsonGenerator.writeStringField("predicate", p instanceof ConnectiveP ? p instanceof AndP ? "and" : "or" : p.getBiPredicate().toString());
            jsonGenerator.writeObjectField("value", p instanceof ConnectiveP ? ((ConnectiveP) p).getPredicates() : p.getValue());
            jsonGenerator.writeEndObject();
        }

    }

    final static class LambdaJacksonSerializer extends StdSerializer<Lambda> {

        public LambdaJacksonSerializer() {
            super(Lambda.class);
        }

        @Override
        public void serialize(final Lambda lambda, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("@type", "Lambda");
            jsonGenerator.writeStringField("value", lambda.getLambdaScript());
            jsonGenerator.writeStringField("language", lambda.getLambdaLanguage());
            jsonGenerator.writeNumberField("arguments", lambda.getLambdaArguments());
            jsonGenerator.writeEndObject();
        }

    }

    final static class BindingJacksonSerializer extends StdSerializer<Bytecode.Binding> {

        public BindingJacksonSerializer() {
            super(Bytecode.Binding.class);
        }

        @Override
        public void serialize(final Bytecode.Binding binding, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("@type", "Binding");
            jsonGenerator.writeStringField("variable", binding.variable());
            jsonGenerator.writeObjectField("value", binding.value());
            jsonGenerator.writeEndObject();
        }

    }

    final static class TraverserSerializer extends StdSerializer<Traverser> {

        public TraverserSerializer() {
            super(Traverser.class);
        }

        @Override
        public void serialize(final Traverser traverserInstance, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(traverserInstance, jsonGenerator);
        }

        @Override
        public void serializeWithType(final Traverser traverser, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(traverser, jsonGenerator);
        }

        private void ser(final Traverser traverserInstance, final JsonGenerator jsonGenerator) throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("@type", "Traverser");
            jsonGenerator.writeObjectField("bulk", traverserInstance.bulk());
            jsonGenerator.writeObjectField("value", traverserInstance.get());
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

        private static void processInstruction(final JsonNode instruction, final ObjectCodec oc, final Bytecode bytecode, final boolean source) throws IOException {
            final String operator = instruction.get(0).textValue();
            final List<Object> arguments = new ArrayList<>();
            for (int j = 1; j < instruction.size(); j++) {
                final JsonNode argument = instruction.get(j);
                if (argument.getNodeType().equals(JsonNodeType.OBJECT)) {
                    if (argument.has("@type")) {
                        final String type = argument.get("@type").textValue();
                        if (type.equals("Bytecode"))
                            arguments.add(oc.readValue(argument.traverse(oc), Bytecode.class));
                        else if (type.equals("Binding"))
                            arguments.add(oc.readValue(argument.traverse(oc), Bytecode.Binding.class));
                        else if (type.equals("P"))
                            arguments.add(oc.readValue(argument.traverse(oc), P.class));
                        else if (type.equals("Lambda"))
                            arguments.add(oc.readValue(argument.traverse(oc), Lambda.class));
                        else
                            arguments.add(oc.readValue(argument.traverse(oc), Enum.class));
                    } else {
                        arguments.add(oc.readValue(argument.traverse(oc), Object.class)); // TODO: vertices/edges/etc. don't get processed correctly
                    }
                } else if (argument.getNodeType().equals(JsonNodeType.NUMBER)) {
                    arguments.add(argument.asInt()); // TODO
                } else if (argument.getNodeType().equals(JsonNodeType.STRING)) {
                    arguments.add(argument.textValue());
                } else if (argument.getNodeType().equals(JsonNodeType.BOOLEAN)) {
                    arguments.add(argument.booleanValue());
                } else if (argument.getNodeType().equals(JsonNodeType.ARRAY)) {
                    final List<Object> list = new ArrayList<>();
                    for (int k = 0; k < argument.size(); k++) {
                        list.add(oc.readValue(argument.get(k).traverse(oc), Object.class));
                        //list.add(argument.get(k).textValue());
                    }
                    arguments.add(list);
                } else {
                    throw new IOException("Unknown argument: " + argument);
                }
            }
            if (source)
                bytecode.addSource(operator, arguments.toArray());
            else
                bytecode.addStep(operator, arguments.toArray());
        }

        @Override
        public Bytecode deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final Bytecode bytecode = new Bytecode();
            final ObjectCodec oc = jsonParser.getCodec();
            final JsonNode node = oc.readTree(jsonParser);
            assert node.get("@type").textValue().equals("Bytecode");
            if (node.has("source")) {
                final JsonNode source = node.get("source");
                for (int i = 0; i < source.size(); i++) {
                    processInstruction(source.get(i), oc, bytecode, true);
                }
            }
            if (node.has("step")) {
                final JsonNode step = node.get("step");
                for (int i = 0; i < step.size(); i++) {
                    processInstruction(step.get(i), oc, bytecode, false);

                }
            }
            return bytecode;
        }
    }

    final static class EnumJacksonDeserializer extends StdDeserializer<Enum> {

        public EnumJacksonDeserializer() {
            super(Enum.class);
        }

        @Override
        public Enum deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {

            final ObjectCodec oc = jsonParser.getCodec();
            final JsonNode node = oc.readTree(jsonParser);
            final String type = node.get("@type").textValue();
            if (type.equals("Cardinality"))
                return VertexProperty.Cardinality.valueOf(node.get("value").textValue());
            else if (type.equals("Column"))
                return Column.valueOf(node.get("value").textValue());
            else if (type.equals("Direction"))
                return Direction.valueOf(node.get("value").textValue());
            else if (type.equals("Barrier"))
                return SackFunctions.Barrier.valueOf(node.get("value").textValue());
            else if (type.equals("Operator"))
                return Operator.valueOf(node.get("value").textValue());
            else if (type.equals("Order"))
                return Order.valueOf(node.get("value").textValue());
            else if (type.equals("Pop"))
                return Pop.valueOf(node.get("value").textValue());
            else if (type.equals("Scope"))
                return Scope.valueOf(node.get("value").textValue());
            else if (type.equals("T"))
                return T.valueOf(node.get("value").textValue());
            else
                throw new IOException("Unknown enum type: " + type);

        }
    }

    final static class PJacksonDeserializer extends StdDeserializer<P> {

        public PJacksonDeserializer() {
            super(P.class);
        }

        @Override
        public P deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {

            final ObjectCodec oc = jsonParser.getCodec();
            final JsonNode node = oc.readTree(jsonParser);
            assert node.get("@type").textValue().equals("P");
            final JsonNode predicate = node.get("predicate");
            if (predicate.textValue().equals("and") || predicate.textValue().equals("or")) {
                final List<P<?>> arguments = new ArrayList<>();
                for (int i = 0; i < node.get("value").size(); i++) {
                    arguments.add(oc.readValue(node.get("value").get(i).traverse(oc), P.class));
                }
                return predicate.textValue().equals("and") ? new AndP(arguments) : new OrP(arguments);
            } else {
                try {
                    final Object argument = oc.readValue(node.get("value").traverse(oc), Object.class);
                    return (P) P.class.getMethod(predicate.textValue(), argument instanceof Collection ? Collection.class : Object.class).invoke(null, argument); // TODO: number stuff, eh?
                } catch (final Exception e) {
                    throw new IOException(e.getMessage(), e);
                }
            }
        }
    }

    final static class LambdaJacksonDeserializer extends StdDeserializer<Lambda> {

        public LambdaJacksonDeserializer() {
            super(Lambda.class);
        }

        @Override
        public Lambda deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {

            final ObjectCodec oc = jsonParser.getCodec();
            final JsonNode node = oc.readTree(jsonParser);
            assert node.get("@type").textValue().equals("Lambda");
            final String lambdaScript = node.get("value").textValue();
            final String lambdaLanguage = node.get("language").textValue();
            final int lambdaArguments = node.get("arguments").intValue();
            if (-1 == lambdaArguments || lambdaArguments > 2)
                return new Lambda.UnknownArgLambda(lambdaScript, lambdaLanguage, lambdaArguments);
            else if (0 == lambdaArguments)
                return new Lambda.ZeroArgLambda<>(lambdaScript, lambdaLanguage);
            else if (1 == lambdaArguments)
                return new Lambda.OneArgLambda<>(lambdaScript, lambdaLanguage);
            else
                return new Lambda.TwoArgLambda<>(lambdaScript, lambdaLanguage);
        }
    }

    final static class BindingJacksonDeserializer extends StdDeserializer<Bytecode.Binding> {

        public BindingJacksonDeserializer() {
            super(Bytecode.Binding.class);
        }

        @Override
        public Bytecode.Binding deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final ObjectCodec oc = jsonParser.getCodec();
            final JsonNode node = oc.readTree(jsonParser);
            assert node.get("@type").textValue().equals("Binding");
            final String variable = node.get("variable").textValue();
            final Object value = oc.readValue(node.get("value").traverse(oc), Object.class);
            return new Bytecode.Binding<>(variable, value);
        }
    }

    static class TraverserJacksonDeserializer extends StdDeserializer<Traverser> {

        public TraverserJacksonDeserializer() {
            super(Traverser.class);
        }

        @Override
        public Traverser deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            jsonParser.nextToken();
            // This will automatically parse all typed stuff.
            final Map<String, Object> mapData = deserializationContext.readValue(jsonParser, Map.class);
            return new DefaultRemoteTraverser<>(mapData.get("value"), (Long) mapData.get("bulk"));
        }
    }
}
