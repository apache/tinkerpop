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
package org.apache.tinkerpop.gremlin.process.remote.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.InputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.OutputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class DefaultRemoteTraverserSerializers {

    private DefaultRemoteTraverserSerializers() {}

    /**
     * Serializes {@link DefaultRemoteTraverser} to and from Gryo.
     */
    public final static class GryoSerializer implements SerializerShim<DefaultRemoteTraverser> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final DefaultRemoteTraverser remoteTraverser) {
            kryo.writeClassAndObject(output, remoteTraverser.get());
            output.writeLong(remoteTraverser.bulk());
        }

        @Override
        public <I extends InputShim> DefaultRemoteTraverser read(final KryoShim<I, ?> kryo, final I input, final Class<DefaultRemoteTraverser> remoteTraverserClass) {
            final Object o = kryo.readClassAndObject(input);
            return new DefaultRemoteTraverser<>(o, input.readLong());
        }
    }

    public final static class GraphSONSerializer extends StdSerializer<Traverser> {

        public GraphSONSerializer() {
            super(Traverser.class);
        }

        @Override
        public void serialize(final Traverser traverserInstance, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(traverserInstance, jsonGenerator, serializerProvider, null);
        }

        @Override
        public void serializeWithType(final Traverser traverserInstance, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(traverserInstance, jsonGenerator, serializerProvider, typeSerializer);
        }

        private static void ser(final Traverser traverserInstance, final JsonGenerator jsonGenerator,
                                final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            jsonGenerator.writeStartObject();
            if (typeSerializer != null) jsonGenerator.writeStringField(GraphSONTokens.CLASS, Traverser.class.getName());
            jsonGenerator.writeObjectField("bulk", traverserInstance.bulk());
            jsonGenerator.writeObjectField("value", traverserInstance.get());
            jsonGenerator.writeEndObject();
        }
    }
}
