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
package org.apache.tinkerpop.gremlin.structure.io;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoXIoRegistry {
    public static class ConstructorBased extends AbstractIoRegistry {
        public ConstructorBased() {
            register(GryoIo.class, IoX.class, null);
        }
    }

    public static class InstanceBased extends AbstractIoRegistry {
        private static InstanceBased INSTANCE = new InstanceBased();

        private InstanceBased() {
            register(GryoIo.class, IoX.class, null);
        }

        public static InstanceBased getInstance() {
            return INSTANCE;
        }
    }

    /**
     * Converts an {@link IoX} to a {@link DetachedVertex}.
     */
    public final static class IoXToVertexSerializer extends Serializer<IoX> {
        public IoXToVertexSerializer() {
        }

        @Override
        public void write(final Kryo kryo, final Output output, final IoX iox) {
            final Map<String,Object> props = new HashMap<>();
            addSingleProperty("x", iox.toString(), props);
            final DetachedVertex vertex = new DetachedVertex(100, Vertex.DEFAULT_LABEL, props);
            try (final OutputStream stream = new ByteArrayOutputStream()) {
                final Output detachedOutput = new Output(stream);
                kryo.writeObject(detachedOutput, vertex);

                // have to remove the first byte because it marks a reference we don't want to have as this
                // serializer is trying to "act" like a DetachedVertex
                final byte[] b = detachedOutput.toBytes();
                output.write(b, 1, b.length - 1);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        @Override
        public IoX read(final Kryo kryo, final Input input, final Class<IoX> ioxClass) {
            throw new UnsupportedOperationException("IoX writes to DetachedVertex and can't be read back in as IoX");
        }

        private static void addSingleProperty(final String key, final Object value, final Map<String, Object> props)
        {
            final List<Map<String, Object>> propertyNames = new ArrayList<>(1);
            final Map<String,Object> propertyName = new HashMap<>();
            propertyName.put(GraphSONTokens.ID, key);
            propertyName.put(GraphSONTokens.KEY, key);
            propertyName.put(GraphSONTokens.VALUE, value);
            propertyNames.add(propertyName);

            props.put(key, propertyNames);
        }
    }
}
