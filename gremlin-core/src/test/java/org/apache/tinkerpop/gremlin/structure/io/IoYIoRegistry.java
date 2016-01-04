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

import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoYIoRegistry {
    public static class ConstructorBased extends AbstractIoRegistry {
        public ConstructorBased() {
            register(GryoIo.class, IoY.class, null);
        }
    }

    public static class InstanceBased extends AbstractIoRegistry {
        private static InstanceBased INSTANCE = new InstanceBased();

        private InstanceBased() {
            register(GryoIo.class, IoY.class, null);
        }

        public static InstanceBased getInstance() {
            return INSTANCE;
        }
    }

    /**
     * Converts an {@link IoX} to a {@link HashMap}.
     */
    public final static class IoYToHashMapSerializer extends Serializer<IoY> {
        public IoYToHashMapSerializer() {
        }

        @Override
        public void write(final Kryo kryo, final Output output, final IoY ioy) {
            final Map<String, Object> map = new HashMap<>();
            map.put("y", ioy.toString());
            try (final OutputStream stream = new ByteArrayOutputStream()) {
                final Output detachedOutput = new Output(stream);
                kryo.writeObject(detachedOutput, map);

                // have to remove the first byte because it marks a reference we don't want to have as this
                // serializer is trying to "act" like a Map
                final byte[] b = detachedOutput.toBytes();
                output.write(b, 1, b.length - 1);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        @Override
        public IoY read(final Kryo kryo, final Input input, final Class<IoY> ioyClass) {
            throw new UnsupportedOperationException("IoX writes to Map and can't be read back in as IoX");
        }
    }
}
