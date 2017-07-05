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
package org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.shaded;

import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

public class ShadedSerializerAdapter<T> extends Serializer<T> {

    private final SerializerShim<T> serializer;

    public ShadedSerializerAdapter(final SerializerShim<T> serializer) {
        this.serializer = serializer;
        setImmutable(this.serializer.isImmutable());
    }

    @Override
    public void write(final Kryo kryo, final Output output, final T t) {
        /* These adapters could be cached pretty efficiently in instance fields if it were guaranteed that this
         * class was never subject to concurrent use.  That's true of Kryo instances, but it is not clear that
         * it is true of Serializer instances.
         */
        final ShadedKryoAdapter shadedKryoAdapter = new ShadedKryoAdapter(kryo);
        final ShadedOutputAdapter shadedOutputAdapter = new ShadedOutputAdapter(output);
        serializer.write(shadedKryoAdapter, shadedOutputAdapter, t);
    }

    @Override
    public T read(final Kryo kryo, final Input input, final Class<T> aClass) {
        // Same caching opportunity as in write(...)
        final ShadedKryoAdapter shadedKryoAdapter = new ShadedKryoAdapter(kryo);
        final ShadedInputAdapter shadedInputAdapter = new ShadedInputAdapter(input);
        return serializer.read(shadedKryoAdapter, shadedInputAdapter, aClass);
    }

    public SerializerShim<T> getSerializerShim() {
        return this.serializer;
    }
}
