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
package org.apache.tinkerpop.gremlin.spark.structure.io.gryo.kryoshim.unshaded;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim;

public class UnshadedSerializerAdapter<T> extends Serializer<T>
{

    SerializerShim<T> serializer;

    public UnshadedSerializerAdapter(final SerializerShim<T> serializer) {
        this.serializer = serializer;
        setImmutable(this.serializer.isImmutable());
    }

    @Override
    public void write(final Kryo kryo, final Output output, final T t) {
        UnshadedKryoAdapter shadedKryoAdapter = new UnshadedKryoAdapter(kryo);
        UnshadedOutputAdapter shadedOutputAdapter = new UnshadedOutputAdapter(output);
        serializer.write(shadedKryoAdapter, shadedOutputAdapter, t);
    }

    @Override
    public T read(final Kryo kryo, final Input input, final Class<T> aClass) {
        UnshadedKryoAdapter shadedKryoAdapter = new UnshadedKryoAdapter(kryo);
        UnshadedInputAdapter shadedInputAdapter = new UnshadedInputAdapter(input);
        return serializer.read(shadedKryoAdapter, shadedInputAdapter, aClass);
    }
}
