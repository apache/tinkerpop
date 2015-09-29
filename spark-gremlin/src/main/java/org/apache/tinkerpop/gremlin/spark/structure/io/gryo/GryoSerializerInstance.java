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

package org.apache.tinkerpop.gremlin.spark.structure.io.gryo;

import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoPool;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import scala.reflect.ClassTag;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GryoSerializerInstance extends SerializerInstance {

    private final GryoSerializer gryoSerializer;
    private final Output output;
    private final Input input;

    public GryoSerializerInstance(final GryoSerializer gryoSerializer) {
        this.gryoSerializer = gryoSerializer;
        this.input = new Input();
        this.output = gryoSerializer.newOutput();
    }

    @Override
    public <T> ByteBuffer serialize(final T t, final ClassTag<T> classTag) {
        this.gryoSerializer.getGryoPool().writeWithKryo(kryo -> kryo.writeClassAndObject(this.output, t));
        return ByteBuffer.wrap(this.output.getBuffer());
    }

    @Override
    public <T> T deserialize(final ByteBuffer byteBuffer, final ClassTag<T> classTag) {
        this.input.setBuffer(byteBuffer.array());
        return this.gryoSerializer.getGryoPool().readWithKryo(kryo -> (T) kryo.readClassAndObject(this.input));
    }

    @Override
    public <T> T deserialize(final ByteBuffer byteBuffer, final ClassLoader classLoader, final ClassTag<T> classTag) {
        this.input.setBuffer(byteBuffer.array());
        return this.gryoSerializer.getGryoPool().readWithKryo(kryo -> {
            kryo.setClassLoader(classLoader);
            return (T) kryo.readClassAndObject(this.input);
        });
    }

    @Override
    public SerializationStream serializeStream(final OutputStream outputStream) {
        return new GryoSerializationStream(this, outputStream);
    }

    @Override
    public DeserializationStream deserializeStream(final InputStream inputStream) {
        return new GryoDeserializationStream(this, inputStream);
    }

    public GryoPool getGryoPool() {
        return this.gryoSerializer.getGryoPool();
    }
}