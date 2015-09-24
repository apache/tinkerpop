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

package org.apache.tinkerpop.gremlin.spark.process.computer.io.gryo;

import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import scala.reflect.ClassTag;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GryoSerializerInstance extends SerializerInstance {

    private final Kryo kryo;

    public GryoSerializerInstance(final Kryo kryo) {
        this.kryo = kryo;
    }

    @Override
    public <T> ByteBuffer serialize(final T t, final ClassTag<T> classTag) {
        final Output output = new Output(100000);
        this.kryo.writeClassAndObject(output, t);
        return ByteBuffer.wrap(output.getBuffer());
    }

    @Override
    public <T> T deserialize(final ByteBuffer byteBuffer, final ClassTag<T> classTag) {
        return (T) this.kryo.readClassAndObject(new Input(byteBuffer.array()));
    }

    @Override
    public <T> T deserialize(final ByteBuffer byteBuffer, final ClassLoader classLoader, final ClassTag<T> classTag) {
        this.kryo.setClassLoader(classLoader);
        return (T) this.kryo.readClassAndObject(new Input(byteBuffer.array()));
    }

    @Override
    public SerializationStream serializeStream(final OutputStream outputStream) {
        return new GryoSerializationStream(this, outputStream);
    }

    @Override
    public DeserializationStream deserializeStream(final InputStream inputStream) {
        return new GryoDeserializationStream(this, inputStream);
    }

    public Kryo getKryo() {
        return this.kryo;
    }
}