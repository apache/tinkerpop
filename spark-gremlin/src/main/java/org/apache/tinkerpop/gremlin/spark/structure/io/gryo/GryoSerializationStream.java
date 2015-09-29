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

import org.apache.spark.serializer.SerializationStream;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import scala.reflect.ClassTag;

import java.io.OutputStream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GryoSerializationStream extends SerializationStream {

    private final Output output;
    private final GryoSerializerInstance gryoSerializer;

    public GryoSerializationStream(final GryoSerializerInstance gryoSerializer, final OutputStream outputStream) {
        this.output = new Output(outputStream);
        this.gryoSerializer = gryoSerializer;
    }

    @Override
    public <T> SerializationStream writeObject(final T t, final ClassTag<T> classTag) {
        this.gryoSerializer.getGryoPool().writeWithKryo(kryo -> kryo.writeClassAndObject(this.output, t));
        return this;
    }

    @Override
    public void flush() {
        this.output.flush();
    }

    @Override
    public void close() {
        this.output.close();
    }
}