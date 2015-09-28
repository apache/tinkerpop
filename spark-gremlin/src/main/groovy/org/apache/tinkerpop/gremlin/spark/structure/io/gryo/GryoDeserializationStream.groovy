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

package org.apache.tinkerpop.gremlin.spark.structure.io.gryo

import org.apache.spark.serializer.DeserializationStream
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader
import org.apache.tinkerpop.shaded.kryo.KryoException
import org.apache.tinkerpop.shaded.kryo.io.Input
import scala.reflect.ClassTag

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GryoDeserializationStream extends DeserializationStream {

    private final Input input;
    private final GryoSerializerInstance gryoSerializer;

    public GryoDeserializationStream(final GryoSerializerInstance gryoSerializer, final InputStream inputStream) {
        this.gryoSerializer = gryoSerializer;
        this.input = new Input(inputStream);
    }

    @Override
    public <T> T readObject(final ClassTag<T> classTag) {
        try {
            final GryoReader gryoReader = this.gryoSerializer.getGryoPool().takeReader();
            final T t = (T) gryoReader.getKryo().readClassAndObject(this.input);
            this.gryoSerializer.getGryoPool().offerReader(gryoReader);
            return t;
        } catch (final Throwable e) {
            if (e instanceof KryoException) {
                final KryoException kryoException = (KryoException) e;
                if (kryoException.getMessage().toLowerCase().contains("buffer underflow")) {
                    throw new EOFException();
                }
            }
            throw e;
        }
    }

    @Override
    public void close() {
        this.input.close();
    }
}