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
import org.apache.tinkerpop.shaded.kryo.io.Input;
import scala.reflect.ClassTag;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GryoDeserializationStream extends DeserializationStream {

    private final InputStream inputStream;
    private final GryoSerializerInstance serializer;

    public GryoDeserializationStream(final GryoSerializerInstance serializer, final InputStream inputStream) {
        this.serializer = serializer;
        this.inputStream = inputStream;
    }

    @Override
    public <T> T readObject(final ClassTag<T> classTag) {
        return (T) this.serializer.getKryo().readClassAndObject(new Input(this.inputStream));
    }

    @Override
    public void close() {
        try {
            this.inputStream.close();
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}