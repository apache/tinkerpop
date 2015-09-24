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

import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import scala.reflect.ClassTag;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GryoSerializationStream extends SerializationStream {

    private final OutputStream outputStream;
    private final SerializerInstance serializer;

    public GryoSerializationStream(final GryoSerializerInstance serializer, final OutputStream outputStream) {
        this.outputStream = outputStream;
        this.serializer = serializer;
    }

    @Override
    public <T> SerializationStream writeObject(final T t, final ClassTag<T> classTag) {
        try {
            this.outputStream.write(this.serializer.serialize(t, classTag).array());
            this.outputStream.flush();
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        return this;
    }

    @Override
    public void flush() {
        try {
            this.outputStream.flush();
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        try {
            this.outputStream.close();
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}