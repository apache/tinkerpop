/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.util.ser;

import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.BufferFactory;

import java.nio.ByteBuffer;

/**
 * A {@link BufferFactory} that creates heap-based {@link Buffer} instances without requiring Netty.
 */
public class HeapBufferFactory implements BufferFactory<byte[]> {

    @Override
    public Buffer create(final byte[] value) {
        final HeapBuffer buffer = new HeapBuffer(value.length);
        buffer.writeBytes(value);
        return buffer;
    }

    @Override
    public Buffer create(final int initialCapacity) {
        return new HeapBuffer(initialCapacity);
    }

    @Override
    public Buffer wrap(final ByteBuffer value) {
        return HeapBuffer.wrap(value);
    }
}
