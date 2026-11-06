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
package org.apache.tinkerpop.gremlin.structure.io.binary.types;

import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Serializes a {@link ByteBuffer} as the GraphBinary {@code binary} type. The value written is the buffer's entire
 * backing array, as returned by {@link ByteBuffer#array()}, regardless of the buffer's position, limit or array
 * offset, so a buffer that is a view of part of a larger array, such as one from
 * {@link ByteBuffer#wrap(byte[], int, int)} or {@link ByteBuffer#slice()}, is written with every byte of that array.
 * The buffer must also be backed by an accessible array, so a direct or read-only buffer cannot be written. Wrap
 * exactly the bytes to send, as with {@link ByteBuffer#wrap(byte[])}. A value that is read back is a new heap buffer
 * positioned at its first byte.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class BinarySerializer extends SimpleTypeSerializer<ByteBuffer> {

    public BinarySerializer() {
        super(DataType.BINARY);
    }

    @Override
    protected ByteBuffer readValue(final Buffer buffer, final GraphBinaryReader context) throws IOException {
        final ByteBuffer bb = ByteBuffer.allocate(buffer.readInt());
        buffer.readBytes(bb);
        bb.rewind();
        return bb;
    }

    @Override
    protected void writeValue(final ByteBuffer value, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
        final byte[] bytes = value.array();
        buffer.writeInt(bytes.length).writeBytes(bytes);
    }
}
