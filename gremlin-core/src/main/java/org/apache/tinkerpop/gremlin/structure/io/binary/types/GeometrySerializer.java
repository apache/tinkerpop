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

import org.apache.tinkerpop.gremlin.structure.geo.Geometry;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Serializes a {@link Geometry} as its SRID followed by its ISO WKB. The end of the WKB is found
 * by parsing it, so the value occupies exactly the bytes of the geometry and a value that follows
 * it in the same buffer is read next.
 * <p>
 * The WKB is written big-endian, as {@link Geometry#toWKB()} produces it. A value that is read
 * may carry either byte order, because every geometry header declares its own.
 */
public class GeometrySerializer extends SimpleTypeSerializer<Geometry> {

    public GeometrySerializer() {
        super(DataType.GEOMETRY);
    }

    @Override
    protected Geometry readValue(final Buffer buffer, final GraphBinaryReader context) throws IOException {
        final int srid = buffer.readInt();
        try {
            return Geometry.fromWkb(new BufferInputStream(buffer), srid);
        } catch (EOFException eofe) {
            throw new IOException("The geometry value ends before its WKB is complete", eofe);
        } catch (IllegalArgumentException iae) {
            throw new IOException(iae.getMessage(), iae);
        }
    }

    @Override
    protected void writeValue(final Geometry value, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
        buffer.writeInt(value.srid()).writeBytes(value.toWKB());
    }

    /**
     * Reads the readable bytes of a {@link Buffer}, advancing its reader index, so that the WKB
     * parser can consume exactly the bytes of one geometry and leave the rest of the buffer to
     * the values that follow.
     */
    private static final class BufferInputStream extends InputStream {

        private final Buffer buffer;

        private BufferInputStream(final Buffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public int read() throws IOException {
            try {
                return buffer.readByte() & 0xff;
            } catch (RuntimeException re) {
                throw endOfValue(re);
            }
        }

        @Override
        public int read(final byte[] destination, final int offset, final int length) throws IOException {
            if (destination == null) {
                throw new NullPointerException("Destination array cannot be null");
            }
            if (offset < 0 || length < 0 || length > destination.length - offset) {
                throw new IndexOutOfBoundsException("Offset " + offset + " and length " + length +
                        " do not fit an array of " + destination.length + " bytes");
            }
            if (length == 0) {
                return 0;
            }

            try {
                buffer.readBytes(destination, offset, length);
            } catch (RuntimeException re) {
                throw endOfValue(re);
            }
            return length;
        }

        /**
         * Reports a buffer that runs out inside a value as an end of stream. A {@link Buffer} may be
         * backed by a stream whose remaining length is unknown, so the end is found by reading rather
         * than by asking, and both implementations raise a runtime exception when a read runs past it.
         */
        private static EOFException endOfValue(final RuntimeException cause) {
            final EOFException eofe = new EOFException("The geometry value ends before its WKB is complete");
            eofe.initCause(cause);
            return eofe;
        }
    }
}
