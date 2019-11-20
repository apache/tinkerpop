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
package org.apache.tinkerpop.gremlin.structure.io.gryo;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * An {@link InputStream} implementation that can independently process a Gryo file written with
 * {@link GryoWriter#writeVertices(OutputStream, Iterator)}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class VertexByteArrayInputStream extends FilterInputStream {

    private static final byte[] vertexTerminatorClass = new byte[]{15, 1, 1, 9};
    private static final byte[] pattern = ByteBuffer.allocate(vertexTerminatorClass.length + 8).put(vertexTerminatorClass).putLong(4185403236219066774L).array();

    public VertexByteArrayInputStream(final InputStream inputStream) {
        super(inputStream);
    }

    /**
     * Read the bytes of the next {@link Vertex} in the stream. The returned
     * stream can then be passed to {@link GryoReader#readVertex(java.io.InputStream, java.util.function.Function)}.
     */
    public ByteArrayOutputStream readVertexBytes() throws IOException {
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        final LinkedList<Byte> buffer = new LinkedList<>();

        int current = read();
        while (current > -1 && (buffer.size() < 12 || !isMatch(buffer))) {
            stream.write(current);

            current = read();
            if (buffer.size() > 11)
                buffer.removeFirst();

            buffer.addLast((byte) current);
        }

        stream.write(current);
        return stream;
    }

    private static boolean isMatch(final List<Byte> input) {
        for (int i = 0; i < pattern.length; i++) {
            if (pattern[i] != input.get(i)) {
                return false;
            }
        }
        return true;
    }
}
