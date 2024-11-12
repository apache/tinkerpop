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
package org.apache.tinkerpop.gremlin.structure.io.graphbinary;

import io.netty.buffer.ByteBufAllocator;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.tinkerpop.gremlin.structure.io.AbstractTypedCompatibilityTest;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class GraphBinaryCompatibilityTest extends AbstractTypedCompatibilityTest {

    private static final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private static final GraphBinaryWriter writerV4 = new GraphBinaryWriter();
    private static final GraphBinaryReader readerV4 = new GraphBinaryReader();
    private static final NettyBufferFactory bufferFactory = new NettyBufferFactory();

    private static final String testCaseDataPath = root.getPath() + File.separator + "test-case-data" + File.separator
            + "io" + File.separator + "graphbinary";

    static {
        resetDirectory(testCaseDataPath);
    }

    @Parameterized.Parameters(name = "expect({0})")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"v4", readerV4, writerV4 },
        });
    }

    @Parameterized.Parameter(value = 0)
    public String compatibility;

    @Parameterized.Parameter(value = 1)
    public GraphBinaryReader reader;

    @Parameterized.Parameter(value = 2)
    public GraphBinaryWriter writer;

    @Override
    protected String getCompatibility() {
        return compatibility;
    }

    @Override
    protected byte[] readFromResource(final String resource) throws IOException {
        final String testResource = resource + "-" + compatibility + ".gbin";
        return IOUtils.toByteArray(GraphBinaryResourceAccess.class.getResourceAsStream(testResource));
    }

    @Override
    public <T> T read(final byte[] bytes, final Class<T> clazz) throws Exception {
        final Buffer buffer = bufferFactory.create(allocator.buffer());
        buffer.writeBytes(bytes);
        return reader.read(buffer);
    }

    @Override
    public byte[] write(final Object o, final Class<?> clazz, final String entryName) throws Exception  {
        final Buffer buffer = bufferFactory.create(allocator.buffer());
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            writer.write(o, buffer);
            buffer.readerIndex(0);
            buffer.readBytes(stream, buffer.readableBytes());

            final byte[] bytes = stream.toByteArray();

            // write out files for debugging purposes
            final File f = new File(testCaseDataPath + File.separator + entryName + "-" + getCompatibility() + ".gbin");
            if (f.exists()) f.delete();
            try (FileOutputStream fileOuputStream = new FileOutputStream(f)) {
                fileOuputStream.write(bytes);
            }

            return bytes;
        }
    }

//    TODO: revisit
//    @Override
//    public void shouldReadWriteAuthenticationChallenge() throws Exception {
//        super.shouldReadWriteAuthenticationChallenge();
//    }
}
