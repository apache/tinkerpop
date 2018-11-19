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
package org.apache.tinkerpop.gremlin.driver;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.tinkerpop.benchmark.util.AbstractBenchmarkBase;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.DataType;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.openjdk.jmh.annotations.Benchmark;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class SerializationBenchmark extends AbstractBenchmarkBase {

    private static final ByteBuf RequestMessageGraphSONBuffer1 = Unpooled.wrappedBuffer(
            ("{\"requestId\":{\"@type\":\"g:UUID\",\"@value\":\"9b6d17c0-c5a9-418e-bff6-a25fbb1b175e\"}," +
                    "\"op\":\"a\",\"processor\":\"b\",\"args\":{}}")
                    .getBytes(StandardCharsets.UTF_8));

    private static final ByteBuf RequestMessageGraphSONBuffer2 = Unpooled.wrappedBuffer(
            ("{\"requestId\":{\"@type\":\"g:UUID\",\"@value\":\"042b8400-d586-4fcb-b085-2cf2ab2bd5cb\"}," +
                    "\"op\":\"bytecode\",\"processor\":\"traversal\",\"args\":{\"gremlin\":" +
                    "{\"@type\":\"g:Bytecode\",\"@value\":{\"step\":[[\"V\"],[\"tail\"]]}},\"aliases\":{\"g\":\"g\"}}}")
                    .getBytes(StandardCharsets.UTF_8));

    private static final ByteBuf RequestMessageBinaryBuffer1 = Unpooled.wrappedBuffer(new byte[]{
            // flag
            0x1,
            // uuid
            (byte) 0xd3, (byte) 0xfd, 0x35, 0x40, 0x67, 0x18, 0x46, (byte) 0x87,(byte) 0x95, 0x6b, (byte) 0xc8, 0x61,
            (byte) 0x8a, 0x26, (byte) 0xe3, 0x35,
            // string length and string value (a)
            0, 0, 0, 0x01, 0x61,
            // string length and string value (b)
            0, 0, 0, 0x01, 0x62,
            // Map (no items)
            0, 0, 0, 0
    });

    private static final ByteBuf RequestMessageBinaryBuffer2 = Unpooled.wrappedBuffer(new byte[]{
            // flag
            0x1,
            // uuid
            (byte) 0xd3, (byte) 0xfd, 0x35, 0x40, 0x67, 0x18, 0x46, (byte) 0x87,(byte) 0x95, 0x6b, (byte) 0xc8, 0x61,
            (byte) 0x8a, 0x26, (byte) 0xe3, 0x35,
            // string length and string value (a)
            0, 0, 0, 0x01, 0x61,
            // string length and string value (b)
            0, 0, 0, 0x01, 0x62,
            // Map (2 items)
            0, 0, 0, 0x2,
            // "aliases"
            DataType.STRING.getCodeByte(), 0, 0, 0, 0, 0x07, 0x61, 0x6c, 0x69, 0x61, 0x73, 0x65, 0x73,
            // map { g: g }
            DataType.MAP.getCodeByte(), 0, 0, 0, 0, 0x1,
            DataType.STRING.getCodeByte(), 0, 0, 0, 0, 0x01, 0x67,
            DataType.STRING.getCodeByte(), 0, 0, 0, 0, 0x01, 0x67,
            // "gremlin"
            DataType.STRING.getCodeByte(), 0, 0, 0, 0, 0x07, 0x67, 0x72, 0x65, 0x6d, 0x6c, 0x69, 0x6e,
            // Bytecode for ['V', 'tail']
            DataType.BYTECODE.getCodeByte(), 0, 0, 0, 0, 0x02,
            // "V" (no values)
            0, 0, 0, 0x1, 0x56, 0, 0, 0, 0,
            // tail (no values)
            0, 0, 0, 0x4, 0x74, 0x61, 0x69, 0x6c, 0, 0, 0, 0,
            // no sources
            0, 0, 0, 0
    });

    private static final UUID id = UUID.randomUUID();

    private static final GraphBinaryReader binaryReader = new GraphBinaryReader();
    private static final GraphSONMessageSerializerV3d0 graphsonReader = new GraphSONMessageSerializerV3d0();

    @Benchmark
    public RequestMessage testReadMessage1Binary() throws SerializationException {
        RequestMessageBinaryBuffer1.readerIndex(0);
        return binaryReader.readValue(RequestMessageBinaryBuffer1, RequestMessage.class, false);
    }

    @Benchmark
    public RequestMessage testReadMessage2Binary() throws SerializationException {
        RequestMessageBinaryBuffer2.readerIndex(0);
        return binaryReader.readValue(RequestMessageBinaryBuffer2, RequestMessage.class, false);
    }

    @Benchmark
    public RequestMessage testReadMessage1GraphSON() throws SerializationException {
        RequestMessageGraphSONBuffer1.readerIndex(0);
        return graphsonReader.deserializeRequest(RequestMessageGraphSONBuffer1);
    }

    @Benchmark
    public RequestMessage testReadMessage2GraphSON() throws SerializationException {
        RequestMessageGraphSONBuffer2.readerIndex(0);
        return graphsonReader.deserializeRequest(RequestMessageGraphSONBuffer2);
    }

    @Benchmark
    public RequestMessage testInstanceCreation() {
        return RequestMessage.build("a").overrideRequestId(id).processor("b").create();
    }
}
