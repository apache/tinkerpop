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
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.tinkerpop.benchmark.util.AbstractBenchmarkBase;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.time.Instant;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Warmup(time = 200, timeUnit = MILLISECONDS)
public class GraphBinaryReaderWriterBenchmark extends AbstractBenchmarkBase {
    private static GraphBinaryReader reader = new GraphBinaryReader();
    private static GraphBinaryWriter writer = new GraphBinaryWriter();
    private static UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);

    @State(Scope.Thread)
    public static class BenchmarkState {
        public ByteBuf bytecodeBuffer1 = allocator.buffer(2048);
        public ByteBuf bytecodeBuffer2 = allocator.buffer(2048);
        public final Bytecode bytecode1 = new Bytecode();

        public ByteBuf bufferWrite = allocator.buffer(2048);

        public Bytecode bytecode2;

        @Setup(Level.Trial)
        public void doSetup() throws IOException, SerializationException {
            bytecode1.addStep("V");
            bytecode1.addStep("values", "name");
            bytecode1.addStep("tail", 5);

            Graph g = TinkerGraph.open();

            bytecode2 = g.traversal()
                    .addV("person")
                    .property("name1", 1)
                    .property("name2", UUID.randomUUID())
                    .property("name3", InetAddress.getByAddress(new byte[] { 127, 0, 0, 1}))
                    .property("name4", BigInteger.valueOf(33343455342245L))
                    .property("name5", "kjlkdnvlkdrnvldnvndlrkvnlhkjdkgkrtnlkndblknlknonboirnlkbnrtbonrobinokbnrklnbkrnblktengotrngotkrnglkt")
                    .property("name6", Instant.now())
                    .asAdmin().getBytecode();

            writer.writeValue(bytecode1, bytecodeBuffer1, false);
            writer.writeValue(bytecode2, bytecodeBuffer2, false);
        }

        @Setup(Level.Invocation)
        public void setupInvocation() {
            bytecodeBuffer1.readerIndex(0);
            bytecodeBuffer2.readerIndex(0);
            bufferWrite.readerIndex(0);
            bufferWrite.writerIndex(0);
        }

        @TearDown(Level.Trial)
        public void doTearDown() {
            bytecodeBuffer1.release();
            bytecodeBuffer2.release();
            bufferWrite.release();
        }
    }

    @Benchmark
    public void writeBytecode1(BenchmarkState state) throws SerializationException {
        writer.writeValue(state.bytecode1, state.bufferWrite, false);
    }

    @Benchmark
    public void writeBytecode2(BenchmarkState state) throws SerializationException {
        writer.writeValue(state.bytecode2, state.bufferWrite, false);
    }

    @Benchmark
    public void readBytecode1(BenchmarkState state) throws SerializationException {
        reader.readValue(state.bytecodeBuffer1, Bytecode.class, false);
    }

    @Benchmark
    public void readBytecode2(BenchmarkState state) throws SerializationException {
        reader.readValue(state.bytecodeBuffer2, Bytecode.class, false);
    }
}
