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

import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.tinkerpop.benchmark.util.AbstractBenchmarkBase;
import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
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
    private static NettyBufferFactory bufferFactory = new NettyBufferFactory();

    @State(Scope.Thread)
    public static class BenchmarkState {
        public Buffer bytecodeBuffer1 = bufferFactory.create(allocator.buffer(2048));
        public Buffer bytecodeBuffer2 = bufferFactory.create(allocator.buffer(2048));
        public Buffer pBuffer1 = bufferFactory.create(allocator.buffer(2048));
        public final GremlinLang gremlinLang1 = new GremlinLang();

        public Buffer bufferWrite = bufferFactory.create(allocator.buffer(2048));

        public GremlinLang gremlinLang2;

        @Setup(Level.Trial)
        public void doSetup() throws IOException {
            gremlinLang1.addStep("V");
            gremlinLang1.addStep("values", "name");
            gremlinLang1.addStep("tail", 5);

            Graph g = TinkerGraph.open();

            gremlinLang2 = g.traversal()
                    .addV("person")
                    .property("name1", 1)
                    .property("name2", UUID.randomUUID())
                    .property("name3", InetAddress.getByAddress(new byte[] { 127, 0, 0, 1}))
                    .property("name4", BigInteger.valueOf(33343455342245L))
                    .property("name5", "kjlkdnvlkdrnvldnvndlrkvnlhkjdkgkrtnlkndblknlknonboirnlkbnrtbonrobinokbnrklnbkrnblktengotrngotkrnglkt")
                    .property("name6", Instant.now())
                    .asAdmin().getGremlinLang();

            writer.writeValue(gremlinLang1, bytecodeBuffer1, false);
            writer.writeValue(gremlinLang2, bytecodeBuffer2, false);
            writer.writeValue(P.between(1, 2), pBuffer1, false);
        }

        @Setup(Level.Invocation)
        public void setupInvocation() {
            bytecodeBuffer1.readerIndex(0);
            bytecodeBuffer2.readerIndex(0);
            pBuffer1.readerIndex(0);
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
    public void writeBytecode1(BenchmarkState state) throws IOException {
        writer.writeValue(state.gremlinLang1, state.bufferWrite, false);
    }

    @Benchmark
    public void writeBytecode2(BenchmarkState state) throws IOException {
        writer.writeValue(state.gremlinLang2, state.bufferWrite, false);
    }

    @Benchmark
    public void readBytecode1(BenchmarkState state) throws IOException {
        reader.readValue(state.bytecodeBuffer1, GremlinLang.class, false);
    }

    @Benchmark
    public void readBytecode2(BenchmarkState state) throws IOException {
        reader.readValue(state.bytecodeBuffer2, GremlinLang.class, false);
    }

    @Benchmark
    public void readP1(BenchmarkState state) throws IOException {
        reader.readValue(state.pBuffer1, P.class, false);
    }
}
