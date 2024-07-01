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

import org.apache.tinkerpop.benchmark.util.AbstractBenchmarkBase;
import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
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
public class GraphSONMapperBenchmark extends AbstractBenchmarkBase {
    private static final ObjectMapper mapper = GraphSONMapper.build()
            .version(GraphSONVersion.V3_0)
            .addDefaultXModule(true)
            .create().createMapper();

    @State(Scope.Thread)
    public static class BenchmarkState {

        public byte[] gremlinBytes1;
        private byte[] gremlinBytes2;
        private final GremlinLang gremlinLang1 = new GremlinLang();
        private GremlinLang gremlinLang2;

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


            gremlinBytes1 = mapper.writeValueAsBytes(gremlinLang1);
            gremlinBytes2 = mapper.writeValueAsBytes(gremlinLang2);
        }

        @TearDown(Level.Trial)
        public void doTearDown() {
        }
    }

    @Benchmark
    public void readBytecode1(BenchmarkState state) throws IOException {
        mapper.readValue(state.gremlinBytes1, GremlinLang.class);
    }

    @Benchmark
    public void readBytecode2(BenchmarkState state) throws IOException {
        mapper.readValue(state.gremlinBytes2, GremlinLang.class);
    }

    @Benchmark
    public void writeBytecode1(BenchmarkState state) throws IOException {
        mapper.writeValueAsString(state.gremlinLang1);
    }

    @Benchmark
    public void writeBytecode2(BenchmarkState state) throws IOException {
        mapper.writeValueAsBytes(state.gremlinLang2);
    }
}
