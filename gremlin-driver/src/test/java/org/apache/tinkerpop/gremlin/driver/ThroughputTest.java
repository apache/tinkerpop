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
package org.apache.tinkerpop.gremlin.driver;

import org.apache.tinkerpop.gremlin.util.ser.Serializers;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Simple throughput test that measures request/sec against a local Gremlin Server.
 * Requires a running server at localhost:8182. Annotated with @Ignore so it does not
 * run in CI.
 */
@Ignore("Requires a running Gremlin Server at localhost:8182")
public class ThroughputTest {

    private static final int WARMUP = 100;
    private static final int ITERATIONS = 1000;

    @Test
    public void testGraphBinaryThroughput() throws Exception {
        runThroughput("GraphBinary", Serializers.GRAPHBINARY_V4);
    }

    @Test
    public void testGraphSONThroughput() throws Exception {
        runThroughput("GraphSON", Serializers.GRAPHSON_V4);
    }

    private void runThroughput(final String name, final Serializers serializer) throws Exception {
        final Cluster cluster = Cluster.build("localhost")
                .port(8182)
                .serializer(serializer)
                .create();
        final Client client = cluster.connect();

        try {
            // warmup
            for (int i = 0; i < WARMUP; i++) {
                client.submit("g.inject(1)").all().get();
            }

            // measurement
            final long start = System.nanoTime();
            for (int i = 0; i < ITERATIONS; i++) {
                client.submit("g.inject(1)").all().get();
            }
            final long elapsed = System.nanoTime() - start;

            final double totalMs = elapsed / 1_000_000.0;
            final double rps = ITERATIONS / (totalMs / 1000.0);
            final double avgLatencyMs = totalMs / ITERATIONS;

            System.out.printf("[%s] Total: %.1f ms | Requests/sec: %.1f | Avg latency: %.2f ms%n",
                    name, totalMs, rps, avgLatencyMs);
        } finally {
            client.close();
            cluster.close();
        }
    }
}
