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
package org.apache.tinkerpop.machine.processor.rxjava;

import org.apache.tinkerpop.language.gremlin.Gremlin;
import org.apache.tinkerpop.language.gremlin.TraversalSource;
import org.apache.tinkerpop.language.gremlin.core.__;
import org.apache.tinkerpop.machine.Machine;
import org.apache.tinkerpop.machine.processor.pipes.PipesProcessor;
import org.apache.tinkerpop.machine.species.LocalMachine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class RxJavaBenchmark {

    //@Test
    void benchmark() {
        final Machine machine = LocalMachine.open();
        final TraversalSource ser = Gremlin.traversal(machine).withProcessor(RxJavaProcessor.class);
        final TraversalSource par = Gremlin.traversal(machine).withProcessor(RxJavaProcessor.class, Map.of(RxJavaProcessor.RX_THREAD_POOL_SIZE, Runtime.getRuntime().availableProcessors() - 1));
        final TraversalSource pipes = Gremlin.traversal(machine).withProcessor(PipesProcessor.class);
        final List<Long> input = new ArrayList<>(1000);
        for (long i = 0; i < 5000; i++) {
            input.add(i + 1);
        }
        final int runs = 30;
        System.out.println("Threads used: " + (Runtime.getRuntime().availableProcessors() - 1));
        System.out.println("Input size: " + input.size());
        float serTime = 0.0f;
        float parTime = 0.0f;
        float pipTime = 0.0f;
        for (int i = 0; i < runs; i++) {
            String source = "ser";
            for (final TraversalSource g : List.of(ser, par, pipes)) {
                final long time = System.currentTimeMillis();
                g.inject(input).unfold().repeat(__.incr()).times(4).iterate();
                if (i > 1) {
                    if ("ser".equals(source)) {
                        serTime = serTime + (System.currentTimeMillis() - time);
                        source = "par";
                    } else if ("par".equals(source)) {
                        parTime = parTime + (System.currentTimeMillis() - time);
                        source = "pip";
                    } else {
                        pipTime = pipTime + (System.currentTimeMillis() - time);
                        source = "ser";
                    }
                }

            }
        }
        System.out.println("Average time [seri]: " + serTime / runs);
        System.out.println("Average time [para]: " + parTime / runs);
        System.out.println("Average time [pipe]: " + pipTime / runs);
    }
}
