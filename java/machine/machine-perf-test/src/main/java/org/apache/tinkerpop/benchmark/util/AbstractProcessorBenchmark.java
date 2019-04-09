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
package org.apache.tinkerpop.benchmark.util;

import org.apache.tinkerpop.language.gremlin.Gremlin;
import org.apache.tinkerpop.language.gremlin.TraversalSource;
import org.apache.tinkerpop.machine.Machine;
import org.apache.tinkerpop.machine.processor.pipes.PipesProcessor;
import org.apache.tinkerpop.machine.processor.rxjava.RxJavaProcessor;
import org.apache.tinkerpop.machine.species.LocalMachine;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@State(Scope.Thread)
public class AbstractProcessorBenchmark extends AbstractBenchmarkBase {
    protected TraversalSource g;
    protected List<Long> input;

    private final int ELEMENT_COUNT = 1000;

    @Setup
    public void prepare() {
        final WithProcessor[] withProcessors = this.getClass().getAnnotationsByType(WithProcessor.class);
        WithProcessor withProcessor = withProcessors.length == 0 ? null : withProcessors[0];
        final ProcessorType processorType = withProcessor.type();

        final Machine machine = LocalMachine.open();
        g = Gremlin.traversal(machine);
        switch (processorType) {
            case PIPES:
                g = g.withProcessor(PipesProcessor.class);
            case RX_SERIAL:
                g = g.withProcessor(RxJavaProcessor.class);
                break;
            case RX_PARALLEL:
                g = g.withProcessor(RxJavaProcessor.class,
                        Map.of(RxJavaProcessor.RX_THREAD_POOL_SIZE, Runtime.getRuntime().availableProcessors() - 1));
                break;
            default:
                throw new RuntimeException("Unrecognized processor type");
        }

        input = new ArrayList<>(ELEMENT_COUNT);
        for (long i = 0; i < ELEMENT_COUNT; i++) {
            input.add(i+1);
        }
    }
}
