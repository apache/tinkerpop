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

import org.apache.tinkerpop.machine.bytecode.BytecodeUtil;
import org.apache.tinkerpop.machine.bytecode.compiler.Compilation;
import org.apache.tinkerpop.machine.processor.Processor;
import org.apache.tinkerpop.machine.processor.ProcessorFactory;
import org.apache.tinkerpop.machine.processor.rxjava.strategy.RxJavaStrategy;
import org.apache.tinkerpop.machine.strategy.Strategy;
import org.apache.tinkerpop.machine.util.StringFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RxJavaProcessor implements ProcessorFactory {

    public static final String RXJAVA_THREADS = "rxjava.threads";
    public static final String RX_BYCODE_ID = "rx:bytecodeId";
    private static final Map<String, ExecutorService> THREAD_POOLS = new ConcurrentHashMap<>();

    private final Map<String, Object> configuration;

    public RxJavaProcessor() {
        this.configuration = Collections.emptyMap();
    }

    public RxJavaProcessor(final Map<String, Object> configuration) {
        this.configuration = configuration;
    }

    @Override
    public <C, S, E> Processor<C, S, E> mint(final Compilation<C, S, E> compilation) {
        final int threads = (int) this.configuration.getOrDefault(RxJavaProcessor.RXJAVA_THREADS, 0);
        final String id = (String) BytecodeUtil.getSourceInstructions(BytecodeUtil.getRootBytecode(compilation.getBytecode()), RX_BYCODE_ID).get(0).args()[0];
        final ExecutorService threadPool = RxJavaProcessor.THREAD_POOLS.compute(id, (key, value) -> null == value && threads > 0 ? Executors.newCachedThreadPool() : value);
        // System.out.println(id + "--" + threadPool + "---" + THREAD_POOLS);
        return null == threadPool ?
                new SerialRxJava<>(compilation) :
                new ParallelRxJava<>(compilation, threadPool);
    }

    @Override
    public Set<Strategy<?>> getStrategies() {
        return Set.of(new RxJavaStrategy());
    }

    @Override
    public String toString() {
        return StringFactory.makeProcessorFactoryString(this);
    }
}
