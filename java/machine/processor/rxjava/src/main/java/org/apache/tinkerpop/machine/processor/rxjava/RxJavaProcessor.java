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

import org.apache.tinkerpop.machine.bytecode.compiler.Compilation;
import org.apache.tinkerpop.machine.processor.Processor;
import org.apache.tinkerpop.machine.processor.ProcessorFactory;
import org.apache.tinkerpop.machine.processor.rxjava.strategy.RxJavaStrategy;
import org.apache.tinkerpop.machine.strategy.Strategy;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RxJavaProcessor implements ProcessorFactory {

    public static final String RXJAVA_THREADS = "rxjava.threads";

    private final Map<String, Object> configuration;

    public RxJavaProcessor() {
        this.configuration = Collections.emptyMap();
    }

    public RxJavaProcessor(final Map<String, Object> configuration) {
        this.configuration = configuration;
    }

    @Override
    public <C, S, E> Processor<C, S, E> mint(final Compilation<C, S, E> compilation) {
        return this.configuration.containsKey(RXJAVA_THREADS) ?
                new ParallelRxJava<>(compilation, (int) this.configuration.get(RXJAVA_THREADS)) :
                new SerialRxJava<>(compilation);
    }

    @Override
    public Set<Strategy<?>> getStrategies() {
        return Set.of(new RxJavaStrategy());
    }
}
