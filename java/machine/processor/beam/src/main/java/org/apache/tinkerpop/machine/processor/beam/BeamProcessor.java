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
package org.apache.tinkerpop.machine.processor.beam;

import org.apache.tinkerpop.machine.bytecode.compiler.BytecodeCompiler;
import org.apache.tinkerpop.machine.bytecode.compiler.CommonCompiler;
import org.apache.tinkerpop.machine.bytecode.compiler.Compilation;
import org.apache.tinkerpop.machine.bytecode.compiler.CoreCompiler;
import org.apache.tinkerpop.machine.processor.Processor;
import org.apache.tinkerpop.machine.processor.ProcessorFactory;
import org.apache.tinkerpop.machine.processor.beam.strategy.BeamStrategy;
import org.apache.tinkerpop.machine.strategy.Strategy;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BeamProcessor implements ProcessorFactory {

    private static final List<BytecodeCompiler> COMPILERS = List.of(CoreCompiler.instance(), CommonCompiler.instance());
    private static final int DEFAULT_SERVER_PORT = 8888;
    public static final String TRAVERSER_SERVER_LOCATION = "beam.traverserServer.location";
    public static final String TRAVERSER_SERVER_PORT = "beam.traverserServer.port";

    private final Map<String, Object> configuration;

    public BeamProcessor() {
        this.configuration = Collections.emptyMap();
    }

    public BeamProcessor(final Map<String, Object> configuration) {
        this.configuration = configuration;
    }

    @Override
    public <C, S, E> Processor<C, S, E> mint(final Compilation<C, S, E> compilation) {
        try {
            return new Beam<>(compilation,
                    (String) this.configuration.getOrDefault(TRAVERSER_SERVER_LOCATION, InetAddress.getLocalHost().getHostAddress()),
                    (Integer) this.configuration.getOrDefault(TRAVERSER_SERVER_PORT, DEFAULT_SERVER_PORT),
                    !this.configuration.containsKey(TRAVERSER_SERVER_LOCATION));
        } catch (final UnknownHostException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public Set<Strategy<?>> getStrategies() {
        return Set.of(new BeamStrategy());
    }

    @Override
    public List<BytecodeCompiler> getCompilers() {
        return COMPILERS;
    }
}
