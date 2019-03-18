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
package org.apache.tinkerpop.machine.structure;

import org.apache.tinkerpop.machine.bytecode.BytecodeCompiler;
import org.apache.tinkerpop.machine.strategy.Strategy;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EmptyStructure implements Structure, StructureFactory {

    private static final EmptyStructure INSTANCE = new EmptyStructure();

    private EmptyStructure() {
        // do nothing
    }

    @Override
    public Structure mint(final Map<String, Object> configuration) {
        return this;
    }

    @Override
    public List<Strategy> getStrategies() {
        return Collections.emptyList();
    }

    @Override
    public Optional<BytecodeCompiler> getCompiler() {
        return Optional.empty();
    }

    public static EmptyStructure instance() {
        return EmptyStructure.INSTANCE;
    }
}
