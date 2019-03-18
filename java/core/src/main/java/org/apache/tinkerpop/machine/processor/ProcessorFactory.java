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
package org.apache.tinkerpop.machine.processor;

import org.apache.tinkerpop.machine.bytecode.Compilation;
import org.apache.tinkerpop.machine.strategy.Strategy;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface ProcessorFactory extends Serializable {

    public <C, S, E> Processor<C, S, E> mint(final Compilation<C, S, E> compilation);

    public Set<Strategy<?>> getStrategies();

    // public Optional<Compiler> getCompiler();

    public static Set<Strategy<?>> processorStrategies(final Class<? extends ProcessorFactory> processFactoryClass) {
        try {
            return processFactoryClass.getConstructor().newInstance().getStrategies();
        } catch (final NoSuchMethodException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | InstantiationException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
