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

import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.BytecodeUtil;
import org.apache.tinkerpop.machine.functions.CFunction;
import org.apache.tinkerpop.machine.traversers.TraverserFactory;

import java.io.Serializable;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface ProcessorFactory extends Serializable {

    public default <C, S, E> Processor<C, S, E> mint(final Bytecode<C> bytecode) {
        return this.mint(BytecodeUtil.getTraverserFactory(bytecode).get(), BytecodeUtil.compile(BytecodeUtil.strategize(bytecode)));
    }

    public <C, S, E> Processor<C, S, E> mint(final TraverserFactory<C> traverserFactory, final List<CFunction<C>> functions);
}
