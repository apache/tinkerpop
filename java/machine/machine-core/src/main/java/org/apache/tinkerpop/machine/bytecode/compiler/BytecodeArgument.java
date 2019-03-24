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
package org.apache.tinkerpop.machine.bytecode.compiler;

import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.traverser.Traverser;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BytecodeArgument<E> implements Argument<E> {

    private final Compilation compilation;

    public BytecodeArgument(final Bytecode arg) {
        this.compilation = Compilation.compileOne(arg);
    }

    @Override
    public <C, S> E mapArg(final Traverser<C, S> traverser) {
        return (E) this.compilation.mapTraverser(traverser).object();
    }

    @Override
    public <C, S> boolean filterArg(final Traverser<C, S> traverser) {
        return this.compilation.filterTraverser(traverser);
    }

    @Override
    public String toString() {
        return this.compilation.toString();
    }
}
