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
package org.apache.tinkerpop.machine.bytecode;

import org.apache.tinkerpop.machine.traverser.Traverser;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Argument<E> implements Serializable {

    private final E arg;
    private final boolean isPrimitive;

    private Argument(final Object arg) {
        this.isPrimitive = !(arg instanceof Bytecode);
        this.arg = this.isPrimitive ? (E) arg : (E) Compilation.compileOne(arg);
    }

    private <C, S> Compilation<C, S, E> getCompilation() {
        return (Compilation<C, S, E>) this.arg;
    }


    public final <C, S> E getArg(final Traverser<C, S> traverser) {
        return this.isPrimitive ? this.arg : this.<C, S>getCompilation().mapTraverser(traverser).object();
    }

    public static <E> Argument<E> create(final Object arg) {
        return new Argument<>(arg);
    }

    @Override
    public String toString() {
        return this.arg.toString();
    }
}
