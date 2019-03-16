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
package org.apache.tinkerpop.machine.traverser;

import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.CFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EmptyTraverser<C, S> implements Traverser<C, S> {

    private static final EmptyTraverser INSTANCE = new EmptyTraverser();

    private EmptyTraverser() {
        // for static instances
    }

    @Override
    public Coefficient<C> coefficient() {
        throw new IllegalStateException(EmptyTraverser.class.getSimpleName() + " does not contain a coefficient");
    }

    @Override
    public S object() {
        throw new IllegalStateException(EmptyTraverser.class.getSimpleName() + " does not contain an object");
    }

    @Override
    public Path path() {
        throw new IllegalStateException(EmptyTraverser.class.getSimpleName() + " does not contain a path");
    }

    @Override
    public <E> Traverser<C, E> split(CFunction<C> function, E object) {
        return INSTANCE;
    }

    @Override
    public Traverser<C, S> clone() {
        return INSTANCE;
    }

    public static final <C, S> EmptyTraverser<C, S> instance() {
        return INSTANCE;
    }
}
