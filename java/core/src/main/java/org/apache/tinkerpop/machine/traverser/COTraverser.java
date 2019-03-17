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
public class COTraverser<C, S> implements Traverser<C, S> {

    private Coefficient<C> coefficient;
    private final S object;

    COTraverser(final Coefficient<C> coefficient, final S object) {
        this.coefficient = coefficient;
        this.object = object;
    }

    @Override
    public Coefficient<C> coefficient() {
        return this.coefficient;
    }

    @Override
    public S object() {
        return this.object;
    }

    @Override
    public Path path() {
        return EmptyPath.instance();
    }

    @Override
    public <E> Traverser<C, E> split(final CFunction<C> function, final E object) {
        return new COTraverser<>(this.coefficient.clone().multiply(function.coefficient().value()), object);
    }

    @Override
    public Traverser<C, S> clone() {
        try {
            final COTraverser<C, S> clone = (COTraverser<C, S>) super.clone();
            clone.coefficient = this.coefficient.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
