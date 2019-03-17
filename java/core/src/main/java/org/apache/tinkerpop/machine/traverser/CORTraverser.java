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
public class CORTraverser<C, S> extends COTraverser<C, S> {

    private short loops = 0;

    public CORTraverser(final Coefficient<C> coefficient, final S object) {
        super(coefficient, object);
    }

    @Override
    public void incrLoops() {
        this.loops++;
    }

    @Override
    public int loops() {
        return this.loops;
    }

    @Override
    public void resetLoops() {
        this.loops = 0;
    }

    @Override
    public <E> Traverser<C, E> split(final CFunction<C> function, final E object) {
        final CORTraverser<C,E> clone = (CORTraverser<C,E>) this.clone();
        clone.object = object;
        return clone;
    }


}
