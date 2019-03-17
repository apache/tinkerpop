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
public class COPTraverser<C, S> extends COTraverser<C, S> {

    private BasicPath path = new BasicPath();

    COPTraverser(final Coefficient<C> coefficient, final S object) {
        super(coefficient, object);
    }

    public Coefficient<C> coefficient() {
        return this.coefficient;
    }

    public S object() {
        return this.object;
    }

    public BasicPath path() {
        return this.path;
    }

    @Override
    public <E> Traverser<C, E> split(final CFunction<C> function, final E object) {
        final COPTraverser<C, E> clone = (COPTraverser<C, E>) this.clone();
        clone.object = object;
        clone.path.add(function.labels(), object);
        return clone;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof COPTraverser && ((COPTraverser<C, S>) other).object.equals(this.object);
    }

    @Override
    public int hashCode() {
        return this.object.hashCode(); // TODO: include path
    }

    @Override
    public Traverser<C, S> clone() {
        final COPTraverser<C, S> clone = (COPTraverser<C, S>) super.clone();
        clone.object = this.object;
        clone.coefficient = this.coefficient.clone();
        clone.path = new BasicPath(this.path);
        return clone;
    }

}
