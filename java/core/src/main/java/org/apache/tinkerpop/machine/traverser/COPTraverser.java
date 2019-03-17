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
import org.apache.tinkerpop.machine.function.ReduceFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class COPTraverser<C, S> implements Traverser<C, S> {

    private Coefficient<C> coefficient;
    private S object;
    private BasicPath path = new BasicPath();

    COPTraverser(final Coefficient<C> coefficient, final S object) {
        this.coefficient = coefficient;
        this.object = object;
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
    public <E> Traverser<C, E> split(final CFunction<C> function, final E newObject) {
        final COPTraverser<C, E> clone = new COPTraverser<>(
                function instanceof ReduceFunction ?
                        function.coefficient().clone().unity() :
                        function.coefficient().clone().multiply(this.coefficient().value()), newObject);
        clone.path = function instanceof ReduceFunction ? new BasicPath() : new BasicPath(this.path);
        clone.path.add(function.labels(), newObject);
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
    public String toString() {
        return this.object.toString();
    }

    @Override
    public COPTraverser<C, S> clone() {
        try {
            final COPTraverser<C, S> clone = (COPTraverser<C, S>) super.clone();
            clone.object = this.object;
            clone.coefficient = this.coefficient.clone();
            clone.path = new BasicPath(this.path);
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

}
