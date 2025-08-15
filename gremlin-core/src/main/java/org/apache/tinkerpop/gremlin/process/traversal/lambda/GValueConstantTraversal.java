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
package org.apache.tinkerpop.gremlin.process.traversal.lambda;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;

/**
 * A {@link Traversal} that always returns a constant value.
 */
public final class GValueConstantTraversal<S, E> extends AbstractLambdaTraversal<S, E> {

    private GValue<E> end;
    private ConstantTraversal<S, E> constantTraversal;

    public GValueConstantTraversal(final GValue<E> end) {
        this.end = end == null ? GValue.of(null) : end;
        this.constantTraversal = new ConstantTraversal<>(this.end.get());
    }

    @Override
    public E next() {
        return constantTraversal.next();
    }

    @Override
    public String toString() {
        return constantTraversal.toString();
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode() ^ end.hashCode();
    }

    @Override
    public boolean equals(final Object other) {
        if (other instanceof GValueConstantTraversal) {
            return this.end.equals(((GValueConstantTraversal<?, ?>) other).end);
        }
        return constantTraversal.equals(other);
    }

    public ConstantTraversal<S, E> getConstantTraversal() {
        return constantTraversal;
    }

    public boolean isParameterized() {
        return end.isVariable();
    }

    public GValue<E> getGValue() {
        return end;
    }

    public void updateVariable(String name, Object value) {
        if (name.equals(end.getName())) {
            end = (GValue<E>) GValue.of(name, value);
            this.constantTraversal = new ConstantTraversal<>(end.get());
        }
    }
}