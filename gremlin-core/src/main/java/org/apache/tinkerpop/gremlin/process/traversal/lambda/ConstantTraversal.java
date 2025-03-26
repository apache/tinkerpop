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

import java.util.Objects;

/**
 * A {@link Traversal} that always returns a constant value.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ConstantTraversal<S, E> extends AbstractLambdaTraversal<S, E> {

    private final GValue<E> end;

    public ConstantTraversal(final E end) {
        this.end = null == end ? GValue.of(null, null) : GValue.of(null, end);
    }

    public ConstantTraversal(final GValue<E> end) {
        this.end = null == end ? GValue.of(null, null) : end;
    }

    @Override
    public E next() {
        return this.end.get();
    }

    public GValue<E> getEnd() {
        return end;
    }

    @Override
    public String toString() {
        return "(" + Objects.toString(this.end) + ")";
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode() ^ Objects.hashCode(this.end);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ConstantTraversal
                && Objects.equals(((ConstantTraversal) other).end, this.end);
    }
}