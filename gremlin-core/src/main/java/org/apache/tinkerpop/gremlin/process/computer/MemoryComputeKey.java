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

package org.apache.tinkerpop.gremlin.process.computer;

import org.apache.tinkerpop.gremlin.process.computer.util.MemoryHelper;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.BinaryOperator;

/**
 * A {@code MemoryComputeKey} specifies what keys will be used by a {@link Memory} during a {@link GraphComputer} computation.
 * A MemoryComputeKey maintains a {@link BinaryOperator} which specifies how to reduce parallel values into a single value.
 * A MemoryComputeKey can be broadcasted and as such, the workers will receive mutations to the {@link Memory} value.
 * A MemoryComputeKey can be transient and thus, will not be accessible once the {@link GraphComputer} computation is complete.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MemoryComputeKey<A> implements Serializable, Cloneable {

    private final String key;
    private BinaryOperator<A> reducer;
    private final boolean isTransient;
    private final boolean isBroadcast;

    private MemoryComputeKey(final String key, final BinaryOperator<A> reducer, final boolean isBroadcast, final boolean isTransient) {
        this.key = key;
        this.reducer = reducer;
        this.isTransient = isTransient;
        this.isBroadcast = isBroadcast;
        MemoryHelper.validateKey(key);
    }

    public String getKey() {
        return this.key;
    }

    public boolean isTransient() {
        return this.isTransient;
    }

    public boolean isBroadcast() {
        return this.isBroadcast;
    }

    public BinaryOperator<A> getReducer() {
        return this.reducer;
    }

    @Override
    public int hashCode() {
        return this.key.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof MemoryComputeKey && ((MemoryComputeKey) object).key.equals(this.key);
    }

    @Override
    public MemoryComputeKey<A> clone() {
        try {
            final MemoryComputeKey<A> clone = (MemoryComputeKey<A>) super.clone();

            try {
                final Method cloneMethod = this.reducer.getClass().getMethod("clone");
                if (cloneMethod != null)
                    clone.reducer = (BinaryOperator<A>) cloneMethod.invoke(this.reducer);
            } catch(Exception ignored) {

            }

            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public static <A> MemoryComputeKey<A> of(final String key, final BinaryOperator<A> reducer, final boolean isBroadcast, final boolean isTransient) {
        return new MemoryComputeKey<>(key, reducer, isBroadcast, isTransient);
    }


}
