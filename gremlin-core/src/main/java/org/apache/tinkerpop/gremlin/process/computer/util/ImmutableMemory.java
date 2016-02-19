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
package org.apache.tinkerpop.gremlin.process.computer.util;

import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ImmutableMemory implements Memory.Admin {

    private final Memory baseMemory;

    public ImmutableMemory(final Memory baseMemory) {
        this.baseMemory = baseMemory;
    }

    @Override
    public Set<String> keys() {
        return this.baseMemory.keys();
    }

    @Override
    public <R> R get(final String key) throws IllegalArgumentException {
        return this.baseMemory.get(key);
    }

    @Override
    public void set(final String key, final Object value) {
        throw Memory.Exceptions.memoryIsCurrentlyImmutable();
    }

    @Override
    public int getIteration() {
        return this.baseMemory.getIteration();
    }

    @Override
    public long getRuntime() {
        return this.baseMemory.getRuntime();
    }

    @Override
    public void add(final String key, final Object value) {
        throw Memory.Exceptions.memoryIsCurrentlyImmutable();
    }

    @Override
    public void incrIteration() {
        throw Memory.Exceptions.memoryIsCurrentlyImmutable();
    }

    @Override
    public void setIteration(final int iteration) {
        throw Memory.Exceptions.memoryIsCurrentlyImmutable();
    }

    @Override
    public void setRuntime(final long runtime) {
        throw Memory.Exceptions.memoryIsCurrentlyImmutable();
    }

    @Override
    public String toString() {
        return StringFactory.memoryString(this.baseMemory);
    }
}
