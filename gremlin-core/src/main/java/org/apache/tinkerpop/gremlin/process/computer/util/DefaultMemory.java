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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultMemory implements Memory.Admin {

    private Map<String, Object> memory = new HashMap<>();
    private int iteration = 0;
    private long runtime = 0l;

    public DefaultMemory() {

    }

    public DefaultMemory(final Memory.Admin copyMemory) {
        this.iteration = copyMemory.getIteration();
        this.runtime = copyMemory.getRuntime();
        copyMemory.keys().forEach(key -> this.memory.put(key, copyMemory.get(key)));
    }

    @Override
    public void setIteration(final int iteration) {
        this.iteration = iteration;
    }

    @Override
    public void setRuntime(final long runtime) {
        this.runtime = runtime;
    }

    @Override
    public Set<String> keys() {
        return Collections.unmodifiableSet(this.memory.keySet());
    }

    @Override
    public <R> R get(final String key) throws IllegalArgumentException {
        final R r = (R) this.memory.get(key);
        if (null == r)
            throw Memory.Exceptions.memoryDoesNotExist(key);
        else
            return r;
    }

    @Override
    public void set(final String key, final Object value) {
        this.memory.put(key, value);
    }

    @Override
    public int getIteration() {
        return this.iteration;
    }

    @Override
    public long getRuntime() {
        return this.runtime;
    }

    @Override
    public long incr(final String key, final long delta) {
        final Long value = (Long) this.memory.get(key);
        final Long newValue = (null == value) ? delta : delta + value;
        this.memory.put(key, newValue);
        return newValue;
    }

    @Override
    public boolean and(final String key, final boolean bool) {
        final Boolean value = (Boolean) this.memory.get(key);
        final Boolean newValue = (null == value) ? bool : bool && value;
        this.memory.put(key, newValue);
        return newValue;
    }

    @Override
    public boolean or(final String key, final boolean bool) {
        final Boolean value = (Boolean) this.memory.get(key);
        final Boolean newValue = (null == value) ? bool : bool || value;
        this.memory.put(key, newValue);
        return newValue;
    }
}
