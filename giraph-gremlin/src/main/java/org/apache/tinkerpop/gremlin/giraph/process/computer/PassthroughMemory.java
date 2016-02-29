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

package org.apache.tinkerpop.gremlin.giraph.process.computer;

import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PassThroughMemory implements Memory.Admin {

    private final GiraphMemory giraphMemory;
    private long runtime = 0l;
    private int iteration = -1;
    private final Map<String, Object> memoryMap = new HashMap<>();

    public PassThroughMemory(final GiraphMemory giraphMemory) {
        this.giraphMemory = giraphMemory;
        giraphMemory.keys().forEach(key -> this.memoryMap.put(key, giraphMemory.get(key)));
        this.iteration = giraphMemory.getIteration();
    }

    @Override
    public Set<String> keys() {
        return this.memoryMap.keySet();
    }

    @Override
    public <R> R get(final String key) throws IllegalArgumentException {
        final R r = (R) this.memoryMap.get(key);
        if (null == r)
            throw Memory.Exceptions.memoryDoesNotExist(key);
        else
            return r;
    }

    @Override
    public void set(final String key, Object value) {
        this.memoryMap.put(key, value);
        this.giraphMemory.set(key, value);
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
    public void add(final String key, final Object value) {
        this.giraphMemory.add(key, value);
    }

    @Override
    public String toString() {
        return StringFactory.memoryString(this);
    }

    @Override
    public void incrIteration() {
        this.iteration = this.iteration + 1;
    }

    @Override
    public void setIteration(final int iteration) {
        this.iteration = iteration;
    }

    @Override
    public void setRuntime(long runtime) {
        this.runtime = runtime;
    }
}
