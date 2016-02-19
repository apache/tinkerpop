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
package org.apache.tinkerpop.gremlin.spark.process.computer;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.MemoryHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkMemory implements Memory.Admin, Serializable {

    public final Map<String, MemoryComputeKey> memoryKeys = new HashMap<>();
    private final AtomicInteger iteration = new AtomicInteger(0);   // do these need to be atomics?
    private final AtomicLong runtime = new AtomicLong(0l);
    private final Map<String, Accumulator> memory = new HashMap<>();
    private Broadcast<Map<String, Object>> broadcast;
    private boolean inTask = false;

    public SparkMemory(final VertexProgram<?> vertexProgram, final Set<MapReduce> mapReducers, final JavaSparkContext sparkContext) {
        if (null != vertexProgram) {
            for (final MemoryComputeKey key : vertexProgram.getMemoryComputeKeys()) {
                MemoryHelper.validateKey(key.getKey());
                this.memoryKeys.put(key.getKey(), key);
            }
        }
        for (final MapReduce mapReduce : mapReducers) {
            this.memoryKeys.put(mapReduce.getMemoryKey(), MemoryComputeKey.of(mapReduce.getMemoryKey(), MemoryComputeKey.setOperator(), false));
        }
        for (final MemoryComputeKey key : this.memoryKeys.values()) {
            this.memory.put(key.getKey(), sparkContext.accumulator(null, key.getKey(), new MemoryAccumulator<>(key)));

        }
        this.broadcast = sparkContext.broadcast(new HashMap<>());
    }

    @Override
    public Set<String> keys() {
        if (this.inTask)
            return this.broadcast.getValue().keySet();
        else {
            final Set<String> trueKeys = new HashSet<>();
            this.memory.forEach((key, value) -> {
                if (value.value() != null)
                    trueKeys.add(key);
            });
            return Collections.unmodifiableSet(trueKeys);
        }
    }

    @Override
    public void incrIteration() {
        this.iteration.getAndIncrement();
    }

    @Override
    public void setIteration(final int iteration) {
        this.iteration.set(iteration);
    }

    @Override
    public int getIteration() {
        return this.iteration.get();
    }

    @Override
    public void setRuntime(final long runTime) {
        this.runtime.set(runTime);
    }

    @Override
    public long getRuntime() {
        return this.runtime.get();
    }

    @Override
    public <R> R get(final String key) throws IllegalArgumentException {
        final R r = this.getValue(key);
        if (null == r)
            throw Memory.Exceptions.memoryDoesNotExist(key);
        else
            return r;
    }

    @Override
    public void add(final String key, final Object value) {
        checkKeyValue(key, value);
        if (this.inTask)
            this.memory.get(key).add(value);
        else
            this.memory.get(key).setValue(value);
    }

    @Override
    public void set(final String key, final Object value) {
        checkKeyValue(key, value);
        if (this.inTask)
            this.memory.get(key).add(value);
        else
            this.memory.get(key).setValue(value);
    }

    @Override
    public String toString() {
        return StringFactory.memoryString(this);
    }

    protected void setInTask(final boolean inTask) {
        this.inTask = inTask;
    }

    protected void broadcastMemory(final JavaSparkContext sparkContext) {
        this.broadcast.destroy(true); // do we need to block?
        final Map<String, Object> toBroadcast = new HashMap<>();
        this.memory.forEach((key, object) -> {
            if (null != object.value())
                toBroadcast.put(key, object.value());
        });
        this.broadcast = sparkContext.broadcast(toBroadcast);
    }

    private void checkKeyValue(final String key, final Object value) {
        if (!this.memoryKeys.containsKey(key))
            throw GraphComputer.Exceptions.providedKeyIsNotAMemoryComputeKey(key);
        MemoryHelper.validateValue(value);
    }

    private <R> R getValue(final String key) {
        return this.inTask ? (R) this.broadcast.value().get(key) : (R) this.memory.get(key).value();
    }
}
