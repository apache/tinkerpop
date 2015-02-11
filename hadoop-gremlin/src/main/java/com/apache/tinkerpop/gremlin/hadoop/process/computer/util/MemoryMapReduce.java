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
package com.apache.tinkerpop.gremlin.hadoop.process.computer.util;

import com.apache.tinkerpop.gremlin.hadoop.Constants;
import com.apache.tinkerpop.gremlin.process.computer.KeyValue;
import com.apache.tinkerpop.gremlin.process.computer.MapReduce;
import com.apache.tinkerpop.gremlin.process.computer.Memory;
import com.apache.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.apache.tinkerpop.gremlin.process.computer.util.MapMemory;
import com.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import com.apache.tinkerpop.gremlin.structure.Vertex;
import com.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MemoryMapReduce extends StaticMapReduce<MapReduce.NullObject, MapMemory, MapReduce.NullObject, MapMemory, MapMemory> {

    public Set<String> memoryKeys = new HashSet<>();

    @Override
    public String getMemoryKey() {
        return Constants.SYSTEM_MEMORY;
    }

    private MemoryMapReduce() {

    }

    public MemoryMapReduce(final Set<String> memoryKeys) {
        this.memoryKeys = memoryKeys;
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        configuration.setProperty(Constants.GREMLIN_HADOOP_MEMORY_KEYS, new ArrayList<>(this.memoryKeys.size()));
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.memoryKeys = new HashSet((List) configuration.getList(Constants.GREMLIN_HADOOP_MEMORY_KEYS));
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<NullObject, MapMemory> emitter) {
        final MapMemory mapMemory = vertex.<MapMemory>property(Constants.MAP_MEMORY).orElse(new MapMemory());
        emitter.emit(mapMemory);
    }

    @Override
    public void combine(final NullObject key, final Iterator<MapMemory> values, final ReduceEmitter<NullObject, MapMemory> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public void reduce(final NullObject key, final Iterator<MapMemory> values, final ReduceEmitter<NullObject, MapMemory> emitter) {
        emitter.emit(key, values.next());
    }

    @Override
    public MapMemory generateFinalResult(final Iterator<KeyValue<NullObject, MapMemory>> keyValues) {
        return keyValues.next().getValue();
    }

    @Override
    public void addResultToMemory(final Memory.Admin memory, final Iterator<KeyValue<NullObject, MapMemory>> keyValues) {
        final MapMemory temp = keyValues.next().getValue();
        temp.asMap().forEach(memory::set);
        memory.setIteration(temp.getIteration());
        memory.setRuntime(temp.getRuntime());
    }

    @Override
    public int hashCode() {
        return (this.getClass().getCanonicalName() + Constants.SYSTEM_MEMORY).hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return GraphComputerHelper.areEqual(this, object);
    }

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this, this.memoryKeys.toString());
    }

}
