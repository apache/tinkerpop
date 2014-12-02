package com.tinkerpop.gremlin.hadoop.process.computer.util;

import com.tinkerpop.gremlin.hadoop.Constants;
import com.tinkerpop.gremlin.process.computer.KeyValue;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.process.computer.util.MapMemory;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MemoryMapReduce implements MapReduce<MapReduce.NullObject, MapMemory, MapReduce.NullObject, MapMemory, MapMemory> {

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
