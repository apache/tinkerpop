package com.tinkerpop.gremlin.giraph.process.computer.util;

import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MemoryMapReduce implements MapReduce<String, Object, String, Object, Map<String, Object>> {

    public Set<String> memoryKeys = new HashSet<>();

    @Override
    public String getMemoryKey() {
        return Constants.SYSTEM_MEMORY;
    }

    public MemoryMapReduce() {

    }

    public MemoryMapReduce(final Set<String> memoryKeys) {
        this.memoryKeys = memoryKeys;
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(Constants.GREMLIN_GIRAPH_MEMORY_KEYS, new ArrayList<>(this.memoryKeys.size()));
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.memoryKeys = new HashSet((List) configuration.getList(Constants.GREMLIN_GIRAPH_MEMORY_KEYS));
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<String, Object> emitter) {
        final Map<String, Object> memoryMap = vertex.<Map<String, Object>>property(Constants.MEMORY_MAP).orElse(Collections.emptyMap());
        for (final String memoryKey : this.memoryKeys) {
            if (memoryMap.containsKey(memoryKey)) {
                emitter.emit(memoryKey, memoryMap.get(memoryKey));
            }
        }
    }

    @Override
    public void combine(final String key, final Iterator<Object> values, final ReduceEmitter<String, Object> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public void reduce(final String key, final Iterator<Object> values, final ReduceEmitter<String, Object> emitter) {
        emitter.emit(key, values.next());
    }

    @Override
    public Map<String, Object> generateFinalResult(final Iterator<Pair<String, Object>> keyValues) {
        final Map<String, Object> map = new HashMap<>();
        while (keyValues.hasNext()) {
            final Pair<String, Object> pair = keyValues.next();
            map.put(pair.getValue0(), pair.getValue1());
        }
        return map;
    }

    @Override
    public void addResultToMemory(final Memory memory, final Iterator<Pair<String, Object>> keyValues) {
        while (keyValues.hasNext()) {
            final Pair<String, Object> keyValue = keyValues.next();
            memory.set(keyValue.getValue0(), keyValue.getValue1());
        }
    }

    @Override
    public int hashCode() {
        return (this.getClass().getCanonicalName() + Constants.SYSTEM_MEMORY).hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return GraphComputerHelper.areEqual(this, object);
    }

}
