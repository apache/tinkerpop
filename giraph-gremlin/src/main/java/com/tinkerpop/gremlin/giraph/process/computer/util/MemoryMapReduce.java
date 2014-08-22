package com.tinkerpop.gremlin.giraph.process.computer.util;

import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MemoryMapReduce implements MapReduce<String, Object, String, Object, Map<String, Object>> {

    public Set<String> memoryKeys = new HashSet<>();

    public String getSideEffectKey() {
        return Constants.HIDDEN_MEMORY;
    }

    public MemoryMapReduce() {

    }

    public MemoryMapReduce(final Set<String> memoryKeys) {
        this.memoryKeys = memoryKeys;
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(Constants.GREMLIN_MEMORY_KEYS, new ArrayList<>(this.memoryKeys.size()));
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.memoryKeys = new HashSet((List) configuration.getList(Constants.GREMLIN_MEMORY_KEYS));
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<String, Object> emitter) {
        for (final String sideEffectKey : this.memoryKeys) {
            final Property property = vertex.property(Graph.Key.hide(sideEffectKey));
            if (property.isPresent()) {
                emitter.emit(sideEffectKey, property.value());
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
    public Map<String, Object> generateSideEffect(final Iterator<Pair<String, Object>> keyValues) {
        final Map<String, Object> map = new HashMap<>();
        while (keyValues.hasNext()) {
            final Pair<String, Object> pair = keyValues.next();
            map.put(pair.getValue0(), pair.getValue1());
        }
        return map;
    }

    @Override
    public void addSideEffectToMemory(final Memory memory, final Iterator<Pair<String, Object>> keyValues) {
        while (keyValues.hasNext()) {
            final Pair<String, Object> keyValue = keyValues.next();
            memory.set(keyValue.getValue0(), keyValue.getValue1());
        }
    }

}
