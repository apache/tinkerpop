package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.Isolation;
import com.tinkerpop.blueprints.computer.VertexProgram;
import com.tinkerpop.blueprints.computer.VertexSystemMemory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerVertexMemory implements VertexSystemMemory {

    protected Map<String, VertexProgram.KeyType> computeKeys;
    protected final Isolation isolation;
    protected boolean phase = true;
    private final Map<Object, Map<String, Property>> memory;

    public TinkerVertexMemory(final Isolation isolation) {
        this.isolation = isolation;
        this.memory = new ConcurrentHashMap<>();
    }

    public void setComputeKeys(final Map<String, VertexProgram.KeyType> computeKeys) {
        this.computeKeys = computeKeys;
    }

    public boolean isComputeKey(final String key) {
        return this.computeKeys.containsKey(key);
    }

    public void completeIteration() {
        this.phase = !this.phase;
    }

    protected String generateGetKey(final String key) {
        final VertexProgram.KeyType keyType = this.computeKeys.get(key);
        if (null == keyType)
            throw new IllegalArgumentException("The provided key is not a compute key: " + key);

        if (keyType.equals(VertexProgram.KeyType.CONSTANT))
            return key;

        if (isolation.equals(Isolation.BSP))
            return key + !phase;
        else
            return key;

    }

    protected String generateSetKey(final String key) {
        if (this.computeKeys.get(key).equals(VertexProgram.KeyType.CONSTANT))
            return key;

        if (isolation.equals(Isolation.BSP))
            return key + phase;
        else
            return key;
    }

    protected boolean isConstantKey(final String key) {
        return VertexProgram.KeyType.CONSTANT.equals(this.computeKeys.get(key));
    }


    public <T> Property<T> setProperty(final Vertex vertex, final String key, final T value) {
        Map<String, Property> map = this.memory.get(vertex.getId());
        if (null == map) {
            map = new HashMap<>();
            this.memory.put(vertex.getId(), map);
        }
        final String bspKey = generateSetKey(key);
        if (isConstantKey(key) && map.containsKey(bspKey))
            throw new IllegalStateException("The constant property " + bspKey + " has already been set for vertex " + vertex);
        else
            return map.put(bspKey, new SimpleProperty<>(key, value));
    }

    public <T> Property<T> getProperty(final Vertex vertex, final String key) {
        final Map<String, Property> map = this.memory.get(vertex.getId());
        if (null == map)
            return null;
        else
            return (Property<T>) map.get(generateGetKey(key));
    }

    public <T> Property<T> removeProperty(final Vertex vertex, final String key) {
        final Map<String, Property> map = this.memory.get(vertex.getId());
        if (null == map)
            return null;
        else {
            return (Property<T>) map.remove(generateGetKey(key));
        }
    }

    public static class SimpleProperty<T> implements Property<T> {
        private final String key;
        private final T value;
        private Map<String, Object> metas = new HashMap<>();

        protected SimpleProperty(String key, T value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return this.key;
        }

        public T getValue() {
            return this.value;
        }

        public boolean isPresent() {
            return this.value != null;
        }

        public <T> void setMetaValue(final String key, final T value) {
            this.metas.put(key, value);
        }

        public <T> T getMetaValue(final String key) {
            return (T) this.metas.get(key);
        }

        public <T> T removeMetaValue(final String key) {
            return (T) this.metas.remove(key);
        }
    }
}
