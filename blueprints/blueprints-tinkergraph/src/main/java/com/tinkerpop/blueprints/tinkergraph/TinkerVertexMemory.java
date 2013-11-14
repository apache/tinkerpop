package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.computer.VertexProgram;
import com.tinkerpop.blueprints.computer.VertexSystemMemory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerVertexMemory implements VertexSystemMemory {

    protected Map<String, VertexProgram.KeyType> computeKeys;
    protected final GraphComputer.Isolation isolation;
    protected boolean phase = true;
    private final Map<Object, Map<String, Property>> memory;

    public TinkerVertexMemory(final GraphComputer.Isolation isolation) {
        this.isolation = isolation;
        this.memory = new HashMap<>();
    }

    public void setComputeKeys(final Map<String, VertexProgram.KeyType> computeKeys) {
        this.computeKeys = computeKeys;
    }

    public Map<String, VertexProgram.KeyType> getComputeKeys() {
        return this.computeKeys;
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

        if (isolation.equals(GraphComputer.Isolation.BSP))
            return key + !phase;
        else
            return key;

    }

    protected String generateSetKey(final String key) {
        if (this.computeKeys.get(key).equals(VertexProgram.KeyType.CONSTANT))
            return key;

        if (isolation.equals(GraphComputer.Isolation.BSP))
            return key + phase;
        else
            return key;
    }

    protected boolean isConstantKey(final String key) {
        return VertexProgram.KeyType.CONSTANT.equals(this.computeKeys.get(key));
    }


    public <T> Property<T, Vertex> setProperty(final Vertex vertex, final String key, final T value) {
        Map<String, Property> map = this.memory.get(vertex.getId());
        if (null == map) {
            map = new HashMap<>();
            this.memory.put(vertex.getId(), map);
        }
        final String bspKey = generateSetKey(key);
        if (isConstantKey(key) && map.containsKey(bspKey))
            throw new IllegalStateException("The constant property " + bspKey + " has already been set for vertex " + vertex);
        else
            return map.put(bspKey, new TinkerProperty<>(key, value, vertex));
    }

    public <T> Property<T, Vertex> getProperty(final Vertex vertex, final String key) {
        final Map<String, Property> map = this.memory.get(vertex.getId());
        if (null == map)
            return Property.empty();
        else {
            Property<T, Vertex> property = map.get(generateGetKey(key));
            return null == property ? Property.empty() : property;
        }
    }

    public <T> Property<T, Vertex> removeProperty(final Vertex vertex, final String key) {
        final Map<String, Property> map = this.memory.get(vertex.getId());
        if (null == map)
            return Property.empty();
        else {
            Property<T, Vertex> property = map.remove(generateGetKey(key));
            return null == property ? Property.empty() : property;
        }
    }
}
