package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.computer.VertexProgram;
import com.tinkerpop.blueprints.computer.VertexSystemMemory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerVertexMemory implements VertexSystemMemory {

    protected Map<String, VertexProgram.KeyType> computeKeys;
    protected final GraphComputer.Isolation isolation;
    protected boolean phase = true;
    private final Map<Object, Map<String, Vertex.Property>> memory;

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


    public <V> Vertex.Property<V> setProperty(final Vertex vertex, final String key, final V value) {
        final Map<String, Vertex.Property> map = this.memory.getOrDefault(vertex.getId(), new HashMap<>());
        this.memory.put(vertex.getId(), map);

        final String bspKey = generateSetKey(key);
        if (isConstantKey(key) && map.containsKey(bspKey))
            throw new IllegalStateException("The constant property " + bspKey + " has already been set for vertex " + vertex);
        else
            return map.put(bspKey, new Vertex.Property<V>() {
                private Map<String, com.tinkerpop.blueprints.Property> properties = new HashMap<>();

                public boolean isPresent() {
                    return null != value;
                }

                public Vertex getVertex() {
                    return vertex;
                }

                public String getKey() {
                    return key;
                }

                public V getValue() {
                    return value;
                }

                public void remove() {
                    map.remove(key);
                }

                public Set<String> getPropertyKeys() {
                    return this.properties.keySet();
                }

                public Map<String, com.tinkerpop.blueprints.Property> getProperties() {
                    return new HashMap<>(this.properties);
                }

                public <V2> com.tinkerpop.blueprints.Property<V2> getProperty(final String key) {
                    return this.properties.get(key);
                }

                public <V2> com.tinkerpop.blueprints.Property<V2> setProperty(final String key, final V2 value) {
                    final com.tinkerpop.blueprints.Property<V2> property = new com.tinkerpop.blueprints.Property<V2>() {
                        public boolean isPresent() {
                            return null != value;
                        }

                        public void remove() {
                            properties.remove(key);
                        }

                        public V2 getValue() {
                            if (!isPresent())
                                throw Vertex.Property.Features.propertyDoesNotExist();
                            return value;
                        }

                        public String getKey() {
                            if (!isPresent())
                                throw Vertex.Property.Features.propertyDoesNotExist();
                            return key;
                        }


                    };
                    this.properties.put(key, property);
                    return property;
                }
            });
    }

    public <V> Vertex.Property<V> getProperty(final Vertex vertex, final String key) {
        final Map<String, Vertex.Property> map = this.memory.get(vertex.getId());
        if (null == map)
            return Vertex.Property.empty();
        else {
            Vertex.Property<V> property = map.get(generateGetKey(key));
            return null == property ? Vertex.Property.empty() : property;
        }
    }
}
