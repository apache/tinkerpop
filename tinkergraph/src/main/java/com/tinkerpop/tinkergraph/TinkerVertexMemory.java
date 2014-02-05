package com.tinkerpop.tinkergraph;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.computer.VertexProgram;
import com.tinkerpop.blueprints.computer.VertexSystemMemory;
import com.tinkerpop.blueprints.computer.util.VertexMemoryHelper;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerVertexMemory implements VertexSystemMemory {

    protected Map<String, VertexProgram.KeyType> computeKeys;
    protected final GraphComputer.Isolation isolation;
    private Map<Object, Map<String, Object>> getMap;
    private Map<Object, Map<String, Object>> setMap;
    private Map<Object, Map<String, Object>> constantMap;

    public TinkerVertexMemory(final GraphComputer.Isolation isolation) {
        this.isolation = isolation;
        this.constantMap = new HashMap<>();
        if (this.isolation.equals(GraphComputer.Isolation.BSP)) {
            this.getMap = new HashMap<>();
            this.setMap = new HashMap<>();
        } else {
            this.getMap = this.setMap = new HashMap<>();
        }
    }

    public void setComputeKeys(final Map<String, VertexProgram.KeyType> computeKeys) {
        this.computeKeys = computeKeys;
    }

    public Map<String, VertexProgram.KeyType> getComputeKeys() {
        return this.computeKeys;
    }

    public void completeIteration() {
        this.getMap = this.setMap;
        this.setMap = new HashMap<>();
    }

    public <V> void setProperty(final Element element, final String key, final V value) {
        VertexMemoryHelper.validateComputeKeyValue(this, key, value);
        final TinkerProperty<V> property = new TinkerProperty<V>(element, key, value) {
            public void remove() {
                removeProperty(element, key);
            }
        };
        property.state = ((TinkerElement) element).state;
        this.setValue(element.getId().toString(), key, property);
    }


    public <V> Property<V> getProperty(final Element element, final String key) {
        return this.getProperty(element.getId().toString(), key);
    }


    private void removeProperty(final Element element, final String key) {
        this.removeValue(element.getId().toString(), key);
    }

    private void setValue(final String id, final String key, final Object value) {
        final Map<Object, Map<String, Object>> map = isConstantKey(key) ? this.constantMap : this.setMap;
        final Map<String, Object> nextMap = map.getOrDefault(id, new HashMap<>());
        map.put(id, nextMap);
        if (isConstantKey(key) && nextMap.containsKey(key))
            throw GraphComputer.Exceptions.constantComputeKeyHasAlreadyBeenSet(key, id);
        nextMap.put(key, value);
    }

    private void removeValue(final String id, final String key) {
        final Map<String, Object> map = this.setMap.get(id);
        if (null != map)
            map.remove(key);
    }

    private <V> Property<V> getProperty(final String id, final String key) {
        final Map<String, Object> map = this.isConstantKey(key) ? this.constantMap.get(id) : this.getMap.get(id);
        if (null == map)
            return Property.empty();
        else {
            final Property<V> property = (Property<V>) map.get(key);
            return null == property ? Property.empty() : property;
        }
    }

}
