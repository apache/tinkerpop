package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphView implements Serializable {

    protected final Map<String, VertexProgram.KeyType> computeKeys;
    protected final GraphComputer.Isolation isolation;
    private Map<Object, Map<String, Object>> getMap;
    private Map<Object, Map<String, Object>> setMap;
    private Map<Object, Map<String, Object>> constantMap;

    public TinkerGraphView(final GraphComputer.Isolation isolation, final Map<String, VertexProgram.KeyType> computeKeys) {
        this.isolation = isolation;
        this.constantMap = new HashMap<>();
        this.computeKeys = computeKeys;
        if (this.isolation.equals(GraphComputer.Isolation.BSP)) {
            this.getMap = new HashMap<>();
            this.setMap = new HashMap<>();
        } else {
            this.getMap = this.setMap = new HashMap<>();
        }
    }

    public void completeIteration() {
        //  todo: is this if statement needed?
        if (this.isolation.equals(GraphComputer.Isolation.BSP)) {
            this.getMap = this.setMap;
            this.setMap = new HashMap<>();
        }
    }

    public <V> Property<V> setProperty(final TinkerElement element, final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        if (isComputeKey(key)) {
            final TinkerProperty<V> property = new TinkerProperty<V>(element, key, value) {
                public void remove() {
                    removeProperty(element, key);
                }
            };
            this.setValue(element.getId(), key, property);
            return property;
        } else {
            throw GraphComputer.Exceptions.providedKeyIsNotAComputeKey(key);
            //element.properties.put(key, new TinkerProperty<>(element, key, value));
        }
    }


    public <V> Property<V> getProperty(final TinkerElement element, final String key) {
        if (isComputeKey(key)) {
            return this.getValue(element.getId(), key);
        } else {
            return element.properties.getOrDefault(key, Property.empty());
        }
    }


    public void removeProperty(final TinkerElement element, final String key) {
        if (isComputeKey(key)) {
            this.removeValue(element.getId(), key);
        } else {
            throw GraphComputer.Exceptions.providedKeyIsNotAComputeKey(key);
            //element.properties.remove(key);
        }
    }

    //////////////////////

    private void setValue(final Object id, final String key, final Object value) {
        final Map<Object, Map<String, Object>> map = isConstantKey(key) ? this.constantMap : this.setMap;
        final Map<String, Object> nextMap = map.getOrDefault(id, new HashMap<>());
        map.put(id, nextMap);
        if (isConstantKey(key) && nextMap.containsKey(key))
            throw GraphComputer.Exceptions.constantComputeKeyHasAlreadyBeenSet(key, id);
        nextMap.put(key, value);
    }

    private void removeValue(final Object id, final String key) {
        final Map<String, Object> map = this.setMap.get(id);
        if (null != map)
            map.remove(key);
    }

    private <V> Property<V> getValue(final Object id, final String key) {
        final Map<String, Object> map = this.isConstantKey(key) ? this.constantMap.get(id) : this.getMap.get(id);
        if (null == map)
            return Property.empty();
        else {
            final Property<V> property = (Property<V>) map.get(key);
            return null == property ? Property.empty() : property;
        }
    }

    public boolean isComputeKey(final String key) {
        return this.computeKeys.containsKey(key);
    }

    public boolean isConstantKey(final String key) {
        return VertexProgram.KeyType.CONSTANT.equals(this.computeKeys.get(key));
    }

    /*public boolean isVariableKey(final String key) {
        return VertexProgram.KeyType.VARIABLE.equals(this.computeKeys.get(key));
    }*/

}