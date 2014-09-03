package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerElement;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerMetaProperty;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerProperty;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;

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
    private boolean inUse = true;

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
        //  TODO: is this if statement needed?
        if (this.isolation.equals(GraphComputer.Isolation.BSP)) {
            this.getMap = this.setMap;
            this.setMap = new HashMap<>();
        }
    }

    public <V, P extends Property<V>> P setProperty(final TinkerElement element, final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        if (isComputeKey(key)) {
            if (element instanceof Vertex) {
                final TinkerMetaProperty<V> property = new TinkerMetaProperty<V>((TinkerVertex) element, key, value) {
                    @Override
                    public void remove() {
                        removeProperty((TinkerElement) element, key);
                    }
                };
                this.setValue(element.id(), key, property);
                return (P) property;
            } else {
                final TinkerProperty<V> property = new TinkerProperty<V>(element, key, value) {
                    @Override
                    public void remove() {
                        removeProperty((TinkerElement) element, key);
                    }
                };
                this.setValue(element.id(), key, property);
                return (P) property;
            }
        } else {
            throw GraphComputer.Exceptions.providedKeyIsNotAComputeKey(key);
        }
    }


    public <V, P extends Property<V>> P getProperty(final TinkerElement element, final String key) {
        if (isComputeKey(key)) {
            return (P) this.getValue(element.id(), key);
        } else {
            // return (P) TinkerHelper.getProperties(element).getOrDefault(key, Property.empty());
            return null;
        }
    }


    public void removeProperty(final TinkerElement element, final String key) {
        if (isComputeKey(key)) {
            this.removeValue(element.id(), key);
        } else {
            throw GraphComputer.Exceptions.providedKeyIsNotAComputeKey(key);
        }
    }

    public void setInUse(final boolean inUse) {
        this.inUse = inUse;
    }

    public boolean getInUse() {
        return this.inUse;
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

    private <V, P extends Property<V>> P getValue(final Object id, final String key) {
        final Map<String, Object> map = this.isConstantKey(key) ? this.constantMap.get(id) : this.getMap.get(id);
        if (null == map)
            return (P) Property.empty();
        else {
            final P property = (P) map.get(key);
            return null == property ? (P) Property.empty() : property;
        }
    }

    public boolean isComputeKey(final String key) {
        return this.computeKeys.containsKey(key);
    }

    public boolean isConstantKey(final String key) {
        return VertexProgram.KeyType.CONSTANT.equals(this.computeKeys.get(key));
    }

    public Map<String, VertexProgram.KeyType> getComputeKeys() {
        return this.computeKeys;
    }

    /*public boolean isVariableKey(final String key) {
        return VertexProgram.KeyType.VARIABLE.equals(this.computeKeys.get(key));
    }*/

}