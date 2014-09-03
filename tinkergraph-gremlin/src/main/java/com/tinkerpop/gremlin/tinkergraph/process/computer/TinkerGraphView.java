package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerElement;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerMetaProperty;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerProperty;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphView implements Serializable {

    protected final Map<String, VertexProgram.KeyType> computeKeys;
    protected final GraphComputer.Isolation isolation;
    private Map<Element, Map<String, List<Property>>> getMap;
    private Map<Element, Map<String, List<Property>>> setMap;
    private Map<Element, Map<String, List<Property>>> constantMap;
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

    public <V> Property<V> setProperty(final TinkerElement element, final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        if (isComputeKey(key)) {
            if (element instanceof Vertex) {
                final TinkerMetaProperty<V> property = new TinkerMetaProperty<V>((TinkerVertex) element, key, value) {
                    @Override
                    public void remove() {
                        removeProperty((TinkerVertex) element, key, this);
                    }
                };
                this.setValue(element, key, property);
                return property;
            } else {
                final TinkerProperty<V> property = new TinkerProperty<V>(element, key, value) {
                    @Override
                    public void remove() {
                        removeProperty((TinkerElement) element, key, this);
                    }
                };
                this.setValue(element, key, property);
                return property;
            }
        } else {
            throw GraphComputer.Exceptions.providedKeyIsNotAComputeKey(key);
        }
    }


    public List<Property> getProperty(final TinkerElement element, final String key) {
        if (isComputeKey(key)) {
            return this.getValue(element, key);
        } else {
            return (List) TinkerHelper.getProperties(element).getOrDefault(key, Collections.emptyList());
        }
    }


    public void removeProperty(final TinkerElement element, final String key, final Property property) {
        if (isComputeKey(key)) {
            if (element instanceof Vertex)
                this.removeValue(element, key, property);
            else
                this.removeValue(element, key);
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

    private void setValue(final Element element, final String key, final Property property) {
        final Map<Element, Map<String, List<Property>>> map = isConstantKey(key) ? this.constantMap : this.setMap;
        final Map<String, List<Property>> nextMap = map.getOrDefault(element, new HashMap<>());
        map.put(element, nextMap);
        if (isConstantKey(key) && nextMap.containsKey(key))
            throw GraphComputer.Exceptions.constantComputeKeyHasAlreadyBeenSet(key, element);
        if (nextMap.containsKey(key)) {
            if (element instanceof Vertex) {
                nextMap.get(key).add(property);
            } else {
                nextMap.get(key).clear();
                nextMap.get(key).add(property);
            }
        } else {
            final List<Property> list = new ArrayList<>();
            list.add(property);
            nextMap.put(key, list);
        }
    }

    private void removeValue(final Element element, final String key) {
        final Map<String, List<Property>> map = this.setMap.get(element);
        if (null != map)
            map.remove(key);
    }

    private void removeValue(final Element element, final String key, final Property property) {
        final Map<String, List<Property>> map = this.setMap.get(element);
        if (null != map)
            map.get(key).remove(property);
    }

    private List<Property> getValue(final Element element, final String key) {
        final Map<String, List<Property>> map = this.isConstantKey(key) ? this.constantMap.get(element) : this.getMap.get(element);
        return (null == map) ? Collections.emptyList() : map.getOrDefault(key, Collections.emptyList());
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