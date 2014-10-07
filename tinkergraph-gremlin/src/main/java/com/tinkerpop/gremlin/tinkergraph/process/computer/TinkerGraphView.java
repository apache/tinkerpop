package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerElement;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerVertexProperty;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerProperty;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphView {

    protected final Set<String> computeKeys;
    protected final GraphComputer.Isolation isolation;
    private Map<Element, Map<String, List<Property>>> computeProperties;

    public TinkerGraphView(final GraphComputer.Isolation isolation, final Set<String> computeKeys) {
        this.isolation = isolation;
        this.computeKeys = computeKeys;
        this.computeProperties = new HashMap<>();
    }

    public <V> Property<V> setProperty(final TinkerElement element, final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        if (isComputeKey(key)) {
            if (element instanceof Vertex) {
                final TinkerVertexProperty<V> property = new TinkerVertexProperty<V>((TinkerVertex) element, key, value) {
                    @Override
                    public void remove() {
                        removeProperty(element, key, this);
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
            throw GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey(key);
        }
    }


    public List<Property> getProperty(final TinkerElement element, final String key) {
        if (isComputeKey(key)) {
            return this.getValue(element, key);
        } else {
            return (List) TinkerHelper.getProperties(element).getOrDefault(key, Collections.emptyList());
        }
    }

    public List<Property> getProperties(final TinkerElement element) {
        final Stream<Property> a = TinkerHelper.getProperties(element).values().stream().flatMap(list -> list.stream());
        final Stream<Property> b = this.computeProperties.containsKey(element) ?
                this.computeProperties.get(element).values().stream().flatMap(list -> list.stream()) :
                Stream.empty();
        return Stream.concat(a, b).collect(Collectors.toList());
    }

    public void removeProperty(final TinkerElement element, final String key, final Property property) {
        if (isComputeKey(key)) {
            if (element instanceof Vertex)
                this.removeValue(element, key, property);
            else
                this.removeValue(element, key);
        } else {
            throw GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey(key);
        }
    }

    //////////////////////

    private void setValue(final Element element, final String key, final Property property) {
        final Map<String, List<Property>> nextMap = this.computeProperties.getOrDefault(element, new HashMap<>());
        this.computeProperties.put(element, nextMap);
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
        final Map<String, List<Property>> map = this.computeProperties.get(element);
        if (null != map)
            map.remove(key);
    }

    private void removeValue(final Element element, final String key, final Property property) {
        final Map<String, List<Property>> map = this.computeProperties.get(element);
        if (null != map)
            map.get(key).remove(property);
    }

    private List<Property> getValue(final Element element, final String key) {
        final Map<String, List<Property>> map = this.computeProperties.get(element);
        return (null == map) ? Collections.emptyList() : map.getOrDefault(key, Collections.emptyList());
    }

    public boolean isComputeKey(final String key) {
        return this.computeKeys.contains(key);
    }
}