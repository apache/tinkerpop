package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.computer.AnnotationSystemMemory;
import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.computer.VertexProgram;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerAnnotationMemory implements AnnotationSystemMemory {

    protected Map<String, VertexProgram.KeyType> computeKeys;
    protected final GraphComputer.Isolation isolation;
    protected boolean phase = true;
    private final Map<Object, Map<String, Object>> memory;

    public TinkerAnnotationMemory(final GraphComputer.Isolation isolation) {
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
            throw GraphComputer.Exceptions.providedKeyIsNotAComputeKey(key);

        if (keyType.equals(VertexProgram.KeyType.CONSTANT))
            return key;

        if (this.isolation.equals(GraphComputer.Isolation.BSP))
            return key + !this.phase;
        else
            return key;

    }

    protected String generateSetKey(final String key) {
        if (this.computeKeys.get(key).equals(VertexProgram.KeyType.CONSTANT))
            return key;

        if (this.isolation.equals(GraphComputer.Isolation.BSP))
            return key + this.phase;
        else
            return key;
    }

    protected boolean isConstantKey(final String key) {
        return VertexProgram.KeyType.CONSTANT.equals(this.computeKeys.get(key));
    }

    public <V> void setElementAnnotation(final Element element, final String key, final V value) {
        this.setAnnotation(element.getId().toString(), key, value);
    }

    public <V> void setPropertyAnnotation(final Element element, final String propertyKey, final String key, final V value) {
        this.setAnnotation(element.getId() + ":" + propertyKey, key, value);
    }

    public <V> Optional<V> getElementAnnotation(final Element element, final String key) {
        return this.getAnnotation(element.getId().toString(), key);
    }

    public <V> Optional<V> getPropertyAnnotation(final Element element, final String propertyKey, final String key) {
        return this.getAnnotation(element.getId() + ":" + propertyKey, key);
    }

    private <V> void setAnnotation(final String id, final String key, final V value) {
        final Map<String, Object> map = this.memory.getOrDefault(id, new HashMap<>());
        this.memory.put(id, map);

        final String bspKey = generateSetKey(key);
        if (isConstantKey(key) && map.containsKey(bspKey))
            throw GraphComputer.Exceptions.constantAnnotationHasAlreadyBeenSet(key, id);
        else
            map.put(bspKey, value);
    }

    private <V> Optional<V> getAnnotation(final String id, final String key) {
        final Map<String, Object> map = this.memory.get(id);
        return null == map ? Optional.empty() : Optional.ofNullable((V) map.get(generateGetKey(key)));
    }
}
