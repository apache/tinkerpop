package com.tinkerpop.gremlin.structure.util.batch.cache;

import com.tinkerpop.gremlin.structure.Vertex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Matthias Broecheler (http://www.matthiasb.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
abstract class AbstractIDVertexCache implements VertexCache {

    static final int INITIAL_CAPACITY = 1000;
    static final int INITIAL_TX_CAPACITY = 100;

    private final Map<Object, Object> map;
    private final Set<Object> mapKeysInCurrentTx;

    AbstractIDVertexCache() {
        map = new HashMap<>(INITIAL_CAPACITY);
        mapKeysInCurrentTx = new HashSet<>(INITIAL_TX_CAPACITY);
    }

    @Override
    public Object getEntry(final Object externalId) {
        return map.get(externalId);
    }

    @Override
    public void set(final Vertex vertex, final Object externalId) {
        setId(vertex, externalId);
    }

    @Override
    public void setId(final Object vertexId, final Object externalId) {
        map.put(externalId, vertexId);
        mapKeysInCurrentTx.add(externalId);
    }

    @Override
    public boolean contains(final Object externalId) {
        return map.containsKey(externalId);
    }

    @Override
    public void newTransaction() {
        for (Object id : mapKeysInCurrentTx) {
            Object o = map.get(id);
            assert null != o;
            if (o instanceof Vertex) {
                Vertex v = (Vertex) o;
                map.put(id, v.id());
            }
        }
        mapKeysInCurrentTx.clear();
    }
}
