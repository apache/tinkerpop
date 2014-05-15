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
public class StringIDVertexCache implements VertexCache {

    private static final int INITIAL_CAPACITY = 1000;
    private static final int INITIAL_TX_CAPACITY = 100;

    private final Map<String, Object> map;
    private final Set<String> mapKeysInCurrentTx;
    private final StringCompression compression;

    public StringIDVertexCache(final StringCompression compression) {
        if (compression == null) throw new IllegalArgumentException("Compression expected.");
        this.compression = compression;
        map = new HashMap<>(INITIAL_CAPACITY);
        mapKeysInCurrentTx = new HashSet<>(INITIAL_TX_CAPACITY);
    }

    public StringIDVertexCache() {
        this(StringCompression.NO_COMPRESSION);
    }

    @Override
    public Object getEntry(final Object externalId) {
        final String id = compression.compress(externalId.toString());
        return map.get(id);
    }

    @Override
    public void set(final Vertex vertex, final Object externalId) {
        setId(vertex,externalId);
    }

    @Override
    public void setId(final Object vertexId, final Object externalId) {
        final String id = compression.compress(externalId.toString());
        map.put(id, vertexId);
        mapKeysInCurrentTx.add(id);
    }

    @Override
    public boolean contains(final Object externalId) {
        return map.containsKey(compression.compress(externalId.toString()));
    }

    @Override
    public void newTransaction() {
        for (String id : mapKeysInCurrentTx) {
            final Object o = map.get(id);
            assert null != o;
            if (o instanceof Vertex) {
                Vertex v = (Vertex)o;
                map.put(id, v.id());
            }
        }
        mapKeysInCurrentTx.clear();
    }
}
