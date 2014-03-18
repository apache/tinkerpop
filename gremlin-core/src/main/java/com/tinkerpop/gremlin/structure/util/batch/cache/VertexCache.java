package com.tinkerpop.gremlin.structure.util.batch.cache;

import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Matthias Broecheler (http://www.matthiasb.com)
 */
public interface VertexCache {
    public Object getEntry(final Object externalId);

    public void set(final Vertex vertex, final Object externalId);

    public void setId(final Object vertexId, final Object externalId);

    public boolean contains(final Object externalId);

    public void newTransaction();
}
