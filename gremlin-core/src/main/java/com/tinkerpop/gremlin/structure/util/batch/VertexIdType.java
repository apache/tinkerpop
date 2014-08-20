package com.tinkerpop.gremlin.structure.util.batch;

import com.tinkerpop.gremlin.structure.util.batch.cache.LongIDVertexCache;
import com.tinkerpop.gremlin.structure.util.batch.cache.ObjectIDVertexCache;
import com.tinkerpop.gremlin.structure.util.batch.cache.StringIDVertexCache;
import com.tinkerpop.gremlin.structure.util.batch.cache.URLCompression;
import com.tinkerpop.gremlin.structure.util.batch.cache.VertexCache;

/**
 * Type of vertex ids expected by BatchGraph. The default is IdType.OBJECT.
 * Use the IdType that best matches the used vertex id types in order to save sideEffects.
 *
 * @author Matthias Broecheler (http://www.matthiasb.com)
 */
public enum VertexIdType {

    OBJECT {
        @Override
        public VertexCache getVertexCache() {
            return new ObjectIDVertexCache();
        }
    },

    NUMBER {
        @Override
        public VertexCache getVertexCache() {
            return new LongIDVertexCache();
        }
    },

    STRING {
        @Override
        public VertexCache getVertexCache() {
            return new StringIDVertexCache();
        }
    },

    URL {
        @Override
        public VertexCache getVertexCache() {
            return new StringIDVertexCache(new URLCompression());

        }
    };

    public abstract VertexCache getVertexCache();
}
