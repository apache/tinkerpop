package com.tinkerpop.gremlin.process.olap.util;

import com.tinkerpop.gremlin.process.olap.GraphComputer;
import com.tinkerpop.gremlin.process.olap.VertexSystemMemory;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexMemoryHelper {

    public static void validateComputeKeyValue(final VertexSystemMemory vertexMemory, final String key, final Object value) throws IllegalArgumentException {
        if (!vertexMemory.isComputeKey(key))
            throw GraphComputer.Exceptions.providedKeyIsNotAComputeKey(key);
        ElementHelper.validateProperty(key, value);
    }
}
