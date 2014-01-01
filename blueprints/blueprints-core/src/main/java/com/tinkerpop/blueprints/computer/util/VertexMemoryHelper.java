package com.tinkerpop.blueprints.computer.util;

import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.computer.VertexSystemMemory;
import com.tinkerpop.blueprints.util.ElementHelper;

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
