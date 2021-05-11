package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.GenericLiteral;

public class VStep {
    /**
     * @type list: org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
     */
    public final java.util.List<GenericLiteral> vertexIdsOrElements;
    
    /**
     * Constructs an immutable VStep object
     */
    public VStep(java.util.List<GenericLiteral> vertexIdsOrElements) {
        this.vertexIdsOrElements = vertexIdsOrElements;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof VStep)) return false;
        VStep o = (VStep) other;
        return vertexIdsOrElements.equals(o.vertexIdsOrElements);
    }
    
    @Override
    public int hashCode() {
        return 2 * vertexIdsOrElements.hashCode();
    }
}
