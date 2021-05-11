package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

public class FlatMapStep {
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public final NestedTraversal flatMapTraversal;
    
    /**
     * Constructs an immutable FlatMapStep object
     */
    public FlatMapStep(NestedTraversal flatMapTraversal) {
        this.flatMapTraversal = flatMapTraversal;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof FlatMapStep)) return false;
        FlatMapStep o = (FlatMapStep) other;
        return flatMapTraversal.equals(o.flatMapTraversal);
    }
    
    @Override
    public int hashCode() {
        return 2 * flatMapTraversal.hashCode();
    }
}
