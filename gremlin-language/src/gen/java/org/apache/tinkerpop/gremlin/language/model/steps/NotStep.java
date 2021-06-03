package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

public class NotStep {
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public final NestedTraversal notTraversal;
    
    /**
     * Constructs an immutable NotStep object
     */
    public NotStep(NestedTraversal notTraversal) {
        this.notTraversal = notTraversal;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof NotStep)) {
            return false;
        }
        NotStep o = (NotStep) other;
        return notTraversal.equals(o.notTraversal);
    }
    
    @Override
    public int hashCode() {
        return 2 * notTraversal.hashCode();
    }
}
