package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalDirection;

public class ToVStep {
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.TraversalDirection
     */
    public final TraversalDirection direction;
    
    /**
     * Constructs an immutable ToVStep object
     */
    public ToVStep(TraversalDirection direction) {
        this.direction = direction;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ToVStep)) {
            return false;
        }
        ToVStep o = (ToVStep) other;
        return direction.equals(o.direction);
    }
    
    @Override
    public int hashCode() {
        return 2 * direction.hashCode();
    }
}
