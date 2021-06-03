package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalDirection;

public class ToEStep {
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.TraversalDirection
     */
    public final TraversalDirection direction;
    
    /**
     * @type list: string
     */
    public final java.util.List<String> edgeLabels;
    
    /**
     * Constructs an immutable ToEStep object
     */
    public ToEStep(TraversalDirection direction, java.util.List<String> edgeLabels) {
        this.direction = direction;
        this.edgeLabels = edgeLabels;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ToEStep)) {
            return false;
        }
        ToEStep o = (ToEStep) other;
        return direction.equals(o.direction)
            && edgeLabels.equals(o.edgeLabels);
    }
    
    @Override
    public int hashCode() {
        return 2 * direction.hashCode()
            + 3 * edgeLabels.hashCode();
    }
    
    /**
     * Construct a new immutable ToEStep object in which direction is overridden
     */
    public ToEStep withDirection(TraversalDirection direction) {
        return new ToEStep(direction, edgeLabels);
    }
    
    /**
     * Construct a new immutable ToEStep object in which edgeLabels is overridden
     */
    public ToEStep withEdgeLabels(java.util.List<String> edgeLabels) {
        return new ToEStep(direction, edgeLabels);
    }
}
