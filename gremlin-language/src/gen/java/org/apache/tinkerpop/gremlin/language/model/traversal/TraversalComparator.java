package org.example.org.apache.tinkerpop.gremlin.language.model.traversal;

/**
 * @type org/apache/tinkerpop/gremlin/language/model/traversal.TraversalOrder
 */
public class TraversalComparator {
    public final TraversalOrder traversalOrder;
    
    /**
     * Constructs an immutable TraversalComparator object
     */
    public TraversalComparator(TraversalOrder traversalOrder) {
        this.traversalOrder = traversalOrder;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof TraversalComparator)) {
            return false;
        }
        TraversalComparator o = (TraversalComparator) other;
        return traversalOrder.equals(o.traversalOrder);
    }
    
    @Override
    public int hashCode() {
        return 2 * traversalOrder.hashCode();
    }
}
