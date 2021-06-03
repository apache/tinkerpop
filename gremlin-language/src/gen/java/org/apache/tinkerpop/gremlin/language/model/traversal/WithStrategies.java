package org.example.org.apache.tinkerpop.gremlin.language.model.traversal;

public class WithStrategies {
    /**
     * @type list: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalStrategy
     */
    public final java.util.List<TraversalStrategy> traversalStrategies;
    
    /**
     * Constructs an immutable WithStrategies object
     */
    public WithStrategies(java.util.List<TraversalStrategy> traversalStrategies) {
        this.traversalStrategies = traversalStrategies;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof WithStrategies)) {
            return false;
        }
        WithStrategies o = (WithStrategies) other;
        return traversalStrategies.equals(o.traversalStrategies);
    }
    
    @Override
    public int hashCode() {
        return 2 * traversalStrategies.hashCode();
    }
}
