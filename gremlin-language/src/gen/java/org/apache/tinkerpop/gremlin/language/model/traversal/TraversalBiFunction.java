package org.example.org.apache.tinkerpop.gremlin.language.model.traversal;

/**
 * @type org/apache/tinkerpop/gremlin/language/model/traversal.TraversalOperator
 */
public class TraversalBiFunction {
    public final TraversalOperator traversalOperator;
    
    /**
     * Constructs an immutable TraversalBiFunction object
     */
    public TraversalBiFunction(TraversalOperator traversalOperator) {
        this.traversalOperator = traversalOperator;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof TraversalBiFunction)) return false;
        TraversalBiFunction o = (TraversalBiFunction) other;
        return traversalOperator.equals(o.traversalOperator);
    }
    
    @Override
    public int hashCode() {
        return 2 * traversalOperator.hashCode();
    }
}
