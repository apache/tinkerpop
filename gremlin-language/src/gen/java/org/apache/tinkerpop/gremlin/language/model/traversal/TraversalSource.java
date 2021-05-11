package org.example.org.apache.tinkerpop.gremlin.language.model.traversal;

/**
 * @type list: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalSourceSelfMethod
 */
public class TraversalSource {
    public final java.util.List<TraversalSourceSelfMethod> value;
    
    /**
     * Constructs an immutable TraversalSource object
     */
    public TraversalSource(java.util.List<TraversalSourceSelfMethod> value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof TraversalSource)) return false;
        TraversalSource o = (TraversalSource) other;
        return value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * value.hashCode();
    }
}
