package org.example.org.apache.tinkerpop.gremlin.language.model.traversal;

import org.example.org.apache.tinkerpop.gremlin.language.model.methods.TraversalSelfMethod;

public class ChainedParentOfGraphTraversal {
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/methods.TraversalSelfMethod
     */
    public final TraversalSelfMethod first;
    
    /**
     * @type optional: org/apache/tinkerpop/gremlin/language/model/traversal.ChainedParentOfGraphTraversal
     */
    public final java.util.Optional<ChainedParentOfGraphTraversal> rest;
    
    /**
     * Constructs an immutable ChainedParentOfGraphTraversal object
     */
    public ChainedParentOfGraphTraversal(TraversalSelfMethod first, java.util.Optional<ChainedParentOfGraphTraversal> rest) {
        this.first = first;
        this.rest = rest;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ChainedParentOfGraphTraversal)) return false;
        ChainedParentOfGraphTraversal o = (ChainedParentOfGraphTraversal) other;
        return first.equals(o.first)
            && rest.equals(o.rest);
    }
    
    @Override
    public int hashCode() {
        return 2 * first.hashCode()
            + 3 * rest.hashCode();
    }
    
    /**
     * Construct a new immutable ChainedParentOfGraphTraversal object in which first is overridden
     */
    public ChainedParentOfGraphTraversal withFirst(TraversalSelfMethod first) {
        return new ChainedParentOfGraphTraversal(first, rest);
    }
    
    /**
     * Construct a new immutable ChainedParentOfGraphTraversal object in which rest is overridden
     */
    public ChainedParentOfGraphTraversal withRest(java.util.Optional<ChainedParentOfGraphTraversal> rest) {
        return new ChainedParentOfGraphTraversal(first, rest);
    }
}
