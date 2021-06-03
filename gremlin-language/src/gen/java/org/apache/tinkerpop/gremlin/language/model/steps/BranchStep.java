package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

public class BranchStep {
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public final NestedTraversal branchTraversal;
    
    /**
     * Constructs an immutable BranchStep object
     */
    public BranchStep(NestedTraversal branchTraversal) {
        this.branchTraversal = branchTraversal;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof BranchStep)) {
            return false;
        }
        BranchStep o = (BranchStep) other;
        return branchTraversal.equals(o.branchTraversal);
    }
    
    @Override
    public int hashCode() {
        return 2 * branchTraversal.hashCode();
    }
}
