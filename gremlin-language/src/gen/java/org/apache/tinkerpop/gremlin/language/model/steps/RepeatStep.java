package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

public class RepeatStep {
    /**
     * @type optional: string
     */
    public final java.util.Optional<String> loopName;
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public final NestedTraversal repeatTraversals;
    
    /**
     * Constructs an immutable RepeatStep object
     */
    public RepeatStep(java.util.Optional<String> loopName, NestedTraversal repeatTraversals) {
        this.loopName = loopName;
        this.repeatTraversals = repeatTraversals;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RepeatStep)) return false;
        RepeatStep o = (RepeatStep) other;
        return loopName.equals(o.loopName)
            && repeatTraversals.equals(o.repeatTraversals);
    }
    
    @Override
    public int hashCode() {
        return 2 * loopName.hashCode()
            + 3 * repeatTraversals.hashCode();
    }
    
    /**
     * Construct a new immutable RepeatStep object in which loopName is overridden
     */
    public RepeatStep withLoopName(java.util.Optional<String> loopName) {
        return new RepeatStep(loopName, repeatTraversals);
    }
    
    /**
     * Construct a new immutable RepeatStep object in which repeatTraversals is overridden
     */
    public RepeatStep withRepeatTraversals(NestedTraversal repeatTraversals) {
        return new RepeatStep(loopName, repeatTraversals);
    }
}
