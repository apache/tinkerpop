package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class GroupStep {
    /**
     * @type optional: string
     */
    public final java.util.Optional<String> sideEffectKey;
    
    /**
     * Constructs an immutable GroupStep object
     */
    public GroupStep(java.util.Optional<String> sideEffectKey) {
        this.sideEffectKey = sideEffectKey;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof GroupStep)) {
            return false;
        }
        GroupStep o = (GroupStep) other;
        return sideEffectKey.equals(o.sideEffectKey);
    }
    
    @Override
    public int hashCode() {
        return 2 * sideEffectKey.hashCode();
    }
}
