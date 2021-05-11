package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class GroupCountStep {
    /**
     * @type optional: string
     */
    public final java.util.Optional<String> sideEffectKey;
    
    /**
     * Constructs an immutable GroupCountStep object
     */
    public GroupCountStep(java.util.Optional<String> sideEffectKey) {
        this.sideEffectKey = sideEffectKey;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof GroupCountStep)) return false;
        GroupCountStep o = (GroupCountStep) other;
        return sideEffectKey.equals(o.sideEffectKey);
    }
    
    @Override
    public int hashCode() {
        return 2 * sideEffectKey.hashCode();
    }
}
