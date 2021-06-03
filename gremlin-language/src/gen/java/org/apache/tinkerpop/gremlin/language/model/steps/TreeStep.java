package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class TreeStep {
    /**
     * @type optional: string
     */
    public final java.util.Optional<String> sideEffectKey;
    
    /**
     * Constructs an immutable TreeStep object
     */
    public TreeStep(java.util.Optional<String> sideEffectKey) {
        this.sideEffectKey = sideEffectKey;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof TreeStep)) {
            return false;
        }
        TreeStep o = (TreeStep) other;
        return sideEffectKey.equals(o.sideEffectKey);
    }
    
    @Override
    public int hashCode() {
        return 2 * sideEffectKey.hashCode();
    }
}
