package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class SubgraphStep {
    /**
     * @type string
     */
    public final String sideEffectKey;
    
    /**
     * Constructs an immutable SubgraphStep object
     */
    public SubgraphStep(String sideEffectKey) {
        this.sideEffectKey = sideEffectKey;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SubgraphStep)) {
            return false;
        }
        SubgraphStep o = (SubgraphStep) other;
        return sideEffectKey.equals(o.sideEffectKey);
    }
    
    @Override
    public int hashCode() {
        return 2 * sideEffectKey.hashCode();
    }
}
