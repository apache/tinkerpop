package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class CapStep {
    /**
     * @type string
     */
    public final String sideEffectKey;
    
    /**
     * @type list: string
     */
    public final java.util.List<String> sideEffectKeys;
    
    /**
     * Constructs an immutable CapStep object
     */
    public CapStep(String sideEffectKey, java.util.List<String> sideEffectKeys) {
        this.sideEffectKey = sideEffectKey;
        this.sideEffectKeys = sideEffectKeys;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CapStep)) return false;
        CapStep o = (CapStep) other;
        return sideEffectKey.equals(o.sideEffectKey)
            && sideEffectKeys.equals(o.sideEffectKeys);
    }
    
    @Override
    public int hashCode() {
        return 2 * sideEffectKey.hashCode()
            + 3 * sideEffectKeys.hashCode();
    }
    
    /**
     * Construct a new immutable CapStep object in which sideEffectKey is overridden
     */
    public CapStep withSideEffectKey(String sideEffectKey) {
        return new CapStep(sideEffectKey, sideEffectKeys);
    }
    
    /**
     * Construct a new immutable CapStep object in which sideEffectKeys is overridden
     */
    public CapStep withSideEffectKeys(java.util.List<String> sideEffectKeys) {
        return new CapStep(sideEffectKey, sideEffectKeys);
    }
}
