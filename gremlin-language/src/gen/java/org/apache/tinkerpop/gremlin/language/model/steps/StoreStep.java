package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class StoreStep {
    /**
     * @type string
     */
    public final String sideEffectKey;
    
    /**
     * Constructs an immutable StoreStep object
     */
    public StoreStep(String sideEffectKey) {
        this.sideEffectKey = sideEffectKey;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StoreStep)) return false;
        StoreStep o = (StoreStep) other;
        return sideEffectKey.equals(o.sideEffectKey);
    }
    
    @Override
    public int hashCode() {
        return 2 * sideEffectKey.hashCode();
    }
}
