package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class ProfileStep {
    /**
     * @type optional: string
     */
    public final java.util.Optional<String> sideEffectKey;
    
    /**
     * Constructs an immutable ProfileStep object
     */
    public ProfileStep(java.util.Optional<String> sideEffectKey) {
        this.sideEffectKey = sideEffectKey;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ProfileStep)) return false;
        ProfileStep o = (ProfileStep) other;
        return sideEffectKey.equals(o.sideEffectKey);
    }
    
    @Override
    public int hashCode() {
        return 2 * sideEffectKey.hashCode();
    }
}
