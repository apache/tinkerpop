package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class LoopsStep {
    /**
     * @type optional: string
     */
    public final java.util.Optional<String> loopName;
    
    /**
     * Constructs an immutable LoopsStep object
     */
    public LoopsStep(java.util.Optional<String> loopName) {
        this.loopName = loopName;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof LoopsStep)) {
            return false;
        }
        LoopsStep o = (LoopsStep) other;
        return loopName.equals(o.loopName);
    }
    
    @Override
    public int hashCode() {
        return 2 * loopName.hashCode();
    }
}
