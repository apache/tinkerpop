package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class PeerPressureStep {
    /**
     * Constructs an immutable PeerPressureStep object
     */
    public PeerPressureStep() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof PeerPressureStep)) return false;
        PeerPressureStep o = (PeerPressureStep) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
