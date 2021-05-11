package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class InEStep {
    /**
     * @type list: string
     */
    public final java.util.List<String> edgeLabels;
    
    /**
     * Constructs an immutable InEStep object
     */
    public InEStep(java.util.List<String> edgeLabels) {
        this.edgeLabels = edgeLabels;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof InEStep)) return false;
        InEStep o = (InEStep) other;
        return edgeLabels.equals(o.edgeLabels);
    }
    
    @Override
    public int hashCode() {
        return 2 * edgeLabels.hashCode();
    }
}
