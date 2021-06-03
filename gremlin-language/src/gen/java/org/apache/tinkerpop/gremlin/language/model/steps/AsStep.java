package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class AsStep {
    /**
     * @type string
     */
    public final String stepLabel;
    
    /**
     * @type list: string
     */
    public final java.util.List<String> stepLabels;
    
    /**
     * Constructs an immutable AsStep object
     */
    public AsStep(String stepLabel, java.util.List<String> stepLabels) {
        this.stepLabel = stepLabel;
        this.stepLabels = stepLabels;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof AsStep)) {
            return false;
        }
        AsStep o = (AsStep) other;
        return stepLabel.equals(o.stepLabel)
            && stepLabels.equals(o.stepLabels);
    }
    
    @Override
    public int hashCode() {
        return 2 * stepLabel.hashCode()
            + 3 * stepLabels.hashCode();
    }
    
    /**
     * Construct a new immutable AsStep object in which stepLabel is overridden
     */
    public AsStep withStepLabel(String stepLabel) {
        return new AsStep(stepLabel, stepLabels);
    }
    
    /**
     * Construct a new immutable AsStep object in which stepLabels is overridden
     */
    public AsStep withStepLabels(java.util.List<String> stepLabels) {
        return new AsStep(stepLabel, stepLabels);
    }
}
