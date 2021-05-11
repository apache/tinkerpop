package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class CoinStep {
    /**
     * @type float
     */
    public final Float probability;
    
    /**
     * Constructs an immutable CoinStep object
     */
    public CoinStep(Float probability) {
        this.probability = probability;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CoinStep)) return false;
        CoinStep o = (CoinStep) other;
        return probability.equals(o.probability);
    }
    
    @Override
    public int hashCode() {
        return 2 * probability.hashCode();
    }
}
