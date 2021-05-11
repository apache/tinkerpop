package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.GenericLiteral;
import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalBiFunction;

/**
 * @type optional:
 *         record:
 *         - index: 1
 *           name: seed
 *           type: org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
 *         - index: 2
 *           name: foldFunction
 *           type: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalBiFunction
 */
public class FoldStep {
    public final java.util.Optional<Value> value;
    
    /**
     * Constructs an immutable FoldStep object
     */
    public FoldStep(java.util.Optional<Value> value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof FoldStep)) return false;
        FoldStep o = (FoldStep) other;
        return value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * value.hashCode();
    }
    
    public static class Value {
        public final GenericLiteral seed;
        
        public final TraversalBiFunction foldFunction;
        
        /**
         * Constructs an immutable Value object
         */
        public Value(GenericLiteral seed, TraversalBiFunction foldFunction) {
            this.seed = seed;
            this.foldFunction = foldFunction;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Value)) return false;
            Value o = (Value) other;
            return seed.equals(o.seed)
                && foldFunction.equals(o.foldFunction);
        }
        
        @Override
        public int hashCode() {
            return 2 * seed.hashCode()
                + 3 * foldFunction.hashCode();
        }
        
        /**
         * Construct a new immutable Value object in which seed is overridden
         */
        public Value withSeed(GenericLiteral seed) {
            return new Value(seed, foldFunction);
        }
        
        /**
         * Construct a new immutable Value object in which foldFunction is overridden
         */
        public Value withFoldFunction(TraversalBiFunction foldFunction) {
            return new Value(seed, foldFunction);
        }
    }
}
