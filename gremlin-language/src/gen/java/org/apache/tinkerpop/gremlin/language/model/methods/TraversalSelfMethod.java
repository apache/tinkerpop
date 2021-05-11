package org.example.org.apache.tinkerpop.gremlin.language.model.methods;

public abstract class TraversalSelfMethod {
    private TraversalSelfMethod() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    /**
     * An interface for applying a function to a TraversalSelfMethod according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(None instance) ;
    }
    
    /**
     * An interface for applying a function to a TraversalSelfMethod according to its variant (subclass). If a visit()
     * method for a particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(TraversalSelfMethod instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        default R visit(None instance) {
            return otherwise(instance);
        }
    }
    
    public static final class None extends TraversalSelfMethod {
        /**
         * Constructs an immutable None object
         */
        public None() {}
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof None)) return false;
            None o = (None) other;
            return true;
        }
        
        @Override
        public int hashCode() {
            return 0;
        }
    }
}
