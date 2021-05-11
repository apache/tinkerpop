package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.predicates.TraversalPredicate;

public abstract class HasValueStep {
    private HasValueStep() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    public static class WithValuesValue {
        /**
         * Constructs an immutable WithValuesValue object
         */
        public WithValuesValue() {}
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithValuesValue)) return false;
            WithValuesValue o = (WithValuesValue) other;
            return true;
        }
        
        @Override
        public int hashCode() {
            return 0;
        }
    }
    
    /**
     * An interface for applying a function to a HasValueStep according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(Predicate instance) ;
        
        R visit(WithValues instance) ;
    }
    
    /**
     * An interface for applying a function to a HasValueStep according to its variant (subclass). If a visit() method for a
     * particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(HasValueStep instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        default R visit(Predicate instance) {
            return otherwise(instance);
        }
        
        default R visit(WithValues instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.TraversalPredicate
     */
    public static final class Predicate extends HasValueStep {
        public final TraversalPredicate predicate;
        
        /**
         * Constructs an immutable Predicate object
         */
        public Predicate(TraversalPredicate predicate) {
            this.predicate = predicate;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Predicate)) return false;
            Predicate o = (Predicate) other;
            return predicate.equals(o.predicate);
        }
        
        @Override
        public int hashCode() {
            return 2 * predicate.hashCode();
        }
    }
    
    public static final class WithValues extends HasValueStep {
        public final WithValuesValue withValues;
        
        /**
         * Constructs an immutable WithValues object
         */
        public WithValues(WithValuesValue withValues) {
            this.withValues = withValues;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithValues)) return false;
            WithValues o = (WithValues) other;
            return withValues.equals(o.withValues);
        }
        
        @Override
        public int hashCode() {
            return 2 * withValues.hashCode();
        }
    }
}
