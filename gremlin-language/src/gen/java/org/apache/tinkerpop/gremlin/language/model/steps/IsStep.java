package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.GenericLiteral;
import org.example.org.apache.tinkerpop.gremlin.language.model.predicates.TraversalPredicate;

public abstract class IsStep {
    private IsStep() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    /**
     * An interface for applying a function to a IsStep according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(Predicate instance) ;
        
        R visit(Value instance) ;
    }
    
    /**
     * An interface for applying a function to a IsStep according to its variant (subclass). If a visit() method for a
     * particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(IsStep instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        default R visit(Predicate instance) {
            return otherwise(instance);
        }
        
        default R visit(Value instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.TraversalPredicate
     */
    public static final class Predicate extends IsStep {
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
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
     */
    public static final class Value extends IsStep {
        public final GenericLiteral value;
        
        /**
         * Constructs an immutable Value object
         */
        public Value(GenericLiteral value) {
            this.value = value;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Value)) return false;
            Value o = (Value) other;
            return value.equals(o.value);
        }
        
        @Override
        public int hashCode() {
            return 2 * value.hashCode();
        }
    }
}
