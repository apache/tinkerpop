package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.predicates.TraversalPredicate;
import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

public abstract class WhereStep {
    private WhereStep() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    public static class WithPredicateValue {
        public final java.util.Optional<String> startKey;
        
        public final TraversalPredicate predicate;
        
        /**
         * Constructs an immutable WithPredicateValue object
         */
        public WithPredicateValue(java.util.Optional<String> startKey, TraversalPredicate predicate) {
            this.startKey = startKey;
            this.predicate = predicate;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithPredicateValue)) return false;
            WithPredicateValue o = (WithPredicateValue) other;
            return startKey.equals(o.startKey)
                && predicate.equals(o.predicate);
        }
        
        @Override
        public int hashCode() {
            return 2 * startKey.hashCode()
                + 3 * predicate.hashCode();
        }
        
        /**
         * Construct a new immutable WithPredicateValue object in which startKey is overridden
         */
        public WithPredicateValue withStartKey(java.util.Optional<String> startKey) {
            return new WithPredicateValue(startKey, predicate);
        }
        
        /**
         * Construct a new immutable WithPredicateValue object in which predicate is overridden
         */
        public WithPredicateValue withPredicate(TraversalPredicate predicate) {
            return new WithPredicateValue(startKey, predicate);
        }
    }
    
    /**
     * An interface for applying a function to a WhereStep according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(WithPredicate instance) ;
        
        R visit(WhereTraversal instance) ;
    }
    
    /**
     * An interface for applying a function to a WhereStep according to its variant (subclass). If a visit() method for a
     * particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(WhereStep instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        default R visit(WithPredicate instance) {
            return otherwise(instance);
        }
        
        default R visit(WhereTraversal instance) {
            return otherwise(instance);
        }
    }
    
    public static final class WithPredicate extends WhereStep {
        public final WithPredicateValue withPredicate;
        
        /**
         * Constructs an immutable WithPredicate object
         */
        public WithPredicate(WithPredicateValue withPredicate) {
            this.withPredicate = withPredicate;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithPredicate)) return false;
            WithPredicate o = (WithPredicate) other;
            return withPredicate.equals(o.withPredicate);
        }
        
        @Override
        public int hashCode() {
            return 2 * withPredicate.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public static final class WhereTraversal extends WhereStep {
        public final NestedTraversal whereTraversal;
        
        /**
         * Constructs an immutable WhereTraversal object
         */
        public WhereTraversal(NestedTraversal whereTraversal) {
            this.whereTraversal = whereTraversal;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WhereTraversal)) return false;
            WhereTraversal o = (WhereTraversal) other;
            return whereTraversal.equals(o.whereTraversal);
        }
        
        @Override
        public int hashCode() {
            return 2 * whereTraversal.hashCode();
        }
    }
}
