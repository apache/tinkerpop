package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.predicates.TraversalPredicate;
import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

public abstract class FilterStep {
    private FilterStep() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    /**
     * An interface for applying a function to a FilterStep according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(Predicate instance) ;
        
        R visit(FilterTraversal instance) ;
    }
    
    /**
     * An interface for applying a function to a FilterStep according to its variant (subclass). If a visit() method for a
     * particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(FilterStep instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        @Override
        default R visit(Predicate instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(FilterTraversal instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.TraversalPredicate
     */
    public static final class Predicate extends FilterStep {
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
            if (!(other instanceof Predicate)) {
                return false;
            }
            Predicate o = (Predicate) other;
            return predicate.equals(o.predicate);
        }
        
        @Override
        public int hashCode() {
            return 2 * predicate.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public static final class FilterTraversal extends FilterStep {
        public final NestedTraversal filterTraversal;
        
        /**
         * Constructs an immutable FilterTraversal object
         */
        public FilterTraversal(NestedTraversal filterTraversal) {
            this.filterTraversal = filterTraversal;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof FilterTraversal)) {
                return false;
            }
            FilterTraversal o = (FilterTraversal) other;
            return filterTraversal.equals(o.filterTraversal);
        }
        
        @Override
        public int hashCode() {
            return 2 * filterTraversal.hashCode();
        }
    }
}
