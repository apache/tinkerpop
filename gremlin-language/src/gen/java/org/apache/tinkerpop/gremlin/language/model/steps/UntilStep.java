package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.predicates.TraversalPredicate;
import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

public abstract class UntilStep {
    private UntilStep() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    /**
     * An interface for applying a function to a UntilStep according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(UntilPredicate instance) ;
        
        R visit(UntilTraversal instance) ;
    }
    
    /**
     * An interface for applying a function to a UntilStep according to its variant (subclass). If a visit() method for a
     * particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(UntilStep instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        default R visit(UntilPredicate instance) {
            return otherwise(instance);
        }
        
        default R visit(UntilTraversal instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.TraversalPredicate
     */
    public static final class UntilPredicate extends UntilStep {
        public final TraversalPredicate untilPredicate;
        
        /**
         * Constructs an immutable UntilPredicate object
         */
        public UntilPredicate(TraversalPredicate untilPredicate) {
            this.untilPredicate = untilPredicate;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof UntilPredicate)) return false;
            UntilPredicate o = (UntilPredicate) other;
            return untilPredicate.equals(o.untilPredicate);
        }
        
        @Override
        public int hashCode() {
            return 2 * untilPredicate.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public static final class UntilTraversal extends UntilStep {
        public final NestedTraversal untilTraversal;
        
        /**
         * Constructs an immutable UntilTraversal object
         */
        public UntilTraversal(NestedTraversal untilTraversal) {
            this.untilTraversal = untilTraversal;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof UntilTraversal)) return false;
            UntilTraversal o = (UntilTraversal) other;
            return untilTraversal.equals(o.untilTraversal);
        }
        
        @Override
        public int hashCode() {
            return 2 * untilTraversal.hashCode();
        }
    }
}
