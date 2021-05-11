package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.GenericLiteral;
import org.example.org.apache.tinkerpop.gremlin.language.model.predicates.TraversalPredicate;

public abstract class HasIdStep {
    private HasIdStep() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    public static class WithIdsValue {
        public final GenericLiteral id;
        
        public final java.util.List<GenericLiteral> otherIds;
        
        /**
         * Constructs an immutable WithIdsValue object
         */
        public WithIdsValue(GenericLiteral id, java.util.List<GenericLiteral> otherIds) {
            this.id = id;
            this.otherIds = otherIds;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithIdsValue)) return false;
            WithIdsValue o = (WithIdsValue) other;
            return id.equals(o.id)
                && otherIds.equals(o.otherIds);
        }
        
        @Override
        public int hashCode() {
            return 2 * id.hashCode()
                + 3 * otherIds.hashCode();
        }
        
        /**
         * Construct a new immutable WithIdsValue object in which id is overridden
         */
        public WithIdsValue withId(GenericLiteral id) {
            return new WithIdsValue(id, otherIds);
        }
        
        /**
         * Construct a new immutable WithIdsValue object in which otherIds is overridden
         */
        public WithIdsValue withOtherIds(java.util.List<GenericLiteral> otherIds) {
            return new WithIdsValue(id, otherIds);
        }
    }
    
    /**
     * An interface for applying a function to a HasIdStep according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(Predicate instance) ;
        
        R visit(WithIds instance) ;
    }
    
    /**
     * An interface for applying a function to a HasIdStep according to its variant (subclass). If a visit() method for a
     * particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(HasIdStep instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        default R visit(Predicate instance) {
            return otherwise(instance);
        }
        
        default R visit(WithIds instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.TraversalPredicate
     */
    public static final class Predicate extends HasIdStep {
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
    
    public static final class WithIds extends HasIdStep {
        public final WithIdsValue withIds;
        
        /**
         * Constructs an immutable WithIds object
         */
        public WithIds(WithIdsValue withIds) {
            this.withIds = withIds;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithIds)) return false;
            WithIds o = (WithIds) other;
            return withIds.equals(o.withIds);
        }
        
        @Override
        public int hashCode() {
            return 2 * withIds.hashCode();
        }
    }
}
