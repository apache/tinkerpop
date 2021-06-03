package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.GenericLiteral;
import org.example.org.apache.tinkerpop.gremlin.language.model.predicates.TraversalPredicate;
import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

public abstract class OptionStep {
    private OptionStep() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    public static class ObjectTraversalValue {
        /**
         * @type org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
         */
        public final GenericLiteral object;
        
        /**
         * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
         */
        public final NestedTraversal traversal;
        
        /**
         * Constructs an immutable ObjectTraversalValue object
         */
        public ObjectTraversalValue(GenericLiteral object, NestedTraversal traversal) {
            this.object = object;
            this.traversal = traversal;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ObjectTraversalValue)) {
                return false;
            }
            ObjectTraversalValue o = (ObjectTraversalValue) other;
            return object.equals(o.object)
                && traversal.equals(o.traversal);
        }
        
        @Override
        public int hashCode() {
            return 2 * object.hashCode()
                + 3 * traversal.hashCode();
        }
        
        /**
         * Construct a new immutable ObjectTraversalValue object in which object is overridden
         */
        public ObjectTraversalValue withObject(GenericLiteral object) {
            return new ObjectTraversalValue(object, traversal);
        }
        
        /**
         * Construct a new immutable ObjectTraversalValue object in which traversal is overridden
         */
        public ObjectTraversalValue withTraversal(NestedTraversal traversal) {
            return new ObjectTraversalValue(object, traversal);
        }
    }
    
    public static class PredicateTraversalValue {
        /**
         * @type org/apache/tinkerpop/gremlin/language/model/predicates.TraversalPredicate
         */
        public final TraversalPredicate predicate;
        
        /**
         * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
         */
        public final NestedTraversal traversal;
        
        /**
         * Constructs an immutable PredicateTraversalValue object
         */
        public PredicateTraversalValue(TraversalPredicate predicate, NestedTraversal traversal) {
            this.predicate = predicate;
            this.traversal = traversal;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof PredicateTraversalValue)) {
                return false;
            }
            PredicateTraversalValue o = (PredicateTraversalValue) other;
            return predicate.equals(o.predicate)
                && traversal.equals(o.traversal);
        }
        
        @Override
        public int hashCode() {
            return 2 * predicate.hashCode()
                + 3 * traversal.hashCode();
        }
        
        /**
         * Construct a new immutable PredicateTraversalValue object in which predicate is overridden
         */
        public PredicateTraversalValue withPredicate(TraversalPredicate predicate) {
            return new PredicateTraversalValue(predicate, traversal);
        }
        
        /**
         * Construct a new immutable PredicateTraversalValue object in which traversal is overridden
         */
        public PredicateTraversalValue withTraversal(NestedTraversal traversal) {
            return new PredicateTraversalValue(predicate, traversal);
        }
    }
    
    /**
     * An interface for applying a function to a OptionStep according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(PredicateTraversal instance) ;
        
        R visit(ObjectTraversal instance) ;
        
        R visit(Traversal instance) ;
    }
    
    /**
     * An interface for applying a function to a OptionStep according to its variant (subclass). If a visit() method for a
     * particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(OptionStep instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        @Override
        default R visit(PredicateTraversal instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(ObjectTraversal instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(Traversal instance) {
            return otherwise(instance);
        }
    }
    
    public static final class PredicateTraversal extends OptionStep {
        public final PredicateTraversalValue predicateTraversal;
        
        /**
         * Constructs an immutable PredicateTraversal object
         */
        public PredicateTraversal(PredicateTraversalValue predicateTraversal) {
            this.predicateTraversal = predicateTraversal;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof PredicateTraversal)) {
                return false;
            }
            PredicateTraversal o = (PredicateTraversal) other;
            return predicateTraversal.equals(o.predicateTraversal);
        }
        
        @Override
        public int hashCode() {
            return 2 * predicateTraversal.hashCode();
        }
    }
    
    public static final class ObjectTraversal extends OptionStep {
        public final ObjectTraversalValue objectTraversal;
        
        /**
         * Constructs an immutable ObjectTraversal object
         */
        public ObjectTraversal(ObjectTraversalValue objectTraversal) {
            this.objectTraversal = objectTraversal;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ObjectTraversal)) {
                return false;
            }
            ObjectTraversal o = (ObjectTraversal) other;
            return objectTraversal.equals(o.objectTraversal);
        }
        
        @Override
        public int hashCode() {
            return 2 * objectTraversal.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public static final class Traversal extends OptionStep {
        public final NestedTraversal traversal;
        
        /**
         * Constructs an immutable Traversal object
         */
        public Traversal(NestedTraversal traversal) {
            this.traversal = traversal;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Traversal)) {
                return false;
            }
            Traversal o = (Traversal) other;
            return traversal.equals(o.traversal);
        }
        
        @Override
        public int hashCode() {
            return 2 * traversal.hashCode();
        }
    }
}
