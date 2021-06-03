package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.predicates.TraversalPredicate;
import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

public abstract class ChooseStep {
    private ChooseStep() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    public static class WithPredicateValue {
        /**
         * @type org/apache/tinkerpop/gremlin/language/model/predicates.TraversalPredicate
         */
        public final TraversalPredicate traversalPredicate;
        
        /**
         * @type org/apache/tinkerpop/gremlin/language/model/predicates.TraversalPredicate
         */
        public final TraversalPredicate trueChoice;
        
        /**
         * @type optional: org/apache/tinkerpop/gremlin/language/model/predicates.TraversalPredicate
         */
        public final java.util.Optional<TraversalPredicate> falseChoice;
        
        /**
         * Constructs an immutable WithPredicateValue object
         */
        public WithPredicateValue(TraversalPredicate traversalPredicate, TraversalPredicate trueChoice, java.util.Optional<TraversalPredicate> falseChoice) {
            this.traversalPredicate = traversalPredicate;
            this.trueChoice = trueChoice;
            this.falseChoice = falseChoice;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithPredicateValue)) {
                return false;
            }
            WithPredicateValue o = (WithPredicateValue) other;
            return traversalPredicate.equals(o.traversalPredicate)
                && trueChoice.equals(o.trueChoice)
                && falseChoice.equals(o.falseChoice);
        }
        
        @Override
        public int hashCode() {
            return 2 * traversalPredicate.hashCode()
                + 3 * trueChoice.hashCode()
                + 5 * falseChoice.hashCode();
        }
        
        /**
         * Construct a new immutable WithPredicateValue object in which traversalPredicate is overridden
         */
        public WithPredicateValue withTraversalPredicate(TraversalPredicate traversalPredicate) {
            return new WithPredicateValue(traversalPredicate, trueChoice, falseChoice);
        }
        
        /**
         * Construct a new immutable WithPredicateValue object in which trueChoice is overridden
         */
        public WithPredicateValue withTrueChoice(TraversalPredicate trueChoice) {
            return new WithPredicateValue(traversalPredicate, trueChoice, falseChoice);
        }
        
        /**
         * Construct a new immutable WithPredicateValue object in which falseChoice is overridden
         */
        public WithPredicateValue withFalseChoice(java.util.Optional<TraversalPredicate> falseChoice) {
            return new WithPredicateValue(traversalPredicate, trueChoice, falseChoice);
        }
    }
    
    public static class WithTraversalValue {
        /**
         * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
         */
        public final NestedTraversal traversalPredicate;
        
        /**
         * @type org/apache/tinkerpop/gremlin/language/model/predicates.TraversalPredicate
         */
        public final TraversalPredicate trueChoice;
        
        /**
         * @type optional: org/apache/tinkerpop/gremlin/language/model/predicates.TraversalPredicate
         */
        public final java.util.Optional<TraversalPredicate> falseChoice;
        
        /**
         * Constructs an immutable WithTraversalValue object
         */
        public WithTraversalValue(NestedTraversal traversalPredicate, TraversalPredicate trueChoice, java.util.Optional<TraversalPredicate> falseChoice) {
            this.traversalPredicate = traversalPredicate;
            this.trueChoice = trueChoice;
            this.falseChoice = falseChoice;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithTraversalValue)) {
                return false;
            }
            WithTraversalValue o = (WithTraversalValue) other;
            return traversalPredicate.equals(o.traversalPredicate)
                && trueChoice.equals(o.trueChoice)
                && falseChoice.equals(o.falseChoice);
        }
        
        @Override
        public int hashCode() {
            return 2 * traversalPredicate.hashCode()
                + 3 * trueChoice.hashCode()
                + 5 * falseChoice.hashCode();
        }
        
        /**
         * Construct a new immutable WithTraversalValue object in which traversalPredicate is overridden
         */
        public WithTraversalValue withTraversalPredicate(NestedTraversal traversalPredicate) {
            return new WithTraversalValue(traversalPredicate, trueChoice, falseChoice);
        }
        
        /**
         * Construct a new immutable WithTraversalValue object in which trueChoice is overridden
         */
        public WithTraversalValue withTrueChoice(TraversalPredicate trueChoice) {
            return new WithTraversalValue(traversalPredicate, trueChoice, falseChoice);
        }
        
        /**
         * Construct a new immutable WithTraversalValue object in which falseChoice is overridden
         */
        public WithTraversalValue withFalseChoice(java.util.Optional<TraversalPredicate> falseChoice) {
            return new WithTraversalValue(traversalPredicate, trueChoice, falseChoice);
        }
    }
    
    /**
     * An interface for applying a function to a ChooseStep according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(ChoiceTraversal instance) ;
        
        R visit(WithTraversal instance) ;
        
        R visit(WithPredicate instance) ;
    }
    
    /**
     * An interface for applying a function to a ChooseStep according to its variant (subclass). If a visit() method for a
     * particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(ChooseStep instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        @Override
        default R visit(ChoiceTraversal instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(WithTraversal instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(WithPredicate instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public static final class ChoiceTraversal extends ChooseStep {
        public final NestedTraversal choiceTraversal;
        
        /**
         * Constructs an immutable ChoiceTraversal object
         */
        public ChoiceTraversal(NestedTraversal choiceTraversal) {
            this.choiceTraversal = choiceTraversal;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ChoiceTraversal)) {
                return false;
            }
            ChoiceTraversal o = (ChoiceTraversal) other;
            return choiceTraversal.equals(o.choiceTraversal);
        }
        
        @Override
        public int hashCode() {
            return 2 * choiceTraversal.hashCode();
        }
    }
    
    public static final class WithTraversal extends ChooseStep {
        public final WithTraversalValue withTraversal;
        
        /**
         * Constructs an immutable WithTraversal object
         */
        public WithTraversal(WithTraversalValue withTraversal) {
            this.withTraversal = withTraversal;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithTraversal)) {
                return false;
            }
            WithTraversal o = (WithTraversal) other;
            return withTraversal.equals(o.withTraversal);
        }
        
        @Override
        public int hashCode() {
            return 2 * withTraversal.hashCode();
        }
    }
    
    public static final class WithPredicate extends ChooseStep {
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
            if (!(other instanceof WithPredicate)) {
                return false;
            }
            WithPredicate o = (WithPredicate) other;
            return withPredicate.equals(o.withPredicate);
        }
        
        @Override
        public int hashCode() {
            return 2 * withPredicate.hashCode();
        }
    }
}
