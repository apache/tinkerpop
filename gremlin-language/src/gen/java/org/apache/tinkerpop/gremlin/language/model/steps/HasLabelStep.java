package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.predicates.TraversalPredicate;

public abstract class HasLabelStep {
    private HasLabelStep() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    public static class WithLabelsValue {
        /**
         * @type string
         */
        public final String label;
        
        /**
         * @type list: string
         */
        public final java.util.List<String> otherLabels;
        
        /**
         * Constructs an immutable WithLabelsValue object
         */
        public WithLabelsValue(String label, java.util.List<String> otherLabels) {
            this.label = label;
            this.otherLabels = otherLabels;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithLabelsValue)) {
                return false;
            }
            WithLabelsValue o = (WithLabelsValue) other;
            return label.equals(o.label)
                && otherLabels.equals(o.otherLabels);
        }
        
        @Override
        public int hashCode() {
            return 2 * label.hashCode()
                + 3 * otherLabels.hashCode();
        }
        
        /**
         * Construct a new immutable WithLabelsValue object in which label is overridden
         */
        public WithLabelsValue withLabel(String label) {
            return new WithLabelsValue(label, otherLabels);
        }
        
        /**
         * Construct a new immutable WithLabelsValue object in which otherLabels is overridden
         */
        public WithLabelsValue withOtherLabels(java.util.List<String> otherLabels) {
            return new WithLabelsValue(label, otherLabels);
        }
    }
    
    /**
     * An interface for applying a function to a HasLabelStep according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(Predicate instance) ;
        
        R visit(WithLabels instance) ;
    }
    
    /**
     * An interface for applying a function to a HasLabelStep according to its variant (subclass). If a visit() method for a
     * particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(HasLabelStep instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        @Override
        default R visit(Predicate instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(WithLabels instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/predicates.TraversalPredicate
     */
    public static final class Predicate extends HasLabelStep {
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
    
    public static final class WithLabels extends HasLabelStep {
        public final WithLabelsValue withLabels;
        
        /**
         * Constructs an immutable WithLabels object
         */
        public WithLabels(WithLabelsValue withLabels) {
            this.withLabels = withLabels;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithLabels)) {
                return false;
            }
            WithLabels o = (WithLabels) other;
            return withLabels.equals(o.withLabels);
        }
        
        @Override
        public int hashCode() {
            return 2 * withLabels.hashCode();
        }
    }
}
