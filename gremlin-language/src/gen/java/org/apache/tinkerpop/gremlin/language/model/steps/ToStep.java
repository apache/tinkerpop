package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;
import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalDirection;

public abstract class ToStep {
    private ToStep() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    public static class WithDirectionValue {
        /**
         * @type org/apache/tinkerpop/gremlin/language/model/traversal.TraversalDirection
         */
        public final TraversalDirection direction;
        
        /**
         * @type list: string
         */
        public final java.util.List<String> edgeLabels;
        
        /**
         * Constructs an immutable WithDirectionValue object
         */
        public WithDirectionValue(TraversalDirection direction, java.util.List<String> edgeLabels) {
            this.direction = direction;
            this.edgeLabels = edgeLabels;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithDirectionValue)) {
                return false;
            }
            WithDirectionValue o = (WithDirectionValue) other;
            return direction.equals(o.direction)
                && edgeLabels.equals(o.edgeLabels);
        }
        
        @Override
        public int hashCode() {
            return 2 * direction.hashCode()
                + 3 * edgeLabels.hashCode();
        }
        
        /**
         * Construct a new immutable WithDirectionValue object in which direction is overridden
         */
        public WithDirectionValue withDirection(TraversalDirection direction) {
            return new WithDirectionValue(direction, edgeLabels);
        }
        
        /**
         * Construct a new immutable WithDirectionValue object in which edgeLabels is overridden
         */
        public WithDirectionValue withEdgeLabels(java.util.List<String> edgeLabels) {
            return new WithDirectionValue(direction, edgeLabels);
        }
    }
    
    /**
     * An interface for applying a function to a ToStep according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(WithDirection instance) ;
        
        R visit(ToStepLabel instance) ;
        
        R visit(ToVertex instance) ;
    }
    
    /**
     * An interface for applying a function to a ToStep according to its variant (subclass). If a visit() method for a
     * particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(ToStep instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        @Override
        default R visit(WithDirection instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(ToStepLabel instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(ToVertex instance) {
            return otherwise(instance);
        }
    }
    
    public static final class WithDirection extends ToStep {
        public final WithDirectionValue withDirection;
        
        /**
         * Constructs an immutable WithDirection object
         */
        public WithDirection(WithDirectionValue withDirection) {
            this.withDirection = withDirection;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithDirection)) {
                return false;
            }
            WithDirection o = (WithDirection) other;
            return withDirection.equals(o.withDirection);
        }
        
        @Override
        public int hashCode() {
            return 2 * withDirection.hashCode();
        }
    }
    
    /**
     * @type string
     */
    public static final class ToStepLabel extends ToStep {
        public final String toStepLabel;
        
        /**
         * Constructs an immutable ToStepLabel object
         */
        public ToStepLabel(String toStepLabel) {
            this.toStepLabel = toStepLabel;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ToStepLabel)) {
                return false;
            }
            ToStepLabel o = (ToStepLabel) other;
            return toStepLabel.equals(o.toStepLabel);
        }
        
        @Override
        public int hashCode() {
            return 2 * toStepLabel.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public static final class ToVertex extends ToStep {
        public final NestedTraversal toVertex;
        
        /**
         * Constructs an immutable ToVertex object
         */
        public ToVertex(NestedTraversal toVertex) {
            this.toVertex = toVertex;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ToVertex)) {
                return false;
            }
            ToVertex o = (ToVertex) other;
            return toVertex.equals(o.toVertex);
        }
        
        @Override
        public int hashCode() {
            return 2 * toVertex.hashCode();
        }
    }
}
